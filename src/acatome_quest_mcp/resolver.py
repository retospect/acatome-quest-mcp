"""Metadata resolution and cross-validation.

Wraps ``acatome-meta`` (Crossref + Semantic Scholar + arXiv) in an async
interface and produces :class:`ResolvedRef` + candidates + misconceptions.

The wrapped functions are synchronous (``habanero`` / ``semanticscholar`` are
not async).  We off-load them to the default executor via
:func:`asyncio.to_thread` so the event loop keeps running.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from rapidfuzz import fuzz

from .misconceptions import Misconception, MisconceptionCode
from .models import Candidate, PaperRef, ResolvedRef

log = logging.getLogger(__name__)

# Fuzz threshold below which DOI↔title looks like a mismatch.
TITLE_MISMATCH_FUZZ = 60


class Resolver:
    """Metadata resolver.  Injectable sync callables for tests."""

    def __init__(
        self,
        *,
        crossref_fn: Any | None = None,
        s2_title_fn: Any | None = None,
        s2_id_fn: Any | None = None,
        mailto: str | None = None,
        s2_key: str | None = None,
    ) -> None:
        if crossref_fn is None:
            from acatome_meta.crossref import lookup_crossref

            crossref_fn = lookup_crossref
        if s2_title_fn is None:
            from acatome_meta.semantic_scholar import lookup_s2

            s2_title_fn = lookup_s2
        if s2_id_fn is None:
            from acatome_meta.semantic_scholar import get_paper_by_id

            s2_id_fn = get_paper_by_id

        self._crossref_fn = crossref_fn
        self._s2_title_fn = s2_title_fn
        self._s2_id_fn = s2_id_fn
        self._mailto = mailto or os.environ.get("ACATOME_CROSSREF_MAILTO", "")
        self._s2_key = s2_key or os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "")

    async def resolve(
        self, ref: PaperRef
    ) -> tuple[ResolvedRef, list[Candidate], list[Misconception]]:
        """Full cascade.  Returns (resolved, candidates, misconceptions)."""
        ref = ref.normalize()
        miscs: list[Misconception] = []
        candidates: list[Candidate] = []

        # 1. DOI → Crossref (authoritative)
        if ref.doi:
            crossref = await self._crossref(ref.doi)
            if crossref:
                resolved = _from_crossref(crossref)
                resolved.score = 0.95
                # Cross-validate title if caller supplied one
                if ref.title and resolved.title:
                    score = fuzz.token_set_ratio(ref.title, resolved.title)
                    if score < TITLE_MISMATCH_FUZZ:
                        miscs.append(
                            Misconception.of(
                                MisconceptionCode.DOI_TITLE_MISMATCH,
                                evidence=(
                                    f"Requested title '{ref.title}' does not match "
                                    f"DOI {ref.doi} title '{resolved.title}' "
                                    f"(fuzz {score})"
                                ),
                            )
                        )
                        resolved.score = 0.4
                return resolved, candidates, miscs

            # DOI didn't resolve — fall through to title search, and flag.
            miscs.append(
                Misconception.of(
                    MisconceptionCode.DOI_INVALID,
                    evidence=f"Crossref returned no record for {ref.doi}",
                )
            )

        # 2. arXiv id → S2 (arXiv metadata).
        if ref.arxiv:
            s2 = await self._s2_id(f"ARXIV:{ref.arxiv}")
            if s2:
                resolved = _from_s2(s2)
                if not resolved.arxiv:
                    resolved.arxiv = ref.arxiv
                resolved.score = 0.85
                return resolved, candidates, miscs

        # 3. Title → S2 search.
        if ref.title:
            s2 = await self._s2_title(ref.title)
            if s2:
                resolved = _from_s2(s2)
                score = 0.7
                if resolved.title:
                    fuzz_score = fuzz.token_set_ratio(ref.title, resolved.title)
                    score = max(0.3, min(0.9, fuzz_score / 100.0))
                resolved.score = score
                # If the requester also gave a DOI that didn't resolve and we
                # got a result here, record the resolved DOI.
                if ref.doi and not resolved.doi:
                    resolved.doi = ref.doi
                return resolved, candidates, miscs
            else:
                miscs.append(
                    Misconception.of(
                        MisconceptionCode.TITLE_NOT_FOUND,
                        evidence=(
                            f"Semantic Scholar returned no match for title "
                            f"'{ref.title}'"
                        ),
                    )
                )

        # 4. Nothing matched.  Return whatever the caller gave us.
        resolved = ResolvedRef(
            doi=ref.doi,
            arxiv=ref.arxiv,
            pmid=ref.pmid,
            title=ref.title,
            authors=list(ref.authors),
            year=ref.year,
            score=0.0,
            source="echo",
        )
        return resolved, candidates, miscs

    # -----------------------------------------------------------------
    # Sync shims — one of these is what's easy to mock in tests.
    # -----------------------------------------------------------------

    async def _crossref(self, doi: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._crossref_fn, doi, self._mailto)

    async def _s2_title(self, title: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._s2_title_fn, title, self._s2_key)

    async def _s2_id(self, paper_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._s2_id_fn, paper_id, self._s2_key)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _authors_to_list(authors_raw: Any) -> list[str]:
    """Coerce acatome-meta's ``[{'name': '...'}]`` shape to a flat list."""
    if not authors_raw:
        return []
    out: list[str] = []
    for a in authors_raw:
        if isinstance(a, dict):
            name = a.get("name") or ""
        else:
            name = str(a)
        name = name.strip()
        if name:
            out.append(name)
    return out


def _from_crossref(d: dict[str, Any]) -> ResolvedRef:
    return ResolvedRef(
        doi=d.get("doi"),
        title=d.get("title") or None,
        authors=_authors_to_list(d.get("authors")),
        year=d.get("year"),
        journal=d.get("journal") or None,
        source="crossref",
    )


def _from_s2(d: dict[str, Any]) -> ResolvedRef:
    doi = d.get("doi")
    if doi:
        doi = doi.lower()
    return ResolvedRef(
        doi=doi,
        arxiv=(d.get("arxiv_id") or "").lower() or None,
        title=d.get("title") or None,
        authors=_authors_to_list(d.get("authors")),
        year=d.get("year"),
        journal=d.get("journal") or None,
        source="s2",
    )
