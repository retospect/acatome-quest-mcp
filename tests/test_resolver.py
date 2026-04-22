"""Tests for the metadata resolver.

We inject fake sync callables in place of ``habanero`` and
``semanticscholar``, so these tests never touch the network.
"""

from __future__ import annotations

from typing import Any

from acatome_quest_mcp.misconceptions import MisconceptionCode
from acatome_quest_mcp.models import PaperRef
from acatome_quest_mcp.resolver import Resolver


def _mk_resolver(
    *,
    crossref: dict[str, Any] | None = None,
    s2_title: dict[str, Any] | None = None,
    s2_id: dict[str, Any] | None = None,
) -> Resolver:
    def cr_fn(doi: str, mailto: str = "") -> dict[str, Any] | None:
        return crossref

    def s2t_fn(title: str, api_key: str = "") -> dict[str, Any] | None:
        return s2_title

    def s2i_fn(paper_id: str, api_key: str = "") -> dict[str, Any] | None:
        return s2_id

    return Resolver(crossref_fn=cr_fn, s2_title_fn=s2t_fn, s2_id_fn=s2i_fn)


def _crossref_result(
    title: str = "Sample paper",
    doi: str = "10.1/sample",
    authors: list[str] | None = None,
    year: int = 2024,
) -> dict[str, Any]:
    return {
        "title": title,
        "doi": doi,
        "authors": [{"name": a} for a in (authors or ["Smith, J."])],
        "year": year,
        "journal": "Sample Journal",
        "source": "crossref",
    }


def _s2_result(
    title: str = "Sample paper",
    doi: str | None = "10.1/sample",
    arxiv_id: str | None = None,
    year: int = 2024,
) -> dict[str, Any]:
    return {
        "title": title,
        "doi": doi,
        "arxiv_id": arxiv_id,
        "authors": [{"name": "Smith, J."}],
        "year": year,
        "journal": "Sample Journal",
        "source": "s2",
    }


class TestDoiResolution:
    def test_doi_hit_returns_resolved(self) -> None:
        r = _mk_resolver(crossref=_crossref_result())
        resolved, candidates, miscs = r.resolve(PaperRef(doi="10.1/sample"))
        assert resolved.title == "Sample paper"
        assert resolved.source == "crossref"
        assert resolved.score > 0.9
        assert not candidates
        assert not miscs

    def test_doi_miss_flags_invalid_and_falls_back_to_title(self) -> None:
        r = _mk_resolver(crossref=None, s2_title=_s2_result(title="Fallback"))
        resolved, _, miscs = r.resolve(PaperRef(doi="10.1/bogus", title="Fallback"))
        assert any(m.code == MisconceptionCode.DOI_INVALID for m in miscs)
        assert resolved.source == "s2"
        assert resolved.title == "Fallback"

    def test_title_not_found_flags_fabrication(self) -> None:
        r = _mk_resolver(crossref=None, s2_title=None)
        resolved, _, miscs = r.resolve(
            PaperRef(doi="10.1/bogus", title="Ghost paper that doesn't exist")
        )
        codes = {m.code for m in miscs}
        assert MisconceptionCode.DOI_INVALID in codes
        assert MisconceptionCode.TITLE_NOT_FOUND in codes
        assert resolved.score == 0.0


class TestTitleMismatch:
    def test_mismatch_flags_critical(self) -> None:
        # DOI resolves to a completely different title.
        r = _mk_resolver(
            crossref=_crossref_result(title="An unrelated paper about catalysis")
        )
        resolved, _, miscs = r.resolve(
            PaperRef(
                doi="10.1/sample",
                title="Anion exchange membranes for NOx reduction",
            )
        )
        codes = {m.code for m in miscs}
        assert MisconceptionCode.DOI_TITLE_MISMATCH in codes
        # Score should be dropped to reflect low confidence.
        assert resolved.score < 0.5

    def test_near_match_does_not_flag(self) -> None:
        r = _mk_resolver(
            crossref=_crossref_result(
                title="Anion-exchange membranes for NOx reduction: a review"
            )
        )
        resolved, _, miscs = r.resolve(
            PaperRef(
                doi="10.1/sample",
                title="Anion exchange membranes for NOx reduction",
            )
        )
        codes = {m.code for m in miscs}
        assert MisconceptionCode.DOI_TITLE_MISMATCH not in codes
        assert resolved.score > 0.9


class TestArxivResolution:
    def test_arxiv_id_uses_s2(self) -> None:
        r = _mk_resolver(s2_id=_s2_result(title="arXiv paper", arxiv_id="2508.20254"))
        resolved, _, miscs = r.resolve(PaperRef(arxiv="2508.20254"))
        assert resolved.source == "s2"
        assert resolved.arxiv == "2508.20254"
        assert not miscs


class TestTitleOnly:
    def test_score_reflects_fuzz(self) -> None:
        r = _mk_resolver(s2_title=_s2_result(title="Slightly different title"))
        resolved, _, _ = r.resolve(PaperRef(title="Different title slightly"))
        assert 0.3 <= resolved.score <= 0.9


class TestEmptyRef:
    def test_empty_returns_echo(self) -> None:
        r = _mk_resolver()
        resolved, _, _ = r.resolve(PaperRef())
        assert resolved.source == "echo"
        assert resolved.score == 0.0
