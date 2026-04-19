"""Data models for acatome-quest-mcp.

A *request* is "the LLM wants this paper".  Its lifecycle is captured in
:class:`RequestStatus`; the full record is :class:`PaperRequest`.

The inputs are what the caller supplied (doi/arxiv/title/...); the resolved
fields are what we confirmed via Crossref/S2.  ``candidates`` lists top-N
alternatives when resolution is ambiguous.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from .misconceptions import Misconception

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class RequestStatus(StrEnum):
    QUEUED = "queued"
    RESOLVING = "resolving"
    FOUND_IN_STORE = "found_in_store"
    NEEDS_USER = "needs_user"
    FETCHING = "fetching"
    INGESTING = "ingesting"
    INGESTED = "ingested"
    EXTRACT_FAILED = "extract_failed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES: frozenset[RequestStatus] = frozenset(
    {
        RequestStatus.FOUND_IN_STORE,
        RequestStatus.INGESTED,
        RequestStatus.EXTRACT_FAILED,
        RequestStatus.FAILED,
        RequestStatus.CANCELLED,
    }
)


OPEN_STATUSES: frozenset[RequestStatus] = frozenset(
    {
        RequestStatus.QUEUED,
        RequestStatus.RESOLVING,
        RequestStatus.NEEDS_USER,
        RequestStatus.FETCHING,
        RequestStatus.INGESTING,
    }
)


class UpdateMode(StrEnum):
    CONFIRM = "confirm"
    REPOINT = "repoint"
    FLAG = "flag"
    PRIORITY = "priority"
    CANCEL = "cancel"


# ---------------------------------------------------------------------------
# Input / resolved / candidate dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PaperRef:
    """What the caller asked for.  Any subset of fields may be set."""

    doi: str | None = None
    arxiv: str | None = None
    pmid: str | None = None
    title: str | None = None
    authors: list[str] = field(default_factory=list)
    year: int | None = None
    raw: str | None = None

    def is_empty(self) -> bool:
        return not any(
            [self.doi, self.arxiv, self.pmid, self.title, self.authors, self.raw]
        )

    def normalize(self) -> PaperRef:
        """Trim whitespace, lowercase DOI, extract DOI from raw if needed."""
        doi = normalize_doi(self.doi) if self.doi else None
        arxiv = normalize_arxiv(self.arxiv) if self.arxiv else None

        # If raw looks like a DOI URL, promote it.
        if not doi and self.raw:
            m = _DOI_IN_TEXT.search(self.raw)
            if m:
                doi = normalize_doi(m.group(0))
        if not arxiv and self.raw:
            m = _ARXIV_IN_TEXT.search(self.raw)
            if m:
                arxiv = normalize_arxiv(m.group(1))

        return PaperRef(
            doi=doi,
            arxiv=arxiv,
            pmid=(self.pmid or "").strip() or None,
            title=(self.title or "").strip() or None,
            authors=[a.strip() for a in self.authors if a and a.strip()],
            year=self.year,
            raw=(self.raw or "").strip() or None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "doi": self.doi,
            "arxiv": self.arxiv,
            "pmid": self.pmid,
            "title": self.title,
            "authors": list(self.authors),
            "year": self.year,
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any] | PaperRef) -> PaperRef:
        if isinstance(d, PaperRef):
            return d
        authors = d.get("authors") or []
        if isinstance(authors, str):
            authors = [authors]
        return cls(
            doi=d.get("doi"),
            arxiv=d.get("arxiv"),
            pmid=d.get("pmid"),
            title=d.get("title"),
            authors=list(authors),
            year=d.get("year"),
            raw=d.get("raw"),
        )


@dataclass
class ResolvedRef:
    """What we confirmed via Crossref / S2 / arXiv."""

    doi: str | None = None
    arxiv: str | None = None
    pmid: str | None = None
    title: str | None = None
    authors: list[str] = field(default_factory=list)
    year: int | None = None
    journal: str | None = None
    ref: str | None = None  # acatome-store slug (only once ingested)
    score: float = 0.0  # 0..1 confidence
    source: str = ""  # "crossref" | "s2" | "arxiv" | "store"

    def to_dict(self) -> dict[str, Any]:
        return {
            "doi": self.doi,
            "arxiv": self.arxiv,
            "pmid": self.pmid,
            "title": self.title,
            "authors": list(self.authors),
            "year": self.year,
            "journal": self.journal,
            "ref": self.ref,
            "score": self.score,
            "source": self.source,
        }


@dataclass
class Candidate:
    """An alternative resolution when the request is ambiguous."""

    ref: ResolvedRef
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"ref": self.ref.to_dict(), "reason": self.reason}


@dataclass
class FetchAttempt:
    source: str
    url: str | None = None
    http_status: int | None = None
    at: datetime | None = None
    error: str | None = None
    success: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "url": self.url,
            "http_status": self.http_status,
            "at": self.at.isoformat() if self.at else None,
            "error": self.error,
            "success": self.success,
        }


@dataclass
class PaperRequest:
    """Full record of one paper request."""

    id: UUID
    created_at: datetime
    updated_at: datetime
    created_by: str | None
    source: dict[str, Any]  # {document?, line?, note?}
    input: PaperRef
    resolved: ResolvedRef
    candidates: list[Candidate]
    status: RequestStatus
    misconceptions: list[Misconception]
    attempts: list[FetchAttempt]
    priority: int
    not_before: datetime
    supersedes: UUID | None = None
    pdf_hash: str | None = None
    pdf_path: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "source": self.source,
            "input": self.input.to_dict(),
            "resolved": self.resolved.to_dict(),
            "candidates": [c.to_dict() for c in self.candidates],
            "status": self.status.value,
            "misconceptions": [m.to_dict() for m in self.misconceptions],
            "attempts": [a.to_dict() for a in self.attempts],
            "priority": self.priority,
            "not_before": self.not_before.isoformat(),
            "supersedes": str(self.supersedes) if self.supersedes else None,
            "pdf_hash": self.pdf_hash,
            "pdf_path": self.pdf_path,
            "last_error": self.last_error,
        }


# ---------------------------------------------------------------------------
# DOI / arXiv normalization
# ---------------------------------------------------------------------------

_DOI_IN_TEXT = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", re.IGNORECASE)
_ARXIV_IN_TEXT = re.compile(r"\barxiv[:\s]*(\d{4}\.\d{4,5}(?:v\d+)?)\b", re.IGNORECASE)
_DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
    "DOI:",
)


def normalize_doi(doi: str | None) -> str | None:
    """Strip URL/prefix and lower-case the registrant prefix.

    Per Crossref: DOIs are case-insensitive, but convention lowercases them.
    """
    if not doi:
        return None
    doi = doi.strip()
    for p in _DOI_PREFIXES:
        if doi.lower().startswith(p.lower()):
            doi = doi[len(p) :]
            break
    # Drop trailing punctuation that often leaks from citation text.
    doi = doi.rstrip(".,;)")
    if not doi:
        return None
    if not doi.startswith("10."):
        return None
    return doi.lower()


_ARXIV_ID_RE = re.compile(r"^\d{4}\.\d{4,5}(?:v\d+)?$", re.IGNORECASE)
_ARXIV_OLD_RE = re.compile(r"^[a-z\-]+/\d{7}(?:v\d+)?$", re.IGNORECASE)


def normalize_arxiv(arxiv: str | None) -> str | None:
    """Strip URL/prefix, strip version suffix for dedup keying."""
    if not arxiv:
        return None
    arxiv = arxiv.strip()
    for prefix in (
        "https://arxiv.org/abs/",
        "http://arxiv.org/abs/",
        "https://arxiv.org/pdf/",
        "http://arxiv.org/pdf/",
        "arxiv:",
        "arXiv:",
        "ARXIV:",
    ):
        if arxiv.lower().startswith(prefix.lower()):
            arxiv = arxiv[len(prefix) :]
            break
    if arxiv.lower().endswith(".pdf"):
        arxiv = arxiv[:-4]
    arxiv = arxiv.rstrip(".")
    if _ARXIV_ID_RE.match(arxiv) or _ARXIV_OLD_RE.match(arxiv):
        return arxiv.lower()
    return None
