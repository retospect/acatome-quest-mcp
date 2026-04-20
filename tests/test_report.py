"""Tests for the markdown exception report renderer."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from acatome_quest_mcp.misconceptions import Misconception, MisconceptionCode, Severity
from acatome_quest_mcp.models import (
    PaperRef,
    PaperRequest,
    RequestStatus,
    ResolvedRef,
)
from acatome_quest_mcp.report import render_report


def _req(
    *,
    status: RequestStatus = RequestStatus.FAILED,
    doi: str | None = "10.1/sample",
    arxiv: str | None = None,
    title: str | None = "Sample paper",
    authors: list[str] | None = None,
    year: int | None = 2024,
    journal: str | None = "Journal of Examples",
    raw: str | None = None,
    misconceptions: list[Misconception] | None = None,
    last_error: str | None = None,
    source: dict | None = None,
    created_by: str | None = None,
) -> PaperRequest:
    now = datetime(2025, 1, 15, 12, 0, tzinfo=UTC)
    return PaperRequest(
        id=uuid4(),
        created_at=now,
        updated_at=now,
        created_by=created_by,
        source=source or {},
        input=PaperRef(doi=doi, arxiv=arxiv, title=title, raw=raw),
        resolved=ResolvedRef(
            doi=doi,
            arxiv=arxiv,
            title=title,
            authors=list(authors or ["Alice", "Bob"]),
            year=year,
            journal=journal,
            score=1.0 if doi else 0.0,
            source="crossref" if doi else "",
        ),
        candidates=[],
        status=status,
        misconceptions=list(misconceptions or []),
        attempts=[],
        priority=0,
        not_before=now,
        last_error=last_error,
    )


# ---------------------------------------------------------------------------


def test_empty_report_is_friendly() -> None:
    out = render_report([])
    assert "# Papers needing manual acquisition" in out
    assert "No paper requests" in out


def test_header_counts_entries() -> None:
    out = render_report([_req(), _req(doi="10.1/b")])
    assert out.startswith("# Papers needing manual acquisition (2)")


def test_citation_and_identifiers_rendered() -> None:
    out = render_report(
        [
            _req(
                doi="10.1234/foo",
                title="Anion exchange membranes for ammonia synthesis",
                authors=["Alice Q. Smith", "Bob Jones", "Carol Wu"],
                year=2024,
                journal="JACS",
            )
        ]
    )
    assert "Alice Q. Smith et al." in out
    assert "2024" in out
    assert '"Anion exchange membranes for ammonia synthesis"' in out
    assert "*JACS*" in out
    assert "`10.1234/foo`" in out
    assert "https://doi.org/10.1234/foo" in out


def test_arxiv_link_rendered() -> None:
    out = render_report([_req(doi=None, arxiv="2508.20254")])
    assert "https://arxiv.org/abs/2508.20254" in out
    assert "`2508.20254`" in out


def test_raw_reference_falls_through_when_no_identifiers() -> None:
    out = render_report(
        [
            _req(
                doi=None,
                arxiv=None,
                title=None,
                authors=[],
                year=None,
                journal=None,
                raw="Smith et al., some paper somewhere, 2024",
            )
        ]
    )
    assert "Smith et al., some paper somewhere, 2024" in out


def test_retracted_gets_terminal_action() -> None:
    misc = Misconception.of(
        MisconceptionCode.RETRACTED, evidence="Crossref retraction notice"
    )
    out = render_report([_req(status=RequestStatus.NEEDS_USER, misconceptions=[misc])])
    assert "retracted" in out.lower()
    assert "do not cite" in out.lower()
    assert "retraction notice" in out


def test_doi_title_mismatch_suggests_confirm_or_repoint() -> None:
    misc = Misconception.of(
        MisconceptionCode.DOI_TITLE_MISMATCH,
        evidence="DOI resolves to 'Photocatalytic H2', title says 'Thermal decomposition'",
    )
    req = _req(status=RequestStatus.NEEDS_USER, misconceptions=[misc])
    out = render_report([req])
    assert "confirm" in out
    assert "repoint" in out
    assert str(req.id) in out


def test_failed_suggests_ill_and_inbox() -> None:
    req = _req(
        status=RequestStatus.FAILED,
        last_error="no OA copy found (arXiv: 404, Unpaywall: no best_oa_location)",
    )
    out = render_report([req])
    assert "interlibrary loan" in out.lower() or "ill" in out.lower()
    assert "~/.acatome/inbox/" in out
    assert "no OA copy found" in out


def test_source_document_and_requester_shown() -> None:
    req = _req(
        source={"document": "ch04.tex", "line": 123},
        created_by="writer",
    )
    out = render_report([req])
    assert "`ch04.tex:123`" in out
    assert "`writer`" in out


def test_severity_label_uppercase_ascii() -> None:
    misc = Misconception(
        code=MisconceptionCode.DOI_INVALID,
        severity=Severity.CRITICAL,
        evidence="DOI 10.1/bogus returned 404",
    )
    out = render_report([_req(misconceptions=[misc])])
    assert "[CRITICAL]" in out


def test_output_ends_with_single_newline() -> None:
    out = render_report([_req()])
    assert out.endswith("\n")
    assert not out.endswith("\n\n\n")
