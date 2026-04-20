"""Markdown exception report for requests needing manual acquisition.

Given a list of :class:`PaperRequest` (typically those in ``needs_user`` /
``failed`` / ``extract_failed`` status), produce a paste-ready document the
user can send to a librarian, interlibrary-loan desk, or use as a worklist.

The report is deliberately plain Markdown — no tables, no HTML — so it
survives Discord, email, plain-text ticketing systems, and printed output.
"""

from __future__ import annotations

from .misconceptions import MisconceptionCode, Severity
from .models import PaperRequest, RequestStatus


def render_report(
    requests: list[PaperRequest],
    *,
    title: str = "Papers needing manual acquisition",
) -> str:
    """Render ``requests`` as a Markdown exception report."""
    if not requests:
        return f"# {title}\n\n_No paper requests need manual acquisition._\n"

    lines: list[str] = [f"# {title} ({len(requests)})", ""]
    for n, req in enumerate(requests, start=1):
        lines.extend(_format_one(n, req))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------


def _format_one(n: int, req: PaperRequest) -> list[str]:
    citation = _format_citation(req)
    out: list[str] = [f"## {n}. {citation}"]

    doi = req.resolved.doi or req.input.doi
    arxiv = req.resolved.arxiv or req.input.arxiv
    if doi:
        out.append(f"- **DOI:** `{doi}` — https://doi.org/{doi}")
    if arxiv:
        out.append(f"- **arXiv:** `{arxiv}` — https://arxiv.org/abs/{arxiv}")
    if not doi and not arxiv and req.input.raw:
        out.append(f"- **Raw reference:** {req.input.raw}")

    out.append(f"- **Status:** `{req.status.value}` — {_explain(req)}")

    for m in req.misconceptions:
        label = _severity_label(m.severity)
        ev = m.evidence or "(no evidence recorded)"
        out.append(f"  - *{m.code.value}* [{label}]: {ev}")

    action = _suggest_action(req)
    if action:
        out.append(f"- **Action:** {action}")

    context_bits: list[str] = []
    src_doc = (req.source or {}).get("document")
    if src_doc:
        ref = src_doc
        line = (req.source or {}).get("line")
        if line:
            ref = f"{src_doc}:{line}"
        context_bits.append(f"cited in `{ref}`")
    if req.created_by:
        context_bits.append(f"requested by `{req.created_by}`")
    context_bits.append(f"first seen {req.created_at.date().isoformat()}")
    out.append(f"- **Context:** {'; '.join(context_bits)}")

    out.append(f"- **Request ID:** `{req.id}`")
    return out


def _format_citation(req: PaperRequest) -> str:
    title = req.resolved.title or req.input.title
    authors = req.resolved.authors or req.input.authors
    year = req.resolved.year or req.input.year
    journal = req.resolved.journal

    bits: list[str] = []
    if authors:
        bits.append(_format_authors(authors))
    if year:
        bits.append(str(year))
    if title:
        bits.append(f'"{title}"')
    if journal:
        bits.append(f"*{journal}*")
    if bits:
        return ", ".join(bits)

    # Nothing resolved — fall back to whatever identifier we have.
    if req.input.doi:
        return f"DOI {req.input.doi}"
    if req.input.arxiv:
        return f"arXiv:{req.input.arxiv}"
    if req.input.raw:
        return req.input.raw
    return "(unidentified request)"


def _format_authors(authors: list[str]) -> str:
    if len(authors) == 1:
        return authors[0]
    if len(authors) == 2:
        return f"{authors[0]} & {authors[1]}"
    return f"{authors[0]} et al."


def _explain(req: PaperRequest) -> str:
    if req.status == RequestStatus.NEEDS_USER:
        return "resolver flagged a problem that needs a human decision"
    if req.status == RequestStatus.FAILED:
        return req.last_error or "no open-access source found"
    if req.status == RequestStatus.EXTRACT_FAILED:
        return req.last_error or "PDF fetched but extraction failed"
    if req.status == RequestStatus.CANCELLED:
        return "cancelled by user"
    return req.status.value


def _severity_label(sev: Severity) -> str:
    # Plain ASCII — survives every downstream renderer.
    return sev.value.upper()


def _suggest_action(req: PaperRequest) -> str:
    codes = {m.code for m in req.misconceptions}
    rid = req.id

    if MisconceptionCode.RETRACTED in codes:
        return (
            "paper was retracted — do not cite. If already cited, remove or annotate."
        )

    if MisconceptionCode.DOI_TITLE_MISMATCH in codes:
        parts = [
            "the DOI and title disagree. If the resolver found a candidate you "
            f"recognise, pick it with `acatome-quest update {rid} confirm "
            "--choice N`. If the DOI was wrong, correct it with "
            f"`acatome-quest update {rid} repoint --doi <correct>`."
        ]
        return " ".join(parts)

    if (
        MisconceptionCode.DOI_INVALID in codes
        or MisconceptionCode.DOI_TRUNCATED in codes
    ):
        return (
            f"DOI did not resolve — correct it with "
            f"`acatome-quest update {rid} repoint --doi <correct>`."
        )

    if MisconceptionCode.TITLE_NOT_FOUND in codes:
        return (
            "no metadata match was found. Please supply a DOI, arXiv ID, or a "
            "full citation with year and co-authors, then re-submit."
        )

    if MisconceptionCode.DUPLICATE_OF in codes:
        # Should have been caught earlier — include for completeness.
        return "already present in the store under another slug."

    if req.status == RequestStatus.FAILED:
        return (
            "request via your library's interlibrary loan, or drop the PDF "
            "manually into `~/.acatome/inbox/`. Corresponding-author email "
            "is also worth a try if an open-access version may exist."
        )

    if req.status == RequestStatus.EXTRACT_FAILED:
        return (
            "PDF was fetched but extraction failed. Inspect the file under "
            f"`{req.pdf_path or '<pdf_path unknown>'}` manually."
        )

    return ""


__all__ = ["render_report"]
