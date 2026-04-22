"""MCP server — four tools: ``submit``, ``status``, ``update``, ``submit_file``.

Thin wrapper around :class:`QuestService`.  All business logic lives in
``service.py``; this file handles stdio plumbing and dict↔dataclass coercion
for the MCP contract.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

from mcp.server.fastmcp import FastMCP

from .db import DB
from .service import NotFoundError, QuestService, RateLimitError

log = logging.getLogger(__name__)

mcp = FastMCP(
    "acatome-quest-mcp",
    instructions=(
        "Paper-request queue for scientific papers.  Submit a DOI / arXiv id / "
        "title / raw citation; Quest resolves metadata (Crossref + S2 + arXiv), "
        "dedups against the local paper store, flags misconceptions (broken "
        "DOI, DOI↔title mismatch, duplicate of existing slug), and fetches "
        "PDFs from open-access sources only (arXiv, Unpaywall, …).  All calls "
        "are non-blocking: slow downloads + extraction happen out of band.  If "
        "a request is not `ingested`, never fabricate quotes from the title "
        "or abstract — cite the DOI only and wait."
    ),
)

_db: DB | None = None
_service: QuestService | None = None


def _get_service() -> QuestService:
    global _db, _service
    if _service is not None:
        return _service
    dsn = os.environ.get("DATABASE_URL", "postgresql://localhost/cluster")
    schema = os.environ.get("QUEST_SCHEMA", "papers")
    _db = DB(dsn, schema=schema)
    _db.connect()
    _db.migrate()
    _service = QuestService(_db)
    return _service


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def submit(
    ref: dict[str, Any],
    dry_run: bool = False,
    source: dict[str, Any] | None = None,
    priority: int = 0,
    created_by: str | None = None,
) -> dict[str, Any]:
    """Submit a paper request.  Resolves metadata, checks dedup, queues for fetch.

    Args:
        ref: The paper identifier.  Any subset of:
            ``{doi, arxiv, pmid, title, authors, year, raw}``.
            ``raw`` is a free-form citation string — Quest will try to extract
            a DOI or arXiv id from it.
        dry_run: If True, resolve and return candidates but do not persist.
        source: Provenance — e.g. ``{"document": "ch02.tex", "line": 147}``.
        priority: Higher = served first by the runner.  Default 0.
        created_by: Agent slug or user id.  Used for per-agent rate-limiting.

    Returns:
        The full request record, including resolved metadata, candidates, and
        misconceptions.  Status will be one of:
        ``found_in_store`` (already have it), ``queued`` (waiting for runner),
        ``needs_user`` (disambiguation or bad DOI required manual action).

    Idempotent: calling submit twice with the same DOI while the first request
    is still open returns the *same* request id.

    Do not fabricate quotes from a paper whose status is not ``ingested`` or
    ``found_in_store``.
    """
    svc = _get_service()
    try:
        req = svc.submit(
            ref,
            dry_run=dry_run,
            source=source,
            priority=priority,
            created_by=created_by,
        )
    except ValueError as exc:
        return {"error": str(exc)}
    except RateLimitError as exc:
        return {"error": "rate_limit", "detail": str(exc)}
    return _card(req)


@mcp.tool()
def status(
    id: str | None = None,
    filter: dict[str, Any] | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Read one or many paper requests.

    Args:
        id: A request id (uuid string).  If given, ``filter`` is ignored.
        filter: Narrow the list of rows returned.  Supported keys:
            ``status``, ``created_by``, ``has_misconception`` (bool),
            ``source_document``, ``limit``.

    Returns:
        A single card (when ``id`` is set) or a list of cards.
    """
    svc = _get_service()
    try:
        out = svc.status(id, filter=filter)
    except NotFoundError as exc:
        return {"error": "not_found", "detail": str(exc)}
    if isinstance(out, list):
        return [_card(r) for r in out]
    return _card(out)


@mcp.tool()
def update(
    id: str,
    mode: str,
    choice: int | None = None,
    doi: str | None = None,
    code: str | None = None,
    severity: str | None = None,
    evidence: str | None = None,
    priority: int | None = None,
) -> dict[str, Any]:
    """Mutate a request.  Single verb with an explicit ``mode`` (see
    ``precis.put`` for the same pattern).

    Modes:
        confirm:  pick one of ``candidates`` by index.  Requires ``choice``.
        repoint:  replace the DOI (user-corrected) and re-resolve.  Requires ``doi``.
        flag:     attach a misconception.  Requires ``code``; optional ``severity``, ``evidence``.
        priority: change the runner priority.  Requires ``priority``.
        cancel:   terminate the request.
    """
    svc = _get_service()
    kwargs: dict[str, Any] = {}
    if choice is not None:
        kwargs["choice"] = choice
    if doi is not None:
        kwargs["doi"] = doi
    if code is not None:
        kwargs["code"] = code
    if severity is not None:
        kwargs["severity"] = severity
    if evidence is not None:
        kwargs["evidence"] = evidence
    if priority is not None:
        kwargs["priority"] = priority
    try:
        req = svc.update(id, mode, **kwargs)
    except NotFoundError as exc:
        return {"error": "not_found", "detail": str(exc)}
    except ValueError as exc:
        return {"error": "invalid", "detail": str(exc)}
    return _card(req)


@mcp.tool()
def submit_file(
    url: str | None = None,
    content_base64: str | None = None,
    filename: str | None = None,
    request_id: str | None = None,
    ref: dict[str, Any] | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    """Attach a user-supplied PDF to a paper request.

    Use this when a user drops a PDF (e.g. a Discord attachment) for a paper
    that Quest could not fetch automatically, or to pre-load a PDF you
    already have on disk.

    Args:
        url: A direct HTTP(S) link to the PDF.  Must resolve to a fresh file
            (follow-redirects is enabled, so short-lived Discord CDN URLs
            work).  Mutually exclusive with ``content_base64``.
        content_base64: Base64-encoded PDF bytes, for agents that already
            have the file in memory.  Prefer ``url`` when available so the
            provenance URL is recorded on the request.
        filename: Optional filename hint, used when naming the file written
            to the inbox.  Falls back to the request's author/year.
        request_id: Attach to an existing request (preferred).  Reopens
            ``failed``, ``extract_failed``, or ``needs_user`` requests.
            Refuses to overwrite already-closed or cancelled requests.
        ref: Create a new request from this reference (same shape as
            :func:`submit`) and attach the PDF.  Use this when the paper
            isn't already being tracked.
        created_by: Agent or user id, used when ``ref`` is given.

    Returns:
        The full request record, flipped to ``ingesting``.  The background
        runner reconciles with ``acatome-store`` once ``acatome-extract``
        has done its work.

    Exactly one of ``url`` / ``content_base64`` is required, and exactly
    one of ``request_id`` / ``ref``.
    """
    svc = _get_service()
    content: bytes | None = None
    if content_base64:
        import base64

        try:
            content = base64.b64decode(content_base64, validate=True)
        except Exception as exc:
            return {"error": "invalid", "detail": f"bad base64: {exc}"}
    try:
        req = svc.submit_file(
            url=url,
            content=content,
            filename=filename,
            request_id=request_id,
            ref=ref,
            created_by=created_by,
        )
    except NotFoundError as exc:
        return {"error": "not_found", "detail": str(exc)}
    except ValueError as exc:
        return {"error": "invalid", "detail": str(exc)}
    except RateLimitError as exc:
        return {"error": "rate_limit", "detail": str(exc)}
    except Exception as exc:  # network errors, disk write errors
        log.exception("submit_file failed")
        return {"error": "io", "detail": str(exc)}
    return _card(req)


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _card(req: Any) -> dict[str, Any]:
    """Compact summary card optimized for MCP responses."""
    d = req.to_dict()
    # Strip transient fields that clutter the agent view unless asked.
    return {
        "id": d["id"],
        "status": d["status"],
        "resolved": d["resolved"],
        "candidates": d["candidates"],
        "misconceptions": d["misconceptions"],
        "priority": d["priority"],
        "source": d["source"],
        "created_by": d["created_by"],
        "created_at": d["created_at"],
        "updated_at": d["updated_at"],
        "last_error": d["last_error"],
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("QUEST_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    mcp.run()


if __name__ == "__main__":
    main()
