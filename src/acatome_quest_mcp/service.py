"""Shared business logic for submit / status / update.

The MCP server, the CLI, and the test suite all go through this module.
Keeping this layer transport-agnostic means a reviewer can drop a unit test
straight onto :class:`QuestService` without spinning up stdio MCP plumbing.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from .db import DB
from .dedup import StoreDedup, StoreHit
from .misconceptions import Misconception, MisconceptionCode
from .models import (
    TERMINAL_STATUSES,
    Candidate,
    FetchAttempt,
    PaperRef,
    PaperRequest,
    RequestStatus,
    ResolvedRef,
    UpdateMode,
)
from .resolver import Resolver

if TYPE_CHECKING:
    import httpx

log = logging.getLogger(__name__)

MAX_OPEN_PER_AGENT = int(os.environ.get("QUEST_MAX_OPEN_PER_AGENT", "50"))

# Maximum accepted size for user-dropped PDFs (default 50 MB).  A runaway
# Discord upload or a pointed-at-disk URL should not eat the disk.
MAX_PDF_SIZE = int(os.environ.get("QUEST_MAX_PDF_SIZE", str(50 * 1024 * 1024)))

_SAFE_FILENAME = re.compile(r"[^a-zA-Z0-9._-]+")

# Source statuses that are not eligible for a user-dropped PDF.  An already-
# ingested request is closed; attaching a new PDF is nonsense.  A cancelled
# request was explicitly abandoned — reopen via submit() instead.
_SUBMIT_FILE_REFUSED: frozenset[RequestStatus] = frozenset(
    {
        RequestStatus.INGESTED,
        RequestStatus.FOUND_IN_STORE,
        RequestStatus.CANCELLED,
    }
)


class RateLimitError(Exception):
    """Raised when a caller has too many open requests."""


class NotFoundError(Exception):
    """Raised when a request id is unknown."""


class QuestService:
    """Stateless orchestrator over DB + resolver + dedup."""

    def __init__(
        self,
        db: DB,
        resolver: Resolver | None = None,
        dedup: StoreDedup | None = None,
    ) -> None:
        self.db = db
        self.resolver = resolver or Resolver()
        self.dedup = dedup if dedup is not None else StoreDedup()

    # -----------------------------------------------------------------
    # submit
    # -----------------------------------------------------------------

    def submit(
        self,
        ref: PaperRef | dict[str, Any],
        *,
        dry_run: bool = False,
        source: dict[str, Any] | None = None,
        priority: int = 0,
        created_by: str | None = None,
    ) -> PaperRequest:
        """Resolve + optionally queue.  Idempotent on DOI.

        Returns the ``PaperRequest`` (either freshly created, or a pre-existing
        open row for the same DOI).  On ``dry_run``, does not persist.
        """
        pref = PaperRef.from_dict(ref).normalize()
        if pref.is_empty():
            raise ValueError(
                "submit requires at least one of doi / arxiv / pmid / title / raw"
            )

        # 0. Dedup against store — cheapest possible hit.  If the store has
        # this paper, its metadata is authoritative and we skip the network
        # roundtrip to Crossref / S2 entirely.
        store_hit = self._dedup(pref)
        candidates: list[Candidate] = []
        miscs: list[Misconception] = []

        if store_hit:
            resolved = store_hit.to_resolved()
        else:
            # 1. Resolve via Crossref / S2.
            resolved, candidates, miscs = self.resolver.resolve(pref)
            # 2. Second dedup pass: resolver may have discovered a DOI we
            # didn't know about.
            if resolved.doi:
                store_hit = self._dedup_by(resolved.doi, resolved.arxiv)
                if store_hit:
                    resolved = store_hit.to_resolved()

        if store_hit and pref.doi and store_hit.doi and pref.doi != store_hit.doi:
            miscs.append(
                Misconception.of(
                    MisconceptionCode.DUPLICATE_OF,
                    evidence=(
                        f"store already has this paper under slug "
                        f"'{store_hit.slug}' with DOI {store_hit.doi}"
                    ),
                )
            )

        # 3. Dry-run: return a synthesized, non-persisted record.
        if dry_run:
            return _synth(
                pref, resolved, candidates, miscs, source, priority, created_by
            )

        # 4. Idempotency — do not create a duplicate open request.
        dedup_doi = resolved.doi or pref.doi
        if dedup_doi:
            existing = self.db.find_open_by_doi(dedup_doi)
            if existing:
                log.info("Idempotent submit: returning existing %s", existing.id)
                return existing
        dedup_arxiv = resolved.arxiv or pref.arxiv
        if not dedup_doi and dedup_arxiv:
            existing = self.db.find_open_by_arxiv(dedup_arxiv)
            if existing:
                return existing

        # 5. Per-agent rate limit.
        if created_by:
            open_count = self.db.count_open_for(created_by)
            if open_count >= MAX_OPEN_PER_AGENT:
                raise RateLimitError(
                    f"{created_by} already has {open_count} open requests "
                    f"(limit {MAX_OPEN_PER_AGENT})"
                )

        # 6. Pick status.
        status = (
            RequestStatus.FOUND_IN_STORE
            if store_hit
            else (
                RequestStatus.NEEDS_USER
                if _needs_user(resolved, miscs)
                else RequestStatus.QUEUED
            )
        )

        now = datetime.now(UTC)
        req = PaperRequest(
            id=uuid4(),  # placeholder, DB default will replace
            created_at=now,
            updated_at=now,
            created_by=created_by,
            source=source or {},
            input=pref,
            resolved=resolved,
            candidates=candidates,
            status=status,
            misconceptions=miscs,
            attempts=[],
            priority=priority,
            not_before=now,
        )
        return self.db.insert(req)

    # -----------------------------------------------------------------
    # status
    # -----------------------------------------------------------------

    def status(
        self,
        id: UUID | str | None = None,
        *,
        filter: dict[str, Any] | None = None,
    ) -> PaperRequest | list[PaperRequest]:
        if id is not None:
            uid = id if isinstance(id, UUID) else UUID(str(id))
            req = self.db.get(uid)
            if not req:
                raise NotFoundError(f"no request with id {uid}")
            return req
        f = filter or {}
        return self.db.find(
            status=f.get("status"),
            created_by=f.get("created_by"),
            has_misconception=f.get("has_misconception"),
            source_document=f.get("source_document"),
            limit=int(f.get("limit") or 100),
        )

    # -----------------------------------------------------------------
    # submit_file
    # -----------------------------------------------------------------

    def submit_file(
        self,
        *,
        url: str | None = None,
        content: bytes | None = None,
        filename: str | None = None,
        request_id: UUID | str | None = None,
        ref: PaperRef | dict[str, Any] | None = None,
        created_by: str | None = None,
        inbox: Path | None = None,
        http: httpx.Client | None = None,
    ) -> PaperRequest:
        """Attach a user-supplied PDF to a request.

        Takes a PDF either as a ``url`` (e.g. a Discord CDN link) or as raw
        ``content`` bytes.  Writes the file to the extractor's inbox and flips
        the associated request to :attr:`RequestStatus.INGESTING`.

        Exactly one of ``request_id`` and ``ref`` must be given:

        - ``request_id``: attach to an existing request. Reopens ``failed``,
          ``extract_failed`` or ``needs_user`` requests.  Refuses terminal-
          success or cancelled requests.
        - ``ref``: synthesize a new request first, then attach. The ``ref``
          follows the same shape as :meth:`submit`.

        The reconciler picks up the ingested bundle from ``acatome-store`` and
        closes the request once the DOI matches. If the PDF turned out to be
        the wrong paper, the reconciler attaches a :class:`PDF_MISMATCH`
        misconception instead of closing.
        """
        if (request_id is None) == (ref is None):
            raise ValueError("submit_file requires exactly one of request_id or ref")
        if (url is None) == (content is None):
            raise ValueError("submit_file requires exactly one of url or content")

        pdf_bytes = self._load_pdf(url=url, content=content, http=http)
        _validate_pdf(pdf_bytes)
        sha = hashlib.sha256(pdf_bytes).hexdigest()

        # Resolve target request.
        if request_id is not None:
            uid = request_id if isinstance(request_id, UUID) else UUID(str(request_id))
            req = self.db.get(uid)
            if req is None:
                raise NotFoundError(f"no request with id {uid}")
            if req.status in _SUBMIT_FILE_REFUSED:
                raise ValueError(
                    f"request {uid} is {req.status.value}; refusing to attach a new PDF"
                )
        else:
            req = self.submit(
                ref,  # type: ignore[arg-type]
                created_by=created_by,
            )
            # If the paper was already in the store, we do not need to ingest
            # the PDF the user dropped — surface the existing slug instead.
            if req.status == RequestStatus.FOUND_IN_STORE:
                return req

        # Write to inbox.
        target_inbox = _resolve_inbox(inbox)
        target_inbox.mkdir(parents=True, exist_ok=True)
        stem = _safe_stem(filename, req)
        pdf_path = target_inbox / f"{sha[:12]}_{stem}.pdf"
        pdf_path.write_bytes(pdf_bytes)
        log.info(
            "submit_file: wrote %s (%d bytes) for request %s",
            pdf_path,
            len(pdf_bytes),
            req.id,
        )

        # Record the drop as a FetchAttempt so status output shows provenance
        # (useful when debugging: "did this PDF come from the runner or the
        # user?").
        attempts = [
            *req.attempts,
            FetchAttempt(
                source="user_upload",
                url=url,
                at=datetime.now(UTC),
                success=True,
            ),
        ]

        updated = self.db.update(
            req.id,
            status=RequestStatus.INGESTING,
            pdf_hash=sha,
            pdf_path=str(pdf_path),
            attempts=attempts,
            last_error=None,
        )
        assert updated is not None
        return updated

    def _load_pdf(
        self,
        *,
        url: str | None,
        content: bytes | None,
        http: httpx.Client | None,
    ) -> bytes:
        if content is not None:
            return content
        assert url is not None  # checked by caller
        close_http = http is None
        if http is None:
            import httpx as _httpx

            http = _httpx.Client(
                timeout=60.0,
                headers={"User-Agent": "acatome-quest-mcp/submit_file"},
            )
        try:
            resp = http.get(url, follow_redirects=True)
            resp.raise_for_status()
            return resp.content
        finally:
            if close_http:
                http.close()

    # -----------------------------------------------------------------
    # update
    # -----------------------------------------------------------------

    def update(
        self,
        id: UUID | str,
        mode: UpdateMode | str,
        **kwargs: Any,
    ) -> PaperRequest:
        uid = id if isinstance(id, UUID) else UUID(str(id))
        m = UpdateMode(mode) if not isinstance(mode, UpdateMode) else mode
        req = self.db.get(uid)
        if not req:
            raise NotFoundError(f"no request with id {uid}")
        if req.status in TERMINAL_STATUSES and m != UpdateMode.FLAG:
            raise ValueError(
                f"request {uid} is terminal ({req.status.value}); "
                f"only mode=flag is allowed"
            )

        if m == UpdateMode.CANCEL:
            updated = self.db.update(uid, status=RequestStatus.CANCELLED)
        elif m == UpdateMode.PRIORITY:
            priority = int(kwargs.get("priority", 0))
            updated = self.db.update(uid, priority=priority)
        elif m == UpdateMode.FLAG:
            code = kwargs.get("code")
            if not code:
                raise ValueError("mode=flag requires code=<MisconceptionCode>")
            severity = kwargs.get("severity")
            evidence = kwargs.get("evidence", "")
            misc = Misconception.of(
                code, evidence=evidence, severity=severity, source="user"
            )
            miscs = [*req.misconceptions, misc]
            updated = self.db.update(uid, misconceptions=miscs)
        elif m == UpdateMode.CONFIRM:
            choice = int(kwargs.get("choice", -1))
            if choice < 0 or choice >= len(req.candidates):
                raise ValueError(
                    f"choice {choice} out of range (have {len(req.candidates)} candidates)"
                )
            picked = req.candidates[choice].ref
            updated = self.db.update(
                uid,
                resolved=picked,
                status=RequestStatus.QUEUED,
                candidates=[],
            )
        elif m == UpdateMode.REPOINT:
            new_doi = kwargs.get("doi")
            if not new_doi:
                raise ValueError("mode=repoint requires doi=<new DOI>")
            # Re-resolve under the new DOI.
            new_ref = PaperRef(doi=new_doi).normalize()
            resolved, candidates, miscs = self.resolver.resolve(new_ref)
            all_miscs = list(req.misconceptions) + list(miscs)
            updated = self.db.update(
                uid,
                resolved=resolved,
                candidates=candidates,
                misconceptions=all_miscs,
                status=RequestStatus.QUEUED,
            )
        else:
            raise ValueError(f"unknown update mode: {m}")

        assert updated is not None  # db.get() just confirmed existence
        return updated

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _dedup(self, pref: PaperRef) -> StoreHit | None:
        return self._dedup_by(pref.doi, pref.arxiv)

    def _dedup_by(self, doi: str | None, arxiv: str | None) -> StoreHit | None:
        if not self.dedup.enabled:
            return None
        if doi:
            hit = self.dedup.lookup_by_doi(doi)
            if hit:
                return hit
        if arxiv:
            hit = self.dedup.lookup_by_arxiv(arxiv)
            if hit:
                return hit
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _needs_user(resolved: ResolvedRef, miscs: list[Misconception]) -> bool:
    """A request needs user action when we have zero confidence in the
    resolution, or when a critical misconception makes automated fetch
    unsafe (wrong DOI, fabrication suspect)."""
    if resolved.score <= 0.0:
        return True
    for m in miscs:
        if m.code in (
            MisconceptionCode.DOI_TITLE_MISMATCH,
            MisconceptionCode.TITLE_NOT_FOUND,
        ):
            return True
    return False


def _synth(
    pref: PaperRef,
    resolved: ResolvedRef,
    candidates: list[Candidate],
    miscs: list[Misconception],
    source: dict[str, Any] | None,
    priority: int,
    created_by: str | None,
) -> PaperRequest:
    now = datetime.now(UTC)
    status = (
        RequestStatus.FOUND_IN_STORE
        if resolved.source == "store"
        else (
            RequestStatus.NEEDS_USER
            if _needs_user(resolved, miscs)
            else RequestStatus.QUEUED
        )
    )
    return PaperRequest(
        id=uuid4(),
        created_at=now,
        updated_at=now,
        created_by=created_by,
        source=source or {},
        input=pref,
        resolved=resolved,
        candidates=candidates,
        status=status,
        misconceptions=miscs,
        attempts=[],
        priority=priority,
        not_before=now,
    )


def _validate_pdf(content: bytes) -> None:
    """Raise ``ValueError`` if ``content`` is not a plausible PDF.

    Checks the magic bytes (``%PDF-``) and enforces ``MAX_PDF_SIZE``.
    """
    if len(content) == 0:
        raise ValueError("submit_file received empty content")
    if not content.startswith(b"%PDF-"):
        raise ValueError(
            "content is not a PDF (missing %PDF- magic bytes). "
            "Discord CDN URLs occasionally redirect to an HTML error page — "
            "verify the attachment URL is fresh."
        )
    if len(content) > MAX_PDF_SIZE:
        raise ValueError(
            f"PDF is {len(content)} bytes; exceeds limit of {MAX_PDF_SIZE} bytes. "
            f"Override with QUEST_MAX_PDF_SIZE if you really mean it."
        )


def _resolve_inbox(inbox: Path | None) -> Path:
    """Default to the runner's inbox.  Imported lazily to avoid a circular
    dependency at module-load time (runner imports service for reconcile)."""
    if inbox is not None:
        return inbox
    from .runner import DEFAULT_INBOX

    return DEFAULT_INBOX


def _safe_stem(filename: str | None, req: PaperRequest) -> str:
    """Build a filesystem-safe stem for the written PDF."""
    if filename:
        base = Path(filename).stem
        cleaned = _SAFE_FILENAME.sub("_", base).strip("_")
        if cleaned:
            return cleaned.lower()[:80]
    # Fall back to the runner's author_year convention.
    from .runner import _filename_stem

    return _filename_stem(req)


__all__ = [
    "MAX_OPEN_PER_AGENT",
    "MAX_PDF_SIZE",
    "NotFoundError",
    "QuestService",
    "RateLimitError",
]
