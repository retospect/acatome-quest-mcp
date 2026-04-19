"""Shared business logic for submit / status / update.

The MCP server, the CLI, and the test suite all go through this module.
Keeping this layer transport-agnostic means a reviewer can drop a unit test
straight onto :class:`QuestService` without spinning up stdio MCP plumbing.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from .db import DB
from .dedup import StoreDedup, StoreHit
from .misconceptions import Misconception, MisconceptionCode
from .models import (
    TERMINAL_STATUSES,
    Candidate,
    PaperRef,
    PaperRequest,
    RequestStatus,
    ResolvedRef,
    UpdateMode,
)
from .resolver import Resolver

log = logging.getLogger(__name__)

MAX_OPEN_PER_AGENT = int(os.environ.get("QUEST_MAX_OPEN_PER_AGENT", "50"))


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

    async def submit(
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
            resolved, candidates, miscs = await self.resolver.resolve(pref)
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
            existing = await self.db.find_open_by_doi(dedup_doi)
            if existing:
                log.info("Idempotent submit: returning existing %s", existing.id)
                return existing
        dedup_arxiv = resolved.arxiv or pref.arxiv
        if not dedup_doi and dedup_arxiv:
            existing = await self.db.find_open_by_arxiv(dedup_arxiv)
            if existing:
                return existing

        # 5. Per-agent rate limit.
        if created_by:
            open_count = await self.db.count_open_for(created_by)
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
        return await self.db.insert(req)

    # -----------------------------------------------------------------
    # status
    # -----------------------------------------------------------------

    async def status(
        self,
        id: UUID | str | None = None,
        *,
        filter: dict[str, Any] | None = None,
    ) -> PaperRequest | list[PaperRequest]:
        if id is not None:
            uid = id if isinstance(id, UUID) else UUID(str(id))
            req = await self.db.get(uid)
            if not req:
                raise NotFoundError(f"no request with id {uid}")
            return req
        f = filter or {}
        return await self.db.find(
            status=f.get("status"),
            created_by=f.get("created_by"),
            has_misconception=f.get("has_misconception"),
            source_document=f.get("source_document"),
            limit=int(f.get("limit") or 100),
        )

    # -----------------------------------------------------------------
    # update
    # -----------------------------------------------------------------

    async def update(
        self,
        id: UUID | str,
        mode: UpdateMode | str,
        **kwargs: Any,
    ) -> PaperRequest:
        uid = id if isinstance(id, UUID) else UUID(str(id))
        m = UpdateMode(mode) if not isinstance(mode, UpdateMode) else mode
        req = await self.db.get(uid)
        if not req:
            raise NotFoundError(f"no request with id {uid}")
        if req.status in TERMINAL_STATUSES and m != UpdateMode.FLAG:
            raise ValueError(
                f"request {uid} is terminal ({req.status.value}); "
                f"only mode=flag is allowed"
            )

        if m == UpdateMode.CANCEL:
            updated = await self.db.update(uid, status=RequestStatus.CANCELLED)
        elif m == UpdateMode.PRIORITY:
            priority = int(kwargs.get("priority", 0))
            updated = await self.db.update(uid, priority=priority)
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
            updated = await self.db.update(uid, misconceptions=miscs)
        elif m == UpdateMode.CONFIRM:
            choice = int(kwargs.get("choice", -1))
            if choice < 0 or choice >= len(req.candidates):
                raise ValueError(
                    f"choice {choice} out of range (have {len(req.candidates)} candidates)"
                )
            picked = req.candidates[choice].ref
            updated = await self.db.update(
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
            resolved, candidates, miscs = await self.resolver.resolve(new_ref)
            all_miscs = list(req.misconceptions) + list(miscs)
            updated = await self.db.update(
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


__all__ = [
    "MAX_OPEN_PER_AGENT",
    "NotFoundError",
    "QuestService",
    "RateLimitError",
]
