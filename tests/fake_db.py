"""In-memory FakeDB implementing the surface of :class:`acatome_quest_mcp.db.DB`.

Lets unit tests exercise the full submit/status/update flow without needing
postgres or psycopg.  The FakeDB is intentionally dumb — it has no
migrations, no ``FOR UPDATE SKIP LOCKED`` semantics, no JSON round-trip.
It stores :class:`PaperRequest` dataclasses directly.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from acatome_quest_mcp.models import (
    OPEN_STATUSES,
    PaperRequest,
    RequestStatus,
)


class FakeDB:
    schema = "papers"

    def __init__(self) -> None:
        self._rows: dict[UUID, PaperRequest] = {}

    def connect(self) -> None:  # pragma: no cover - no-op
        pass

    def close(self) -> None:  # pragma: no cover - no-op
        pass

    def migrate(self) -> None:  # pragma: no cover - no-op
        pass

    def insert(self, req: PaperRequest) -> PaperRequest:
        stored = replace(req, id=uuid4())
        self._rows[stored.id] = stored
        return stored

    def update(self, id: UUID, **fields: Any) -> PaperRequest | None:
        if id not in self._rows:
            return None
        row = self._rows[id]
        changes: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if "status" in fields:
            st = fields["status"]
            changes["status"] = (
                st if isinstance(st, RequestStatus) else RequestStatus(st)
            )
        if "priority" in fields:
            changes["priority"] = int(fields["priority"])
        if "not_before" in fields:
            changes["not_before"] = fields["not_before"]
        if "source" in fields:
            changes["source"] = fields["source"]
        if "resolved" in fields:
            changes["resolved"] = fields["resolved"]
        if "misconceptions" in fields:
            changes["misconceptions"] = list(fields["misconceptions"])
        if "attempts" in fields:
            changes["attempts"] = list(fields["attempts"])
        if "candidates" in fields:
            changes["candidates"] = list(fields["candidates"])
        if "pdf_hash" in fields:
            changes["pdf_hash"] = fields["pdf_hash"]
        if "pdf_path" in fields:
            changes["pdf_path"] = fields["pdf_path"]
        if "last_error" in fields:
            changes["last_error"] = fields["last_error"]
        if "resolved_ref" in fields:
            changes["resolved"] = replace(row.resolved, ref=fields["resolved_ref"])
        self._rows[id] = replace(row, **changes)
        return self._rows[id]

    def get(self, id: UUID) -> PaperRequest | None:
        return self._rows.get(id)

    def find_open_by_doi(self, doi: str) -> PaperRequest | None:
        matches = [
            r
            for r in self._rows.values()
            if r.status in OPEN_STATUSES
            and (r.input.doi == doi or r.resolved.doi == doi)
        ]
        if not matches:
            return None
        matches.sort(key=lambda r: r.created_at, reverse=True)
        return matches[0]

    def find_open_by_arxiv(self, arxiv: str) -> PaperRequest | None:
        matches = [
            r
            for r in self._rows.values()
            if r.status in OPEN_STATUSES
            and (r.input.arxiv == arxiv or r.resolved.arxiv == arxiv)
        ]
        if not matches:
            return None
        matches.sort(key=lambda r: r.created_at, reverse=True)
        return matches[0]

    def count_open_for(self, created_by: str) -> int:
        return sum(
            1
            for r in self._rows.values()
            if r.created_by == created_by and r.status in OPEN_STATUSES
        )

    def find(
        self,
        *,
        status: Any = None,
        created_by: str | None = None,
        has_misconception: bool | None = None,
        source_document: str | None = None,
        limit: int = 100,
    ) -> list[PaperRequest]:
        out = list(self._rows.values())
        if status is not None:
            s = status if isinstance(status, RequestStatus) else RequestStatus(status)
            out = [r for r in out if r.status == s]
        if created_by is not None:
            out = [r for r in out if r.created_by == created_by]
        if has_misconception is True:
            out = [r for r in out if r.misconceptions]
        elif has_misconception is False:
            out = [r for r in out if not r.misconceptions]
        if source_document is not None:
            out = [
                r for r in out if (r.source or {}).get("document") == source_document
            ]
        out.sort(key=lambda r: r.created_at, reverse=True)
        return out[:limit]

    def claim_queued(self, limit: int = 1) -> list[PaperRequest]:
        now = datetime.now(UTC)
        ready = [
            r
            for r in self._rows.values()
            if r.status == RequestStatus.QUEUED and r.not_before <= now
        ]
        ready.sort(key=lambda r: (-r.priority, r.created_at))
        picked = ready[:limit]
        for r in picked:
            self._rows[r.id] = replace(r, status=RequestStatus.FETCHING)
        return [self._rows[r.id] for r in picked]

    def requeue(
        self, id: UUID, *, backoff_seconds: int, error: str | None = None
    ) -> None:
        if id not in self._rows:
            return
        row = self._rows[id]
        self._rows[id] = replace(
            row,
            status=RequestStatus.QUEUED,
            not_before=datetime.now(UTC) + timedelta(seconds=backoff_seconds),
            last_error=error or row.last_error,
            updated_at=datetime.now(UTC),
        )
