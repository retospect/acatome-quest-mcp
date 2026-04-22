"""Database layer for acatome-quest-mcp.

Sync rewrite (April 2026) — replaces the former ``asyncpg`` layer with
``psycopg3`` + ``psycopg_pool``.  The handler stack above (service,
resolver, CLI, precis ``QuestHandler``) is all sync, so the whole async
bridge was cargo-culted plumbing around sync libraries.  See
``CHANGELOG.md`` for the motivation.

All state lives in a single table ``<schema>.requests``.  No embeddings,
no secondary tables — a request is cheap to write and cheap to read.

The schema name is configurable (default ``papers``) so the same codebase
can run against the production ``cluster`` DB or a throwaway
``quest_test`` schema in CI.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .misconceptions import Misconception
from .models import (
    OPEN_STATUSES,
    Candidate,
    FetchAttempt,
    PaperRef,
    PaperRequest,
    RequestStatus,
    ResolvedRef,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.requests (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_by      text,
    source          jsonb NOT NULL DEFAULT '{{}}',

    -- input
    input_doi       text,
    input_arxiv     text,
    input_pmid      text,
    input_title     text,
    input_authors   text[],
    input_year      int,
    input_raw       text,

    -- resolved
    resolved_doi    text,
    resolved_arxiv  text,
    resolved_pmid   text,
    resolved_title  text,
    resolved_authors text[],
    resolved_year   int,
    resolved_journal text,
    resolved_ref    text,
    resolved_score  real NOT NULL DEFAULT 0,
    resolved_source text,
    candidates      jsonb NOT NULL DEFAULT '[]',

    -- lifecycle
    status          text NOT NULL DEFAULT 'queued',
    misconceptions  jsonb NOT NULL DEFAULT '[]',
    attempts        jsonb NOT NULL DEFAULT '[]',
    priority        int NOT NULL DEFAULT 0,
    not_before      timestamptz NOT NULL DEFAULT now(),
    supersedes      uuid REFERENCES {schema}.requests(id),

    -- ops
    pdf_hash        text,
    pdf_path        text,
    last_error      text
);

CREATE INDEX IF NOT EXISTS idx_requests_status_open
    ON {schema}.requests (status)
    WHERE status IN ('queued', 'resolving', 'fetching', 'ingesting', 'needs_user');

CREATE INDEX IF NOT EXISTS idx_requests_created_by
    ON {schema}.requests (created_by);

CREATE INDEX IF NOT EXISTS idx_requests_input_doi
    ON {schema}.requests (input_doi);

CREATE INDEX IF NOT EXISTS idx_requests_resolved_doi
    ON {schema}.requests (resolved_doi);

CREATE INDEX IF NOT EXISTS idx_requests_resolved_ref
    ON {schema}.requests (resolved_ref);

CREATE INDEX IF NOT EXISTS idx_requests_due
    ON {schema}.requests (not_before)
    WHERE status = 'queued';
"""


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------


class DB:
    """Sync database interface for acatome-quest-mcp.

    Usage::

        db = DB("postgresql://...", schema="papers")
        db.connect()
        db.migrate()
        ...
        db.close()
    """

    def __init__(self, dsn: str, *, schema: str = "papers") -> None:
        self.dsn = dsn
        self.schema = schema
        self._pool: ConnectionPool | None = None

    def connect(self) -> None:
        # ``open=True`` so the pool eagerly opens connections rather than
        # lazy-opening at first use — matches the previous asyncpg behaviour.
        self._pool = ConnectionPool(
            self.dsn, min_size=1, max_size=10, open=True, kwargs={"autocommit": True}
        )
        log.info("acatome-quest DB connected (schema=%s)", self.schema)

    def close(self) -> None:
        if self._pool:
            self._pool.close()
            self._pool = None

    @property
    def pool(self) -> ConnectionPool:
        if self._pool is None:
            raise RuntimeError("DB not connected — call db.connect() first")
        return self._pool

    def migrate(self) -> None:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(SCHEMA_SQL.format(schema=self.schema))

    # -----------------------------------------------------------------
    # Writes
    # -----------------------------------------------------------------

    def insert(self, req: PaperRequest) -> PaperRequest:
        """Insert a new request.  ``req.id`` may be set or left for the DB
        to default; this method returns the row as stored."""
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.requests (
                    created_by, source,
                    input_doi, input_arxiv, input_pmid, input_title,
                    input_authors, input_year, input_raw,
                    resolved_doi, resolved_arxiv, resolved_pmid,
                    resolved_title, resolved_authors, resolved_year,
                    resolved_journal, resolved_ref, resolved_score, resolved_source,
                    candidates, status, misconceptions, attempts,
                    priority, not_before, supersedes,
                    pdf_hash, pdf_path, last_error
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                RETURNING *
                """,
                (
                    req.created_by,
                    Jsonb(req.source),
                    req.input.doi,
                    req.input.arxiv,
                    req.input.pmid,
                    req.input.title,
                    req.input.authors or None,
                    req.input.year,
                    req.input.raw,
                    req.resolved.doi,
                    req.resolved.arxiv,
                    req.resolved.pmid,
                    req.resolved.title,
                    req.resolved.authors or None,
                    req.resolved.year,
                    req.resolved.journal,
                    req.resolved.ref,
                    req.resolved.score,
                    req.resolved.source,
                    Jsonb([c.to_dict() for c in req.candidates]),
                    req.status.value,
                    Jsonb([m.to_dict() for m in req.misconceptions]),
                    Jsonb([a.to_dict() for a in req.attempts]),
                    req.priority,
                    req.not_before,
                    req.supersedes,
                    req.pdf_hash,
                    req.pdf_path,
                    req.last_error,
                ),
            )
            row = cur.fetchone()
        out = _row_to_request(row)
        assert out is not None  # RETURNING * cannot produce NULL
        return out

    def update(self, id: UUID, **fields: Any) -> PaperRequest | None:
        """Partial update by column name.

        Accepts either scalar column values or structured fields:
        ``status``, ``priority``, ``not_before``, ``resolved`` (ResolvedRef),
        ``misconceptions`` (list[Misconception]), ``attempts``
        (list[FetchAttempt]), ``candidates`` (list[Candidate]),
        ``pdf_hash``, ``pdf_path``, ``last_error``, ``source`` (dict).
        """
        if not fields:
            return self.get(id)

        sets: list[str] = []
        args: list[Any] = []

        def add(col: str, val: Any) -> None:
            sets.append(f"{col} = %s")
            args.append(val)

        if "status" in fields:
            st = fields["status"]
            add("status", st.value if isinstance(st, RequestStatus) else st)
        if "priority" in fields:
            add("priority", int(fields["priority"]))
        if "not_before" in fields:
            add("not_before", fields["not_before"])
        if "source" in fields:
            add("source", Jsonb(fields["source"]))
        if "resolved" in fields:
            r: ResolvedRef = fields["resolved"]
            for col, val in {
                "resolved_doi": r.doi,
                "resolved_arxiv": r.arxiv,
                "resolved_pmid": r.pmid,
                "resolved_title": r.title,
                "resolved_authors": r.authors or None,
                "resolved_year": r.year,
                "resolved_journal": r.journal,
                "resolved_ref": r.ref,
                "resolved_score": r.score,
                "resolved_source": r.source,
            }.items():
                add(col, val)
        if "misconceptions" in fields:
            ms: list[Misconception] = fields["misconceptions"]
            add("misconceptions", Jsonb([m.to_dict() for m in ms]))
        if "attempts" in fields:
            ats: list[FetchAttempt] = fields["attempts"]
            add("attempts", Jsonb([a.to_dict() for a in ats]))
        if "candidates" in fields:
            cs: list[Candidate] = fields["candidates"]
            add("candidates", Jsonb([c.to_dict() for c in cs]))
        if "pdf_hash" in fields:
            add("pdf_hash", fields["pdf_hash"])
        if "pdf_path" in fields:
            add("pdf_path", fields["pdf_path"])
        if "last_error" in fields:
            add("last_error", fields["last_error"])
        if "resolved_ref" in fields:
            add("resolved_ref", fields["resolved_ref"])

        if not sets:
            return self.get(id)

        sets.append("updated_at = %s")
        args.append(datetime.now(UTC))

        args.append(id)
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"UPDATE {self.schema}.requests SET {', '.join(sets)} "
                f"WHERE id = %s RETURNING *",
                tuple(args),
            )
            row = cur.fetchone()
        return _row_to_request(row) if row else None

    # -----------------------------------------------------------------
    # Reads
    # -----------------------------------------------------------------

    def get(self, id: UUID) -> PaperRequest | None:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT * FROM {self.schema}.requests WHERE id = %s", (id,))
            row = cur.fetchone()
        return _row_to_request(row) if row else None

    def find_open_by_doi(self, doi: str) -> PaperRequest | None:
        """Return the most-recent open (non-terminal) request for this DOI,
        matching on either input_doi or resolved_doi."""
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self.schema}.requests
                WHERE (input_doi = %s OR resolved_doi = %s)
                  AND status = ANY(%s::text[])
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (doi, doi, [s.value for s in OPEN_STATUSES]),
            )
            row = cur.fetchone()
        return _row_to_request(row) if row else None

    def find_open_by_arxiv(self, arxiv: str) -> PaperRequest | None:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self.schema}.requests
                WHERE (input_arxiv = %s OR resolved_arxiv = %s)
                  AND status = ANY(%s::text[])
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (arxiv, arxiv, [s.value for s in OPEN_STATUSES]),
            )
            row = cur.fetchone()
        return _row_to_request(row) if row else None

    def count_open_for(self, created_by: str) -> int:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*) FROM {self.schema}.requests
                WHERE created_by = %s
                  AND status = ANY(%s::text[])
                """,
                (created_by, [s.value for s in OPEN_STATUSES]),
            )
            row = cur.fetchone()
        return int(row[0] if row else 0)

    def find(
        self,
        *,
        status: RequestStatus | str | None = None,
        created_by: str | None = None,
        has_misconception: bool | None = None,
        source_document: str | None = None,
        limit: int = 100,
    ) -> list[PaperRequest]:
        where: list[str] = []
        args: list[Any] = []

        if status is not None:
            where.append("status = %s")
            args.append(status.value if isinstance(status, RequestStatus) else status)
        if created_by is not None:
            where.append("created_by = %s")
            args.append(created_by)
        if has_misconception is True:
            where.append("jsonb_array_length(misconceptions) > 0")
        elif has_misconception is False:
            where.append("jsonb_array_length(misconceptions) = 0")
        if source_document is not None:
            where.append("source->>'document' = %s")
            args.append(source_document)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        args.append(limit)
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self.schema}.requests
                {where_sql}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(args),
            )
            rows = cur.fetchall()
        return _non_null([_row_to_request(r) for r in rows])

    # -----------------------------------------------------------------
    # Runner claim
    # -----------------------------------------------------------------

    def claim_queued(self, limit: int = 1) -> list[PaperRequest]:
        """Atomically claim up to ``limit`` queued requests whose
        ``not_before`` has passed.  Sets status to ``fetching``.

        Uses ``FOR UPDATE SKIP LOCKED`` so multiple runners may coexist.
        """
        # Autocommit is on at pool level; wrap in an explicit transaction
        # so the SELECT…FOR UPDATE + UPDATE are atomic.
        with self.pool.connection() as conn:
            conn.autocommit = False
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        WITH picked AS (
                            SELECT id FROM {self.schema}.requests
                            WHERE status = 'queued' AND not_before <= now()
                            ORDER BY priority DESC, created_at ASC
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE {self.schema}.requests r
                        SET status = 'fetching', updated_at = now()
                        FROM picked
                        WHERE r.id = picked.id
                        RETURNING r.*
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.autocommit = True
        return _non_null([_row_to_request(r) for r in rows])

    def requeue(
        self, id: UUID, *, backoff_seconds: int, error: str | None = None
    ) -> None:
        """Return a request to ``queued`` with exponential backoff."""
        not_before = datetime.now(UTC) + timedelta(seconds=backoff_seconds)
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.schema}.requests
                SET status = 'queued',
                    not_before = %s,
                    last_error = COALESCE(%s, last_error),
                    updated_at = now()
                WHERE id = %s
                """,
                (not_before, error, id),
            )


# ---------------------------------------------------------------------------
# Row → dataclass mapping
# ---------------------------------------------------------------------------


def _jsonb(val: Any) -> Any:
    """psycopg returns jsonb as a Python object (list/dict) by default.

    Kept as a defensive coercion: mocks or older rows may still carry a
    JSON string; handle that uniformly.
    """
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        return json.loads(val)
    return val


def _non_null(xs: list[PaperRequest | None]) -> list[PaperRequest]:
    return [x for x in xs if x is not None]


def _row_to_request(row: dict[str, Any] | None) -> PaperRequest | None:
    """Convert a ``dict_row``-typed psycopg row to a ``PaperRequest``."""
    if row is None:
        return None
    misconceptions_raw = _jsonb(row["misconceptions"]) or []
    attempts_raw = _jsonb(row["attempts"]) or []
    candidates_raw = _jsonb(row["candidates"]) or []
    source = _jsonb(row["source"]) or {}

    return PaperRequest(
        id=row["id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        created_by=row["created_by"],
        source=source,
        input=PaperRef(
            doi=row["input_doi"],
            arxiv=row["input_arxiv"],
            pmid=row["input_pmid"],
            title=row["input_title"],
            authors=list(row["input_authors"] or []),
            year=row["input_year"],
            raw=row["input_raw"],
        ),
        resolved=ResolvedRef(
            doi=row["resolved_doi"],
            arxiv=row["resolved_arxiv"],
            pmid=row["resolved_pmid"],
            title=row["resolved_title"],
            authors=list(row["resolved_authors"] or []),
            year=row["resolved_year"],
            journal=row["resolved_journal"],
            ref=row["resolved_ref"],
            score=row["resolved_score"] or 0.0,
            source=row["resolved_source"] or "",
        ),
        candidates=[
            Candidate(ref=ResolvedRef(**c["ref"]), reason=c.get("reason", ""))
            for c in candidates_raw
        ],
        status=RequestStatus(row["status"]),
        misconceptions=[Misconception.from_dict(m) for m in misconceptions_raw],
        attempts=[_attempt_from_dict(a) for a in attempts_raw],
        priority=row["priority"],
        not_before=row["not_before"],
        supersedes=row["supersedes"],
        pdf_hash=row["pdf_hash"],
        pdf_path=row["pdf_path"],
        last_error=row["last_error"],
    )


def _attempt_from_dict(d: dict[str, Any]) -> FetchAttempt:
    at = d.get("at")
    if isinstance(at, str):
        at = datetime.fromisoformat(at)
    return FetchAttempt(
        source=d["source"],
        url=d.get("url"),
        http_status=d.get("http_status"),
        at=at,
        error=d.get("error"),
        success=d.get("success", False),
    )
