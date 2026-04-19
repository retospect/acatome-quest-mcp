"""Background runner — claims queued requests, fetches PDFs, polls for ingest.

Runs as a long-lived daemon::

    acatome-quest-runner

or as a single tick for testing::

    acatome-quest-runner --once

The runner never blocks the MCP server.  Agents call ``submit()`` which returns
in milliseconds; the runner polls the DB, downloads the PDF, writes it to the
inbox, and waits for ``acatome-extract watch`` to do the rest.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import re
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx

from .db import DB
from .dedup import StoreDedup
from .fetchers import DEFAULT_FETCHERS, Fetcher, FetchResult
from .models import (
    FetchAttempt,
    PaperRequest,
    RequestStatus,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_INBOX = Path(os.environ.get("ACATOME_INBOX", "~/.acatome/inbox")).expanduser()
POLL_INTERVAL = int(os.environ.get("QUEST_POLL_INTERVAL", "30"))
MAX_CONCURRENT = int(os.environ.get("QUEST_MAX_CONCURRENT", "4"))
INGEST_TIMEOUT = int(os.environ.get("QUEST_INGEST_TIMEOUT", "900"))
MAX_ATTEMPTS = int(os.environ.get("QUEST_MAX_ATTEMPTS", "5"))
INITIAL_BACKOFF = int(os.environ.get("QUEST_INITIAL_BACKOFF", "60"))  # seconds

_SLUG_SAFE = re.compile(r"[^a-z0-9]+")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class Runner:
    """Claim queued requests, fetch PDFs from OA sources, poll for ingest."""

    def __init__(
        self,
        db: DB,
        *,
        inbox: Path | None = None,
        fetchers: list[Fetcher] | None = None,
        dedup: StoreDedup | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.db = db
        self.inbox = inbox or DEFAULT_INBOX
        self.fetchers: list[Fetcher] = (
            fetchers if fetchers is not None else DEFAULT_FETCHERS
        )
        self.dedup = dedup if dedup is not None else StoreDedup()
        self._owns_client = http_client is None
        self.http = http_client or httpx.AsyncClient(
            timeout=60.0,
            headers={"User-Agent": "acatome-quest-mcp/0.1 (+oa-fetch)"},
        )

    async def close(self) -> None:
        if self._owns_client:
            await self.http.aclose()

    async def tick(self, *, limit: int = MAX_CONCURRENT) -> int:
        """Run one pass.  Returns number of requests processed."""
        # 1. Reconcile: close needs_user rows whose DOI now appears in store.
        await self._reconcile()
        # 2. Escalate: flip ingesting rows that timed out → extract_failed.
        await self._escalate_timeouts()
        # 3. Claim queued rows.
        reqs = await self.db.claim_queued(limit=limit)
        if not reqs:
            return 0
        log.info("Runner claimed %d request(s)", len(reqs))
        for req in reqs:
            try:
                await self._process(req)
            except Exception as exc:
                log.exception("Runner failed on %s: %s", req.id, exc)
                await self.db.update(
                    req.id,
                    status=RequestStatus.QUEUED,
                    last_error=str(exc),
                )
        return len(reqs)

    async def run_forever(self) -> None:
        self.inbox.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                await self.tick()
            except Exception as exc:
                log.exception("Runner tick failed: %s", exc)
            await asyncio.sleep(POLL_INTERVAL)

    # -----------------------------------------------------------------
    # Per-request processing
    # -----------------------------------------------------------------

    async def _process(self, req: PaperRequest) -> None:
        # Walk fetchers until one succeeds.
        attempts = list(req.attempts)
        for fetcher in self.fetchers:
            result: FetchResult = await fetcher.try_fetch(self.http, req)
            if result.not_applicable:
                continue
            attempts.append(
                FetchAttempt(
                    source=result.source,
                    url=result.url,
                    http_status=result.http_status,
                    at=datetime.now(UTC),
                    error=result.error,
                    success=result.success,
                )
            )
            if result.success and result.pdf_bytes:
                await self._deliver(req, result.pdf_bytes, attempts)
                return

        # All fetchers either didn't apply or failed.
        n_real_attempts = sum(1 for a in attempts if a.url)
        if n_real_attempts >= MAX_ATTEMPTS:
            await self.db.update(
                req.id,
                status=RequestStatus.NEEDS_USER,
                attempts=attempts,
                last_error="all OA sources exhausted",
            )
            return

        # Backoff and requeue.
        backoff = min(INITIAL_BACKOFF * (2**n_real_attempts), 3600)
        await self.db.update(req.id, attempts=attempts)
        await self.db.requeue(req.id, backoff_seconds=backoff, error="no OA source")

    async def _deliver(
        self,
        req: PaperRequest,
        pdf_bytes: bytes,
        attempts: list[FetchAttempt],
    ) -> None:
        """Write PDF to inbox, flip to ingesting, wait for extract."""
        self.inbox.mkdir(parents=True, exist_ok=True)
        sha = hashlib.sha256(pdf_bytes).hexdigest()
        stem = _filename_stem(req)
        path = self.inbox / f"{stem}__{sha[:12]}.pdf"
        path.write_bytes(pdf_bytes)
        log.info("Wrote %s (%d bytes) for request %s", path, len(pdf_bytes), req.id)

        await self.db.update(
            req.id,
            status=RequestStatus.INGESTING,
            pdf_hash=sha,
            pdf_path=str(path),
            attempts=attempts,
        )

    # -----------------------------------------------------------------
    # Reconcile: acatome-extract watch finishes out of band.
    # -----------------------------------------------------------------

    async def _reconcile(self) -> None:
        """For any INGESTING request, check if the paper now exists in the
        store by DOI; if so, flip to INGESTED.

        Also picks up NEEDS_USER requests whose paper appeared in the store
        (user dropped it manually).
        """
        if not self.dedup.enabled:
            return
        for status in (RequestStatus.INGESTING, RequestStatus.NEEDS_USER):
            rows = await self.db.find(status=status, limit=200)
            for req in rows:
                doi = req.resolved.doi or req.input.doi
                if not doi:
                    continue
                hit = self.dedup.lookup_by_doi(doi)
                if not hit:
                    continue
                resolved = replace(
                    req.resolved, ref=hit.slug, source="store", score=1.0
                )
                await self.db.update(
                    req.id,
                    status=RequestStatus.INGESTED,
                    resolved=resolved,
                )
                log.info(
                    "Reconciled %s: %s → slug %s",
                    req.id,
                    doi,
                    hit.slug,
                )

    async def _escalate_timeouts(self) -> None:
        """INGESTING requests older than INGEST_TIMEOUT seconds and still not
        in the store → EXTRACT_FAILED.  Operational failure, not a paper-
        identity misconception, so we record only ``last_error``."""
        cutoff = datetime.now(UTC).timestamp() - INGEST_TIMEOUT
        rows = await self.db.find(status=RequestStatus.INGESTING, limit=200)
        for req in rows:
            if req.updated_at.timestamp() > cutoff:
                continue
            await self.db.update(
                req.id,
                status=RequestStatus.EXTRACT_FAILED,
                last_error=(
                    f"PDF delivered {INGEST_TIMEOUT}s ago but no ref appeared "
                    f"in acatome-store — extraction likely failed"
                ),
            )
            log.warning("Ingest timeout on %s — marked extract_failed", req.id)


def _filename_stem(req: PaperRequest) -> str:
    """Human-friendly filename stem (safe-slugged)."""
    parts: list[str] = []
    if req.resolved.authors:
        first = req.resolved.authors[0].split(",")[0]
        parts.append(first.strip())
    elif req.input.authors:
        parts.append(req.input.authors[0].split(",")[0].strip())
    if req.resolved.year:
        parts.append(str(req.resolved.year))
    elif req.input.year:
        parts.append(str(req.input.year))
    if not parts:
        parts.append("paper")
    raw = "_".join(parts).lower()
    return _SLUG_SAFE.sub("_", raw).strip("_") or "paper"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


async def _amain(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="acatome-quest-mcp runner")
    parser.add_argument(
        "--once", action="store_true", help="Run one tick and exit (for cron/testing)"
    )
    parser.add_argument(
        "--log-level", default=os.environ.get("QUEST_LOG_LEVEL", "INFO")
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL", "postgresql://localhost/cluster")
    schema = os.environ.get("QUEST_SCHEMA", "papers")
    db = DB(dsn, schema=schema)
    await db.connect()
    await db.migrate()

    runner = Runner(db)
    try:
        if args.once:
            n = await runner.tick()
            log.info("Single tick processed %d request(s)", n)
        else:
            await runner.run_forever()
    finally:
        await runner.close()
        await db.close()
    return 0


def main() -> None:
    sys.exit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
