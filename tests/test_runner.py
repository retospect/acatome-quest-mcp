"""Tests for the background Runner."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from acatome_quest_mcp.fetchers import FetchResult
from acatome_quest_mcp.models import (
    PaperRef,
    PaperRequest,
    RequestStatus,
    ResolvedRef,
)
from acatome_quest_mcp.runner import Runner, _filename_stem

from .fake_db import FakeDB


class _FakeDedup:
    """Library-mode dedup that's easy to program for tests."""

    def __init__(self, rows: dict[str, dict] | None = None) -> None:
        self._rows = rows or {}
        self.enabled = True

    def lookup_by_doi(self, doi: str):
        from acatome_quest_mcp.dedup import StoreHit

        r = self._rows.get(doi)
        if not r:
            return None
        return StoreHit(
            slug=r["slug"],
            doi=r.get("doi"),
            arxiv=r.get("arxiv_id"),
            title=r.get("title"),
            year=r.get("year"),
        )

    def lookup_by_arxiv(self, arxiv: str):
        return None


class _FakeFetcher:
    """Fetcher stub — returns a pre-programmed FetchResult."""

    def __init__(self, name: str, result: FetchResult) -> None:
        self.name = name
        self._result = result

    async def try_fetch(
        self, client: httpx.AsyncClient, req: PaperRequest
    ) -> FetchResult:
        return self._result


async def _queued_request(
    db: FakeDB,
    *,
    doi: str = "10.1/sample",
    arxiv: str | None = None,
    not_before: datetime | None = None,
) -> PaperRequest:
    now = datetime.now(UTC)
    return await db.insert(
        PaperRequest(
            id=uuid4(),
            created_at=now,
            updated_at=now,
            created_by="test",
            source={},
            input=PaperRef(doi=doi, arxiv=arxiv),
            resolved=ResolvedRef(
                doi=doi,
                arxiv=arxiv,
                title="Sample",
                authors=["Smith, J."],
                year=2024,
                score=0.95,
                source="crossref",
            ),
            candidates=[],
            status=RequestStatus.QUEUED,
            misconceptions=[],
            attempts=[],
            priority=0,
            not_before=not_before or now,
        )
    )


@pytest.fixture
async def client():
    async with httpx.AsyncClient() as c:
        yield c


class TestRunnerFetch:
    async def test_success_writes_to_inbox_and_flips_to_ingesting(
        self, tmp_path: Path, client: httpx.AsyncClient
    ) -> None:
        db = FakeDB()
        req = await _queued_request(db, arxiv="2508.20254")

        pdf = b"%PDF-1.7\nhello"
        fetcher = _FakeFetcher(
            "arxiv",
            FetchResult(
                success=True,
                source="arxiv",
                url="https://arxiv.org/pdf/2508.20254.pdf",
                http_status=200,
                pdf_bytes=pdf,
            ),
        )
        runner = Runner(
            db,  # type: ignore[arg-type]
            inbox=tmp_path,
            fetchers=[fetcher],
            dedup=_FakeDedup(),  # type: ignore[arg-type]
            http_client=client,
        )
        n = await runner.tick()
        await runner.close()

        assert n == 1
        out = await db.get(req.id)
        assert out is not None
        assert out.status == RequestStatus.INGESTING
        assert out.pdf_path is not None
        assert Path(out.pdf_path).read_bytes() == pdf
        assert len(out.attempts) == 1
        assert out.attempts[0].success

    async def test_all_sources_fail_requeues_with_backoff(
        self, tmp_path: Path, client: httpx.AsyncClient
    ) -> None:
        db = FakeDB()
        req = await _queued_request(db)

        failing = _FakeFetcher(
            "arxiv",
            FetchResult(
                success=False,
                source="arxiv",
                url="https://arxiv.org/pdf/x.pdf",
                http_status=500,
                error="upstream 500",
            ),
        )
        runner = Runner(
            db,  # type: ignore[arg-type]
            inbox=tmp_path,
            fetchers=[failing],
            dedup=_FakeDedup(),  # type: ignore[arg-type]
            http_client=client,
        )
        await runner.tick()
        await runner.close()

        out = await db.get(req.id)
        assert out is not None
        assert out.status == RequestStatus.QUEUED
        assert out.not_before > datetime.now(UTC) + timedelta(seconds=10)
        assert out.last_error

    async def test_fetcher_not_applicable_skipped(
        self, tmp_path: Path, client: httpx.AsyncClient
    ) -> None:
        db = FakeDB()
        req = await _queued_request(db)

        na = _FakeFetcher(
            "arxiv",
            FetchResult(success=False, source="arxiv", not_applicable=True),
        )
        pdf = b"%PDF-1.7"
        winner = _FakeFetcher(
            "unpaywall",
            FetchResult(
                success=True,
                source="unpaywall",
                url="https://example.org/p.pdf",
                http_status=200,
                pdf_bytes=pdf,
            ),
        )
        runner = Runner(
            db,  # type: ignore[arg-type]
            inbox=tmp_path,
            fetchers=[na, winner],
            dedup=_FakeDedup(),  # type: ignore[arg-type]
            http_client=client,
        )
        await runner.tick()
        await runner.close()

        out = await db.get(req.id)
        assert out is not None
        assert out.status == RequestStatus.INGESTING
        # Only the applicable fetcher should show up in attempts.
        assert [a.source for a in out.attempts] == ["unpaywall"]


class TestRunnerReconcile:
    async def test_ingesting_request_closed_when_store_has_paper(
        self, tmp_path: Path, client: httpx.AsyncClient
    ) -> None:
        db = FakeDB()
        req = await _queued_request(db)
        await db.update(req.id, status=RequestStatus.INGESTING)

        dedup = _FakeDedup(
            {
                "10.1/sample": {
                    "slug": "smith2024sample",
                    "doi": "10.1/sample",
                    "title": "Sample",
                    "year": 2024,
                }
            }
        )
        runner = Runner(
            db,  # type: ignore[arg-type]
            inbox=tmp_path,
            fetchers=[],
            dedup=dedup,  # type: ignore[arg-type]
            http_client=client,
        )
        await runner._reconcile()
        await runner.close()

        out = await db.get(req.id)
        assert out is not None
        assert out.status == RequestStatus.INGESTED
        assert out.resolved.ref == "smith2024sample"
        assert out.resolved.score == 1.0

    async def test_needs_user_also_closed_on_manual_drop(
        self, tmp_path: Path, client: httpx.AsyncClient
    ) -> None:
        db = FakeDB()
        req = await _queued_request(db)
        await db.update(req.id, status=RequestStatus.NEEDS_USER)

        dedup = _FakeDedup(
            {
                "10.1/sample": {
                    "slug": "smith2024sample",
                    "doi": "10.1/sample",
                    "title": "Sample",
                    "year": 2024,
                }
            }
        )
        runner = Runner(
            db,  # type: ignore[arg-type]
            inbox=tmp_path,
            fetchers=[],
            dedup=dedup,  # type: ignore[arg-type]
            http_client=client,
        )
        await runner._reconcile()
        await runner.close()

        out = await db.get(req.id)
        assert out is not None
        assert out.status == RequestStatus.INGESTED


class TestRunnerTimeout:
    async def test_ingesting_row_escalates_after_timeout(
        self,
        tmp_path: Path,
        client: httpx.AsyncClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import acatome_quest_mcp.runner as runner_mod

        monkeypatch.setattr(runner_mod, "INGEST_TIMEOUT", 0)
        db = FakeDB()
        req = await _queued_request(db)
        await db.update(req.id, status=RequestStatus.INGESTING)

        runner = Runner(
            db,  # type: ignore[arg-type]
            inbox=tmp_path,
            fetchers=[],
            dedup=_FakeDedup(),  # type: ignore[arg-type]
            http_client=client,
        )
        await runner._escalate_timeouts()
        await runner.close()

        out = await db.get(req.id)
        assert out is not None
        assert out.status == RequestStatus.EXTRACT_FAILED
        assert out.last_error


class TestFilenameStem:
    def test_uses_resolved_first_author_and_year(self) -> None:
        now = datetime.now(UTC)
        req = PaperRequest(
            id=uuid4(),
            created_at=now,
            updated_at=now,
            created_by=None,
            source={},
            input=PaperRef(),
            resolved=ResolvedRef(authors=["Wang, X.", "Li, M."], year=2020),
            candidates=[],
            status=RequestStatus.QUEUED,
            misconceptions=[],
            attempts=[],
            priority=0,
            not_before=now,
        )
        assert _filename_stem(req) == "wang_2020"

    def test_falls_back_to_input(self) -> None:
        now = datetime.now(UTC)
        req = PaperRequest(
            id=uuid4(),
            created_at=now,
            updated_at=now,
            created_by=None,
            source={},
            input=PaperRef(authors=["Rao, B."], year=2024),
            resolved=ResolvedRef(),
            candidates=[],
            status=RequestStatus.QUEUED,
            misconceptions=[],
            attempts=[],
            priority=0,
            not_before=now,
        )
        assert _filename_stem(req) == "rao_2024"

    def test_sanitizes_special_chars(self) -> None:
        now = datetime.now(UTC)
        req = PaperRequest(
            id=uuid4(),
            created_at=now,
            updated_at=now,
            created_by=None,
            source={},
            input=PaperRef(),
            resolved=ResolvedRef(authors=["O'Neill, Patrick"], year=2023),
            candidates=[],
            status=RequestStatus.QUEUED,
            misconceptions=[],
            attempts=[],
            priority=0,
            not_before=now,
        )
        stem = _filename_stem(req)
        assert stem == "o_neill_2023"
        assert " " not in stem
        assert "'" not in stem
