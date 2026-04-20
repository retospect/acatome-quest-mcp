"""Tests for QuestService.submit_file."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from acatome_quest_mcp.dedup import StoreDedup
from acatome_quest_mcp.models import RequestStatus
from acatome_quest_mcp.resolver import Resolver
from acatome_quest_mcp.service import MAX_PDF_SIZE, NotFoundError, QuestService

from .fake_db import FakeDB

# Ruff doesn't realise these module-level fixture imports are used.
_ = MAX_PDF_SIZE

# A minimal byte string that starts with the PDF magic bytes.
_PDF_BYTES = b"%PDF-1.4\n%fake pdf content\n%%EOF\n"
_HTML_BYTES = b"<!DOCTYPE html>\n<html><body>oops</body></html>"


# ---------------------------------------------------------------------------
# Resolver / service setup (mirrors the pattern in test_service.py)
# ---------------------------------------------------------------------------


class _FakeStore:
    def __init__(self, rows: dict[str, dict] | None = None) -> None:
        self._rows = rows or {}

    def get(self, identifier: str) -> dict | None:
        return self._rows.get(identifier)


def _resolver(crossref: dict[str, Any] | None = None) -> Resolver:
    def cr_fn(doi: str, mailto: str = "") -> dict[str, Any] | None:
        if crossref is None:
            return None
        if crossref.get("doi"):
            return dict(crossref)
        return {**crossref, "doi": doi}

    return Resolver(
        crossref_fn=cr_fn,
        s2_title_fn=lambda title, api_key="": None,
        s2_id_fn=lambda pid, api_key="": None,
    )


def _crossref(
    title: str = "Sample", authors: list[str] | None = None, year: int = 2024
) -> dict[str, Any]:
    return {
        "title": title,
        "authors": [{"name": a} for a in (authors or ["Smith, J."])],
        "year": year,
        "journal": "Sample Journal",
    }


async def _mk_service(
    *,
    store_rows: dict[str, dict] | None = None,
    crossref: dict[str, Any] | None = None,
) -> tuple[QuestService, FakeDB]:
    db = FakeDB()
    svc = QuestService(
        db,  # type: ignore[arg-type]
        resolver=_resolver(crossref=crossref),
        dedup=StoreDedup(store=_FakeStore(store_rows)),
    )
    return svc, db


# ---------------------------------------------------------------------------
# Validation tests (no DB needed)
# ---------------------------------------------------------------------------


class TestValidation:
    async def test_rejects_missing_source(self, tmp_path: Path) -> None:
        svc, _ = await _mk_service()
        with pytest.raises(ValueError, match="url or content"):
            await svc.submit_file(request_id="ignored", inbox=tmp_path)

    async def test_rejects_missing_target(self, tmp_path: Path) -> None:
        svc, _ = await _mk_service()
        with pytest.raises(ValueError, match="request_id or ref"):
            await svc.submit_file(content=_PDF_BYTES, inbox=tmp_path)

    async def test_rejects_both_sources(self, tmp_path: Path) -> None:
        svc, _ = await _mk_service()
        with pytest.raises(ValueError, match="url or content"):
            await svc.submit_file(
                url="https://ex/x.pdf",
                content=_PDF_BYTES,
                request_id="ignored",
                inbox=tmp_path,
            )

    async def test_rejects_both_targets(self, tmp_path: Path) -> None:
        svc, _ = await _mk_service()
        with pytest.raises(ValueError, match="request_id or ref"):
            await svc.submit_file(
                content=_PDF_BYTES,
                request_id="ignored",
                ref={"doi": "10.1/x"},
                inbox=tmp_path,
            )

    async def test_rejects_non_pdf_bytes(self, tmp_path: Path) -> None:
        svc, _ = await _mk_service(crossref=_crossref())
        with pytest.raises(ValueError, match="%PDF-"):
            await svc.submit_file(
                content=_HTML_BYTES,
                ref={"doi": "10.1/x"},
                inbox=tmp_path,
            )

    async def test_rejects_empty_bytes(self, tmp_path: Path) -> None:
        svc, _ = await _mk_service(crossref=_crossref())
        with pytest.raises(ValueError, match="empty"):
            await svc.submit_file(
                content=b"",
                ref={"doi": "10.1/x"},
                inbox=tmp_path,
            )

    async def test_rejects_oversized_pdf(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import acatome_quest_mcp.service as svc_module

        monkeypatch.setattr(svc_module, "MAX_PDF_SIZE", 16)
        svc, _ = await _mk_service(crossref=_crossref())
        with pytest.raises(ValueError, match="exceeds limit"):
            await svc.submit_file(
                content=_PDF_BYTES + b"x" * 100,
                ref={"doi": "10.1/x"},
                inbox=tmp_path,
            )


# ---------------------------------------------------------------------------
# Attach to existing request
# ---------------------------------------------------------------------------


class TestAttachToExisting:
    async def test_flips_failed_request_to_ingesting(self, tmp_path: Path) -> None:
        svc, db = await _mk_service(crossref=_crossref())
        req = await svc.submit({"doi": "10.1/x"})
        await db.update(req.id, status=RequestStatus.FAILED, last_error="no OA")

        updated = await svc.submit_file(
            content=_PDF_BYTES,
            request_id=req.id,
            filename="paper.pdf",
            inbox=tmp_path,
        )

        assert updated.status == RequestStatus.INGESTING
        assert updated.pdf_hash is not None
        assert updated.pdf_path is not None
        assert updated.last_error is None
        # File was written.
        assert Path(updated.pdf_path).read_bytes() == _PDF_BYTES
        # Fetch attempt recorded with provenance.
        assert any(a.source == "user_upload" for a in updated.attempts)

    async def test_reopens_needs_user(self, tmp_path: Path) -> None:
        svc, db = await _mk_service(crossref=_crossref())
        req = await svc.submit({"doi": "10.1/x"})
        await db.update(req.id, status=RequestStatus.NEEDS_USER)

        updated = await svc.submit_file(
            content=_PDF_BYTES, request_id=req.id, inbox=tmp_path
        )
        assert updated.status == RequestStatus.INGESTING

    async def test_refuses_ingested_request(self, tmp_path: Path) -> None:
        svc, db = await _mk_service(crossref=_crossref())
        req = await svc.submit({"doi": "10.1/x"})
        await db.update(req.id, status=RequestStatus.INGESTED)

        with pytest.raises(ValueError, match="ingested"):
            await svc.submit_file(content=_PDF_BYTES, request_id=req.id, inbox=tmp_path)

    async def test_refuses_found_in_store(self, tmp_path: Path) -> None:
        svc, db = await _mk_service(crossref=_crossref())
        req = await svc.submit({"doi": "10.1/x"})
        await db.update(req.id, status=RequestStatus.FOUND_IN_STORE)

        with pytest.raises(ValueError, match="found_in_store"):
            await svc.submit_file(content=_PDF_BYTES, request_id=req.id, inbox=tmp_path)

    async def test_refuses_cancelled(self, tmp_path: Path) -> None:
        svc, db = await _mk_service(crossref=_crossref())
        req = await svc.submit({"doi": "10.1/x"})
        await db.update(req.id, status=RequestStatus.CANCELLED)

        with pytest.raises(ValueError, match="cancelled"):
            await svc.submit_file(content=_PDF_BYTES, request_id=req.id, inbox=tmp_path)

    async def test_unknown_request_id_errors(self, tmp_path: Path) -> None:
        from uuid import uuid4

        svc, _ = await _mk_service()
        with pytest.raises(NotFoundError):
            await svc.submit_file(
                content=_PDF_BYTES,
                request_id=str(uuid4()),
                inbox=tmp_path,
            )


# ---------------------------------------------------------------------------
# Create-from-ref path
# ---------------------------------------------------------------------------


class TestCreateFromRef:
    async def test_creates_new_request_and_attaches(self, tmp_path: Path) -> None:
        svc, _db = await _mk_service(crossref=_crossref())
        req = await svc.submit_file(
            content=_PDF_BYTES,
            ref={"doi": "10.1/brand_new"},
            created_by="writer",
            inbox=tmp_path,
        )
        assert req.status == RequestStatus.INGESTING
        assert req.pdf_hash is not None
        assert req.created_by == "writer"

    async def test_short_circuits_when_paper_in_store(self, tmp_path: Path) -> None:
        # If the paper is already in the store, we do not drop the PDF at
        # all — the existing slug is authoritative.
        svc, _db = await _mk_service(
            store_rows={
                "10.1/have_it": {
                    "slug": "smith2024have",
                    "doi": "10.1/have_it",
                    "arxiv_id": None,
                    "title": "Have it",
                    "year": 2024,
                }
            },
        )
        req = await svc.submit_file(
            content=_PDF_BYTES,
            ref={"doi": "10.1/have_it"},
            inbox=tmp_path,
        )
        assert req.status == RequestStatus.FOUND_IN_STORE
        assert req.pdf_hash is None
        # Inbox stays empty.
        assert not list(tmp_path.iterdir())


# ---------------------------------------------------------------------------
# URL download
# ---------------------------------------------------------------------------


class TestUrlDownload:
    @respx.mock
    async def test_downloads_pdf_from_url(self, tmp_path: Path) -> None:
        respx.get("https://cdn.example/file.pdf").mock(
            return_value=httpx.Response(200, content=_PDF_BYTES)
        )
        svc, _ = await _mk_service(crossref=_crossref())
        req = await svc.submit_file(
            url="https://cdn.example/file.pdf",
            ref={"doi": "10.1/x"},
            inbox=tmp_path,
        )
        assert req.status == RequestStatus.INGESTING
        # URL recorded on the fetch attempt for provenance.
        upload_attempts = [a for a in req.attempts if a.source == "user_upload"]
        assert len(upload_attempts) == 1
        assert upload_attempts[0].url == "https://cdn.example/file.pdf"

    @respx.mock
    async def test_404_raises(self, tmp_path: Path) -> None:
        respx.get("https://cdn.example/missing.pdf").mock(
            return_value=httpx.Response(404)
        )
        svc, _ = await _mk_service(crossref=_crossref())
        with pytest.raises(httpx.HTTPStatusError):
            await svc.submit_file(
                url="https://cdn.example/missing.pdf",
                ref={"doi": "10.1/x"},
                inbox=tmp_path,
            )

    @respx.mock
    async def test_html_error_page_rejected_by_validator(self, tmp_path: Path) -> None:
        # Many CDNs return 200 + HTML when the signed URL expires.  We must
        # reject this before writing to disk.
        respx.get("https://cdn.example/expired.pdf").mock(
            return_value=httpx.Response(200, content=_HTML_BYTES)
        )
        svc, _ = await _mk_service(crossref=_crossref())
        with pytest.raises(ValueError, match="%PDF-"):
            await svc.submit_file(
                url="https://cdn.example/expired.pdf",
                ref={"doi": "10.1/x"},
                inbox=tmp_path,
            )


# ---------------------------------------------------------------------------
# Filename hygiene
# ---------------------------------------------------------------------------


class TestFilenames:
    async def test_filename_hint_sanitised(self, tmp_path: Path) -> None:
        svc, _db = await _mk_service(crossref=_crossref())
        req = await svc.submit({"doi": "10.1/x"})

        updated = await svc.submit_file(
            content=_PDF_BYTES,
            request_id=req.id,
            filename="../../etc/passwd: pwn.pdf",
            inbox=tmp_path,
        )
        assert updated.pdf_path is not None
        name = Path(updated.pdf_path).name
        assert "/" not in name
        assert ":" not in name
        assert ".." not in name.replace(".pdf", "").replace("_.", "")
        assert name.endswith(".pdf")

    async def test_no_filename_falls_back_to_author_year(self, tmp_path: Path) -> None:
        svc, _db = await _mk_service(
            crossref=_crossref(authors=["Feng, Z."], year=2024),
        )
        req = await svc.submit({"doi": "10.1/abc"})

        updated = await svc.submit_file(
            content=_PDF_BYTES, request_id=req.id, inbox=tmp_path
        )
        assert updated.pdf_path is not None
        name = Path(updated.pdf_path).name
        assert "feng" in name.lower()
        assert "2024" in name
