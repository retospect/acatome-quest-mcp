"""Tests for the PDF fetchers — arxiv + unpaywall, with HTTP mocked."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import respx

from acatome_quest_mcp.fetchers import ArxivFetcher, UnpaywallFetcher
from acatome_quest_mcp.models import PaperRef, PaperRequest, RequestStatus, ResolvedRef


def _make_request(*, doi: str | None = None, arxiv: str | None = None) -> PaperRequest:
    now = datetime.now(UTC)
    return PaperRequest(
        id=uuid4(),
        created_at=now,
        updated_at=now,
        created_by=None,
        source={},
        input=PaperRef(doi=doi, arxiv=arxiv),
        resolved=ResolvedRef(doi=doi, arxiv=arxiv),
        candidates=[],
        status=RequestStatus.QUEUED,
        misconceptions=[],
        attempts=[],
        priority=0,
        not_before=now,
    )


@pytest.fixture
def client():
    with httpx.Client(timeout=5.0) as c:
        yield c


class TestArxivFetcher:
    def test_not_applicable_without_arxiv_id(self, client: httpx.Client) -> None:
        fetcher = ArxivFetcher()
        res = fetcher.try_fetch(client, _make_request())
        assert res.not_applicable is True
        assert res.success is False

    @respx.mock
    def test_success(self, client: httpx.Client) -> None:
        pdf_body = b"%PDF-1.7\n..."
        route = respx.get("https://arxiv.org/pdf/2508.20254.pdf").mock(
            return_value=httpx.Response(
                200, content=pdf_body, headers={"content-type": "application/pdf"}
            )
        )
        fetcher = ArxivFetcher()
        res = fetcher.try_fetch(client, _make_request(arxiv="2508.20254"))
        assert route.called
        assert res.success is True
        assert res.pdf_bytes == pdf_body
        assert res.source == "arxiv"

    @respx.mock
    def test_404_returns_failure(self, client: httpx.Client) -> None:
        respx.get("https://arxiv.org/pdf/9999.99999.pdf").mock(
            return_value=httpx.Response(404)
        )
        fetcher = ArxivFetcher()
        res = fetcher.try_fetch(client, _make_request(arxiv="9999.99999"))
        assert res.success is False
        assert res.http_status == 404

    @respx.mock
    def test_wrong_content_type(self, client: httpx.Client) -> None:
        respx.get("https://arxiv.org/pdf/2508.20254.pdf").mock(
            return_value=httpx.Response(
                200,
                text="<html>not a pdf</html>",
                headers={"content-type": "text/html"},
            )
        )
        fetcher = ArxivFetcher()
        res = fetcher.try_fetch(client, _make_request(arxiv="2508.20254"))
        assert res.success is False
        assert res.http_status == 200


class TestUnpaywallFetcher:
    def test_not_applicable_without_email(self, client: httpx.Client) -> None:
        fetcher = UnpaywallFetcher(email="")
        res = fetcher.try_fetch(client, _make_request(doi="10.1/x"))
        assert res.not_applicable is True

    def test_not_applicable_without_doi(self, client: httpx.Client) -> None:
        fetcher = UnpaywallFetcher(email="me@example.com")
        res = fetcher.try_fetch(client, _make_request())
        assert res.not_applicable is True

    @respx.mock
    def test_success(self, client: httpx.Client) -> None:
        doi = "10.1021/jacs.2c01234"
        meta = {
            "best_oa_location": {"url_for_pdf": "https://example.org/paper.pdf"},
            "oa_locations": [],
        }
        respx.get(f"https://api.unpaywall.org/v2/{doi}").mock(
            return_value=httpx.Response(200, json=meta)
        )
        respx.get("https://example.org/paper.pdf").mock(
            return_value=httpx.Response(
                200, content=b"%PDF-1.7", headers={"content-type": "application/pdf"}
            )
        )
        fetcher = UnpaywallFetcher(email="me@example.com")
        res = fetcher.try_fetch(client, _make_request(doi=doi))
        assert res.success is True
        assert res.source == "unpaywall"
        assert res.pdf_bytes == b"%PDF-1.7"

    @respx.mock
    def test_no_oa_pdf(self, client: httpx.Client) -> None:
        doi = "10.1/behind-paywall"
        respx.get(f"https://api.unpaywall.org/v2/{doi}").mock(
            return_value=httpx.Response(
                200, json={"best_oa_location": None, "oa_locations": []}
            )
        )
        fetcher = UnpaywallFetcher(email="me@example.com")
        res = fetcher.try_fetch(client, _make_request(doi=doi))
        assert res.success is False
        assert "no OA" in (res.error or "")

    @respx.mock
    def test_fallback_to_oa_locations(self, client: httpx.Client) -> None:
        doi = "10.1/multiple"
        respx.get(f"https://api.unpaywall.org/v2/{doi}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "best_oa_location": {"url_for_pdf": None},
                    "oa_locations": [
                        {"url_for_pdf": None},
                        {"url_for_pdf": "https://example.org/p.pdf"},
                    ],
                },
            )
        )
        respx.get("https://example.org/p.pdf").mock(
            return_value=httpx.Response(
                200, content=b"%PDF-1", headers={"content-type": "application/pdf"}
            )
        )
        fetcher = UnpaywallFetcher(email="me@example.com")
        res = fetcher.try_fetch(client, _make_request(doi=doi))
        assert res.success is True
