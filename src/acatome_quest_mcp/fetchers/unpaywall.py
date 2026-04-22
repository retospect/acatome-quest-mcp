"""Unpaywall OA fetcher.  Uses the free polite-pool API; email required."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from ..models import PaperRequest
from .base import FetchResult

log = logging.getLogger(__name__)

UNPAYWALL_URL = "https://api.unpaywall.org/v2/{doi}"


class UnpaywallFetcher:
    name = "unpaywall"

    def __init__(self, email: str | None = None) -> None:
        # Distinguish None (auto — fall back to env) from "" (explicit empty).
        self._email = (
            email if email is not None else os.environ.get("UNPAYWALL_EMAIL", "")
        )

    def try_fetch(self, client: httpx.Client, req: PaperRequest) -> FetchResult:
        doi = req.resolved.doi or req.input.doi
        if not doi:
            return FetchResult(success=False, source=self.name, not_applicable=True)
        if not self._email:
            return FetchResult(
                success=False,
                source=self.name,
                error="UNPAYWALL_EMAIL not set — skipping Unpaywall",
                not_applicable=True,
            )

        meta_url = UNPAYWALL_URL.format(doi=doi)
        try:
            resp = client.get(meta_url, params={"email": self._email}, timeout=30.0)
        except httpx.HTTPError as exc:
            return FetchResult(
                success=False, source=self.name, url=meta_url, error=str(exc)
            )

        if resp.status_code != 200:
            return FetchResult(
                success=False,
                source=self.name,
                url=meta_url,
                http_status=resp.status_code,
                error=f"metadata lookup HTTP {resp.status_code}",
            )

        data = resp.json()
        pdf_url = _best_oa_pdf(data)
        if not pdf_url:
            return FetchResult(
                success=False,
                source=self.name,
                url=meta_url,
                http_status=resp.status_code,
                error="no OA PDF available",
            )

        try:
            pdf_resp = client.get(pdf_url, follow_redirects=True, timeout=60.0)
        except httpx.HTTPError as exc:
            return FetchResult(
                success=False, source=self.name, url=pdf_url, error=str(exc)
            )

        ct = pdf_resp.headers.get("content-type", "")
        if pdf_resp.status_code != 200 or "pdf" not in ct.lower():
            return FetchResult(
                success=False,
                source=self.name,
                url=pdf_url,
                http_status=pdf_resp.status_code,
                error=f"download HTTP {pdf_resp.status_code} ct={ct!r}",
            )

        return FetchResult(
            success=True,
            source=self.name,
            url=pdf_url,
            http_status=pdf_resp.status_code,
            pdf_bytes=pdf_resp.content,
        )


def _best_oa_pdf(data: dict[str, Any]) -> str | None:
    """Pick the best OA PDF URL from an Unpaywall response."""
    best = data.get("best_oa_location")
    if isinstance(best, dict) and best.get("url_for_pdf"):
        return best["url_for_pdf"]
    for loc in data.get("oa_locations") or []:
        if isinstance(loc, dict) and loc.get("url_for_pdf"):
            return loc["url_for_pdf"]
    return None
