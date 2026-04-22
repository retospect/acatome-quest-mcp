"""arXiv PDF fetcher.  Direct GET against https://arxiv.org/pdf/<id>.pdf."""

from __future__ import annotations

import logging

import httpx

from ..models import PaperRequest
from .base import FetchResult

log = logging.getLogger(__name__)

ARXIV_PDF_URL = "https://arxiv.org/pdf/{id}.pdf"


class ArxivFetcher:
    name = "arxiv"

    def try_fetch(self, client: httpx.Client, req: PaperRequest) -> FetchResult:
        arxiv = req.resolved.arxiv or req.input.arxiv
        if not arxiv:
            return FetchResult(success=False, source=self.name, not_applicable=True)

        url = ARXIV_PDF_URL.format(id=arxiv)
        try:
            resp = client.get(url, follow_redirects=True, timeout=60.0)
        except httpx.HTTPError as exc:
            return FetchResult(success=False, source=self.name, url=url, error=str(exc))

        if resp.status_code != 200:
            return FetchResult(
                success=False,
                source=self.name,
                url=url,
                http_status=resp.status_code,
                error=f"HTTP {resp.status_code}",
            )

        ct = resp.headers.get("content-type", "")
        if "pdf" not in ct.lower():
            return FetchResult(
                success=False,
                source=self.name,
                url=url,
                http_status=resp.status_code,
                error=f"unexpected content-type {ct!r}",
            )

        return FetchResult(
            success=True,
            source=self.name,
            url=url,
            http_status=resp.status_code,
            pdf_bytes=resp.content,
        )
