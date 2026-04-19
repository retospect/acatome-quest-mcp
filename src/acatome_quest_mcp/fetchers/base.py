"""Shared types for fetchers — separated out to break the import cycle
between :mod:`fetchers.__init__` and the individual fetcher modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import httpx

    from ..models import PaperRequest


@dataclass
class FetchResult:
    success: bool
    source: str
    url: str | None = None
    http_status: int | None = None
    pdf_bytes: bytes | None = None
    error: str | None = None
    not_applicable: bool = False  # e.g. no DOI / no arxiv id — try next source


class Fetcher(Protocol):
    """Structural type for a PDF fetcher."""

    name: str

    async def try_fetch(
        self, client: httpx.AsyncClient, req: PaperRequest
    ) -> FetchResult: ...
