"""PDF fetchers — open-access sources only.

Each fetcher exposes::

    class SomeFetcher:
        name: str
        async def try_fetch(client, req) -> FetchResult

and is registered in :data:`DEFAULT_FETCHERS` in source-preference order.

No Sci-Hub, no LibGen, no paywall circumvention — these are design
requirements, not configuration switches.
"""

from __future__ import annotations

from .arxiv import ArxivFetcher
from .base import Fetcher, FetchResult
from .unpaywall import UnpaywallFetcher

DEFAULT_FETCHERS: list[Fetcher] = [
    ArxivFetcher(),
    UnpaywallFetcher(),
]


__all__ = [
    "DEFAULT_FETCHERS",
    "ArxivFetcher",
    "FetchResult",
    "Fetcher",
    "UnpaywallFetcher",
]
