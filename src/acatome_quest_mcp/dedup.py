"""Dedup — check whether the store already has this paper.

``acatome-store`` is an optional dependency.  If it isn't installed, dedup is
a no-op and every request proceeds to resolution + fetch.

We support two modes:

- **Library mode** (preferred): import ``acatome_store.Store`` and call its
  ``get(doi)`` method directly.  Used by the MCP server running on the same
  host as the store.
- **SQL mode** (fallback): connect to the store's DB directly via asyncpg
  and query the ``refs`` table.  Used when the store library isn't on the
  Python path (e.g. a minimal runner container).

Both modes return the same shape: ``StoreHit(ref, doi, slug)`` or ``None``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .models import ResolvedRef

log = logging.getLogger(__name__)


@dataclass
class StoreHit:
    slug: str
    doi: str | None
    arxiv: str | None
    title: str | None
    year: int | None

    def to_resolved(self) -> ResolvedRef:
        return ResolvedRef(
            doi=self.doi,
            arxiv=self.arxiv,
            title=self.title,
            year=self.year,
            ref=self.slug,
            score=1.0,
            source="store",
        )


class StoreDedup:
    """Library-mode dedup via ``acatome_store.Store.get()``."""

    def __init__(self, store: Any | None = None) -> None:
        if store is None:
            try:
                from acatome_store import Store
            except ImportError:
                log.info("acatome-store not installed; dedup disabled")
                self._store = None
                return
            try:
                store = Store()
            except Exception as exc:  # pragma: no cover - depends on env
                log.warning("acatome-store Store() failed: %s", exc)
                self._store = None
                return
        self._store = store

    @property
    def enabled(self) -> bool:
        return self._store is not None

    def lookup_by_doi(self, doi: str) -> StoreHit | None:
        if not self._store or not doi:
            return None
        try:
            paper = self._store.get(doi)
        except Exception as exc:  # pragma: no cover
            log.warning("acatome-store get(%s) failed: %s", doi, exc)
            return None
        if not paper:
            return None
        return _from_store_row(paper)

    def lookup_by_arxiv(self, arxiv: str) -> StoreHit | None:
        """Store.get() doesn't support arxiv lookup directly; try the slug
        convention (``arxiv:<id>``) and fall back to scanning by DOI.

        In practice arXiv-only papers end up with slug ``<firstauthor><year>``
        in acatome-store, so this is a best-effort lookup.
        """
        if not self._store or not arxiv:
            return None
        try:
            paper = self._store.get(f"arxiv:{arxiv}")
        except Exception:
            paper = None
        if paper:
            return _from_store_row(paper)
        return None


def _from_store_row(paper: dict[str, Any]) -> StoreHit:
    return StoreHit(
        slug=paper.get("slug") or str(paper.get("ref_id") or ""),
        doi=paper.get("doi"),
        arxiv=paper.get("arxiv_id"),
        title=paper.get("title"),
        year=paper.get("year"),
    )


# ---------------------------------------------------------------------------
# SQL-mode fallback
# ---------------------------------------------------------------------------


class SqlDedup:
    """SQL-mode dedup — direct asyncpg query against the store's refs table.

    Only used by the runner in minimal deployments where we don't want to
    pull in all of acatome-store's dependencies (SQLAlchemy, chromadb, etc.).
    """

    def __init__(self, pool: Any, *, table: str = "refs") -> None:
        self._pool = pool
        self._table = table

    @property
    def enabled(self) -> bool:
        return self._pool is not None

    async def lookup_by_doi(self, doi: str) -> StoreHit | None:
        if not self._pool or not doi:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT slug, doi, arxiv_id, title, year "
                f"FROM {self._table} WHERE doi = $1 LIMIT 1",
                doi,
            )
        if not row:
            return None
        return StoreHit(
            slug=row["slug"] or "",
            doi=row["doi"],
            arxiv=row["arxiv_id"],
            title=row["title"],
            year=row["year"],
        )

    async def lookup_by_arxiv(self, arxiv: str) -> StoreHit | None:
        if not self._pool or not arxiv:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT slug, doi, arxiv_id, title, year "
                f"FROM {self._table} WHERE arxiv_id = $1 LIMIT 1",
                arxiv,
            )
        if not row:
            return None
        return StoreHit(
            slug=row["slug"] or "",
            doi=row["doi"],
            arxiv=row["arxiv_id"],
            title=row["title"],
            year=row["year"],
        )
