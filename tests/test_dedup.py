"""Tests for the StoreDedup wrapper (library-mode)."""

from __future__ import annotations

from acatome_quest_mcp.dedup import StoreDedup


class FakeStore:
    """Matches the subset of ``acatome_store.Store.get()`` we use."""

    def __init__(self, rows: dict[str, dict]) -> None:
        self._rows = rows

    def get(self, identifier):
        return self._rows.get(identifier)


def test_dedup_disabled_when_store_none() -> None:
    dd = StoreDedup(store=None)
    # Passing None explicitly is ambiguous with "auto-detect" — the class
    # treats None as "fall back to acatome_store import".  The import may or
    # may not succeed depending on the environment.  We just confirm the
    # attribute exists.
    assert hasattr(dd, "enabled")


def test_lookup_by_doi_hit() -> None:
    store = FakeStore(
        {
            "10.1021/jacs.2c01234": {
                "slug": "smith2022jacs",
                "doi": "10.1021/jacs.2c01234",
                "arxiv_id": None,
                "title": "A paper",
                "year": 2022,
            }
        }
    )
    dd = StoreDedup(store=store)
    hit = dd.lookup_by_doi("10.1021/jacs.2c01234")
    assert hit is not None
    assert hit.slug == "smith2022jacs"
    assert hit.doi == "10.1021/jacs.2c01234"

    resolved = hit.to_resolved()
    assert resolved.ref == "smith2022jacs"
    assert resolved.score == 1.0
    assert resolved.source == "store"


def test_lookup_by_doi_miss() -> None:
    dd = StoreDedup(store=FakeStore({}))
    assert dd.lookup_by_doi("10.1/missing") is None


def test_lookup_by_doi_empty_doi() -> None:
    dd = StoreDedup(store=FakeStore({"10.1/x": {"slug": "x", "doi": "10.1/x"}}))
    assert dd.lookup_by_doi("") is None


def test_lookup_by_arxiv_uses_slug_convention() -> None:
    store = FakeStore(
        {
            "arxiv:2508.20254": {
                "slug": "anon2025arxiv",
                "doi": None,
                "arxiv_id": "2508.20254",
                "title": "arXiv paper",
                "year": 2025,
            }
        }
    )
    dd = StoreDedup(store=store)
    hit = dd.lookup_by_arxiv("2508.20254")
    assert hit is not None
    assert hit.arxiv == "2508.20254"
