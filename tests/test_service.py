"""End-to-end tests for QuestService using FakeDB + injected resolver + fake store."""

from __future__ import annotations

from typing import Any

import pytest

from acatome_quest_mcp.dedup import StoreDedup
from acatome_quest_mcp.misconceptions import MisconceptionCode
from acatome_quest_mcp.models import (
    Candidate,
    RequestStatus,
    ResolvedRef,
    UpdateMode,
)
from acatome_quest_mcp.resolver import Resolver
from acatome_quest_mcp.service import (
    NotFoundError,
    QuestService,
    RateLimitError,
)

from .fake_db import FakeDB

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeStore:
    """Stand-in for ``acatome_store.Store``."""

    def __init__(self, rows: dict[str, dict] | None = None) -> None:
        self._rows = rows or {}

    def get(self, identifier):
        return self._rows.get(identifier)


def _resolver(
    *,
    crossref: dict[str, Any] | None = None,
    s2_title: dict[str, Any] | None = None,
    s2_id: dict[str, Any] | None = None,
) -> Resolver:
    """Build a Resolver with canned responses.

    ``crossref`` is a template: we echo the caller's DOI back so that
    ``submit({"doi": "10.1/a"})`` resolves to DOI ``10.1/a`` (not the
    hard-coded template DOI — that would break per-paper idempotency tests).
    """

    def cr_fn(doi: str, mailto: str = "") -> dict[str, Any] | None:
        if crossref is None:
            return None
        # If the template supplies its own DOI, preserve it — this models the
        # "DOI resolves to a different paper" scenario.  Otherwise echo the
        # request so per-paper idempotency tests see per-paper resolutions.
        if crossref.get("doi"):
            return dict(crossref)
        return {**crossref, "doi": doi}

    return Resolver(
        crossref_fn=cr_fn,
        s2_title_fn=lambda title, api_key="": s2_title,
        s2_id_fn=lambda pid, api_key="": s2_id,
    )


def _crossref(
    doi: str | None = None,
    title: str = "Sample paper",
    authors: list[str] | None = None,
    year: int = 2024,
) -> dict[str, Any]:
    """Template Crossref response.

    ``doi=None`` (default) means the wrapper will echo the caller's DOI;
    passing an explicit DOI simulates "this DOI resolves to a different
    paper than expected" (the mismatch scenario).
    """
    d: dict[str, Any] = {
        "title": title,
        "authors": [{"name": a} for a in (authors or ["Smith, J."])],
        "year": year,
        "journal": "Sample Journal",
    }
    if doi is not None:
        d["doi"] = doi
    return d


def _mk_service(
    *,
    store_rows: dict[str, dict] | None = None,
    crossref: dict[str, Any] | None = None,
    s2_title: dict[str, Any] | None = None,
    s2_id: dict[str, Any] | None = None,
) -> tuple[QuestService, FakeDB]:
    db = FakeDB()
    svc = QuestService(
        db,  # type: ignore[arg-type]
        resolver=_resolver(crossref=crossref, s2_title=s2_title, s2_id=s2_id),
        dedup=StoreDedup(store=_FakeStore(store_rows)),
    )
    return svc, db


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


class TestSubmitDedup:
    def test_store_hit_returns_found_in_store(self) -> None:
        svc, _ = _mk_service(
            store_rows={
                "10.1/sample": {
                    "slug": "smith2024sample",
                    "doi": "10.1/sample",
                    "arxiv_id": None,
                    "title": "Sample paper",
                    "year": 2024,
                }
            },
        )
        req = svc.submit({"doi": "10.1/sample"})
        assert req.status == RequestStatus.FOUND_IN_STORE
        assert req.resolved.ref == "smith2024sample"
        assert req.resolved.score == 1.0

    def test_store_hit_under_different_doi_flags_duplicate(self) -> None:
        # User submits DOI A; resolver maps to DOI B; store has DOI B.
        svc, _ = _mk_service(
            store_rows={
                "10.1/real": {
                    "slug": "wang2020state",
                    "doi": "10.1/real",
                    "arxiv_id": None,
                    "title": "The real paper",
                    "year": 2020,
                }
            },
            crossref=_crossref(doi="10.1/real", title="The real paper"),
        )
        req = svc.submit({"doi": "10.1/wrongdoi"})
        codes = {m.code for m in req.misconceptions}
        assert MisconceptionCode.DUPLICATE_OF in codes
        assert req.status == RequestStatus.FOUND_IN_STORE


class TestSubmitResolution:
    def test_new_paper_enters_queued(self) -> None:
        svc, db = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/sample"})
        assert req.status == RequestStatus.QUEUED
        assert req.resolved.title == "Sample paper"
        assert req.resolved.score > 0.9
        # Persisted.
        stored = db.get(req.id)
        assert stored is not None
        assert stored.status == RequestStatus.QUEUED

    def test_title_mismatch_routes_to_needs_user(self) -> None:
        svc, _ = _mk_service(
            crossref=_crossref(title="Completely unrelated topic"),
        )
        req = svc.submit(
            {"doi": "10.1/sample", "title": "Anion exchange for NOx reduction"}
        )
        assert req.status == RequestStatus.NEEDS_USER
        assert any(
            m.code == MisconceptionCode.DOI_TITLE_MISMATCH for m in req.misconceptions
        )

    def test_empty_ref_rejected(self) -> None:
        svc, _ = _mk_service()
        with pytest.raises(ValueError):
            svc.submit({})


class TestSubmitIdempotency:
    def test_same_doi_returns_same_id(self) -> None:
        svc, db = _mk_service(crossref=_crossref())
        a = svc.submit({"doi": "10.1/sample"}, created_by="asa")
        b = svc.submit({"doi": "10.1/sample"}, created_by="asa")
        assert a.id == b.id
        # Only one row in the DB.
        rows = db.find()
        assert len(rows) == 1

    def test_different_dois_create_different_rows(self) -> None:
        svc, db = _mk_service(crossref=_crossref())
        a = svc.submit({"doi": "10.1/a"})
        b = svc.submit({"doi": "10.1/b"})
        assert a.id != b.id
        rows = db.find()
        assert len(rows) == 2


class TestSubmitDryRun:
    def test_dry_run_does_not_persist(self) -> None:
        svc, db = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/sample"}, dry_run=True)
        assert req.resolved.title == "Sample paper"
        rows = db.find()
        assert len(rows) == 0


class TestSubmitRateLimit:
    def test_exceeded_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Patch the constant the service already imported.
        import acatome_quest_mcp.service as svc_module

        monkeypatch.setattr(svc_module, "MAX_OPEN_PER_AGENT", 2)

        svc, _ = _mk_service(crossref=_crossref())
        svc.submit({"doi": "10.1/a"}, created_by="bot")
        svc.submit({"doi": "10.1/b"}, created_by="bot")
        with pytest.raises(RateLimitError):
            svc.submit({"doi": "10.1/c"}, created_by="bot")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


class TestStatus:
    def test_by_id(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        created = svc.submit({"doi": "10.1/sample"})
        out = svc.status(str(created.id))
        assert not isinstance(out, list)
        assert out.id == created.id

    def test_by_id_missing(self) -> None:
        svc, _ = _mk_service()
        from uuid import uuid4

        with pytest.raises(NotFoundError):
            svc.status(str(uuid4()))

    def test_filter_by_status(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        svc.submit({"doi": "10.1/a"})
        rows = svc.status(filter={"status": "queued"})
        assert isinstance(rows, list)
        assert len(rows) == 1

    def test_filter_by_has_misconception(self) -> None:
        svc, _ = _mk_service(
            crossref=_crossref(title="Foo"),
        )
        svc.submit({"doi": "10.1/good", "title": "Foo"})  # no misc
        svc.submit(
            {"doi": "10.1/bad", "title": "Totally different thing"}
        )  # mismatch misc
        flagged = svc.status(filter={"has_misconception": True})
        assert isinstance(flagged, list)
        assert len(flagged) == 1

    def test_filter_by_source_document(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        svc.submit({"doi": "10.1/a"}, source={"document": "ch02.tex"})
        svc.submit({"doi": "10.1/b"}, source={"document": "ch03.tex"})
        rows = svc.status(filter={"source_document": "ch02.tex"})
        assert isinstance(rows, list)
        assert len(rows) == 1
        assert rows[0].source["document"] == "ch02.tex"


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_cancel(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/a"})
        out = svc.update(str(req.id), UpdateMode.CANCEL)
        assert out.status == RequestStatus.CANCELLED

    def test_priority(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/a"})
        out = svc.update(str(req.id), "priority", priority=9)
        assert out.priority == 9

    def test_flag_adds_misconception(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/a"})
        out = svc.update(
            str(req.id),
            "flag",
            code="retracted",
            evidence="Retraction Watch 2024-08-12",
        )
        assert any(m.code == MisconceptionCode.RETRACTED for m in out.misconceptions)

    def test_flag_allowed_on_terminal_request(self) -> None:
        svc, _ = _mk_service(
            store_rows={
                "10.1/a": {"slug": "s", "doi": "10.1/a", "title": "t", "year": 2024}
            }
        )
        req = svc.submit({"doi": "10.1/a"})  # found_in_store → terminal
        out = svc.update(str(req.id), "flag", code="retracted")
        assert len(out.misconceptions) == 1
        # But cancel on terminal is rejected.
        with pytest.raises(ValueError):
            svc.update(str(req.id), "cancel")

    def test_confirm_picks_candidate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc, db = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/a"})
        # Inject a candidate directly (resolver doesn't emit them in MVP).
        candidate_ref = ResolvedRef(
            doi="10.1/alt", title="An alternate paper", score=0.8
        )
        db.update(
            req.id,
            candidates=[Candidate(ref=candidate_ref, reason="near title match")],
        )
        out = svc.update(str(req.id), "confirm", choice=0)
        assert out.resolved.doi == "10.1/alt"
        assert out.status == RequestStatus.QUEUED
        assert out.candidates == []

    def test_confirm_rejects_bad_choice(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/a"})
        with pytest.raises(ValueError):
            svc.update(str(req.id), "confirm", choice=5)

    def test_repoint_reresolves(self) -> None:
        svc, _db = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/broken"})
        # Flip the resolver's canned answer for the new DOI.
        svc.resolver._crossref_fn = lambda doi, mailto="": _crossref(
            doi=doi, title="Real paper"
        )
        out = svc.update(str(req.id), "repoint", doi="10.1/correct")
        assert out.resolved.doi == "10.1/correct"
        assert out.resolved.title == "Real paper"
        assert out.status == RequestStatus.QUEUED

    def test_repoint_requires_doi(self) -> None:
        svc, _ = _mk_service(crossref=_crossref())
        req = svc.submit({"doi": "10.1/a"})
        with pytest.raises(ValueError):
            svc.update(str(req.id), "repoint")

    def test_unknown_id_errors(self) -> None:
        svc, _ = _mk_service()
        from uuid import uuid4

        with pytest.raises(NotFoundError):
            svc.update(str(uuid4()), "cancel")
