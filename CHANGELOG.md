# Changelog

## 0.3.0 — Full sync rewrite (April 2026)

Replaces the `asyncpg` / `asyncio.to_thread` / `async httpx.AsyncClient`
stack with a fully synchronous one.  The async layer was cargo-culted
plumbing: every caller (precis handlers, CLI, runner, resolver, MCP
server) was already sync or trivially sync-convertible, and the
wrapped libraries (`habanero`, `semanticscholar`, `wolframalpha`) are
blocking anyway.  Deleting the bridge removes a whole class of
"why is my handler async again?" questions.

### Changed

- **`acatome_quest_mcp.db.DB` now uses `psycopg3` + `psycopg_pool`.**
  `@/Users/bots/Documents/openclaw-cluster/pips/packages/acatome-quest-mcp/src/acatome_quest_mcp/db.py:131-158`
  `ConnectionPool(open=True, kwargs={"autocommit": True})` — eager
  pool open matches the previous asyncpg behaviour.  All queries
  converted from `$1, $2` to `%s` placeholders.  Row access switched
  from asyncpg's named-tuple to `row_factory=dict_row` so the existing
  `_row_to_request(row)` helper keeps working unchanged.
- **`Resolver` dropped `asyncio.to_thread`.**
  `@/Users/bots/Documents/openclaw-cluster/pips/packages/acatome-quest-mcp/src/acatome_quest_mcp/resolver.py`
  `habanero` / `semanticscholar` are sync libraries; the thread-pool
  wrap was pure ceremony.
- **`QuestService` is sync end-to-end.**  `submit`, `status`, `update`,
  `submit_file` all lost their `async def`s.  `_load_pdf` uses
  `httpx.Client` instead of `httpx.AsyncClient`.
- **`Runner` is a plain blocking loop.**
  `@/Users/bots/Documents/openclaw-cluster/pips/packages/acatome-quest-mcp/src/acatome_quest_mcp/runner.py`
  `asyncio.sleep(POLL_INTERVAL)` → `time.sleep(POLL_INTERVAL)`.  The
  runner processes one quest at a time anyway (bounded by OA-source
  politeness, not concurrency); no benefit was being extracted from
  the event loop.
- **MCP tools (`server.py`) are sync.**  FastMCP's sync dispatch
  invokes them directly — no `asyncio.run` inside every tool.
- **CLI commands are sync.**  `acatome-quest submit`, `status`,
  `update`, `submit-file`, `report`, `runner`, `reconcile` all lost
  their `asyncio.run` wrappers.
- **Fetchers (`arxiv`, `unpaywall`) use `httpx.Client`.**
  The `Fetcher` protocol's `try_fetch` is now a plain sync method.

### Removed

- `pytest-asyncio` dev dependency.
- `asyncio_mode = "auto"` from `pyproject.toml`'s `[tool.pytest.ini_options]`.
- Dead `SqlDedup` class from `dedup.py` — never imported anywhere,
  still written in asyncpg after the rewrite.  If SQL-mode dedup is
  ever genuinely needed in a minimal runner container, it should be
  reintroduced on `psycopg3`.

### Dependencies

- `asyncpg>=0.30` → `psycopg[binary]>=3.1` + `psycopg_pool>=3.2`

### Tests

- All 111 tests converted to sync (`async def test_*` → `def test_*`,
  `await svc.x()` → `svc.x()`, `AsyncMock` → `MagicMock`, `AsyncClient`
  → `Client`, `@pytest.mark.asyncio` removed).  `tests/fake_db.py`
  methods are now plain `def`.  No functional test changes.

## 0.2.0 — (historical)

Initial internal release.
