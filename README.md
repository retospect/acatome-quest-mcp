# acatome-quest-mcp

**Paper-request MCP for scientific papers.**  The missing piece between
[`precis-mcp`](https://github.com/retospect/precis-mcp) (navigates what's
already in your library) and
[`acatome-extract`](https://github.com/retospect/acatome-extract) (ingests
PDFs that land in an inbox).

An LLM says *"I want this paper"* (DOI, arXiv id, title, or free-form citation).
Quest:

1. **Checks the store first** — no duplicate work if we already have it.
2. **Resolves the metadata** via Crossref + Semantic Scholar + arXiv.
3. **Flags misconceptions** — broken DOI, DOI↔title mismatch, duplicate of an
   existing slug, fabrication suspect.
4. **Fetches the PDF** from legitimate open-access sources only and drops it
   into the existing watch inbox, where `acatome-extract` takes over.
5. Returns a **request id** in milliseconds.  Slow extraction happens out of
   band; the MCP call never blocks.

## Open access only — by policy

Quest fetches from **arXiv, Unpaywall, OpenAlex, Europe PMC, and Semantic
Scholar's open-access index only**.  It does not, will not, and cannot be
configured to use Sci-Hub, LibGen, institutional proxies without explicit opt-
in, or any other paywall-circumvention mechanism.  Failed retrievals yield a
`needs_user` status with the publisher URL, for you to retrieve manually.

## Install

```bash
pip install acatome-quest-mcp
# or with uv
uv add acatome-quest-mcp
```

For dedup against a local `acatome-store`:

```bash
pip install 'acatome-quest-mcp[store]'
```

## Three tools

| Tool | What it does |
|------|--------------|
| `submit(ref, *, dry_run=False, source=None, priority=0, created_by=None)` | Resolve + optionally queue.  Idempotent. |
| `status(id=None, *, filter=None)` | Read one or many requests. |
| `update(id, mode, **kwargs)` | Mutate.  Modes: `confirm`, `repoint`, `flag`, `priority`, `cancel`. |

### submit

```python
submit(ref={"doi": "10.1021/jacs.2c01234"})
submit(ref={"title": "Anion exchange membranes for NOx reduction",
            "authors": ["Feng, Z."], "year": 2024})
submit(ref={"raw": "Feng et al. 2024, Adv. Funct. Mater. 34, 2300512"})
submit(ref={"doi": "10.1234/x"}, dry_run=True)      # resolve only, no queue
submit(ref={"doi": "10.1234/x"},
       source={"document": "ch02.tex", "line": 147})
```

Response:

```json
{
  "id": "9f3b…",
  "status": "found_in_store",
  "resolved": {"doi": "10.1021/jacs.2c01234",
               "title": "…", "authors": ["…"], "year": 2024,
               "ref": "smith2022jacs"},
  "candidates": [],
  "misconceptions": []
}
```

### status

```python
status(id="9f3b…")
status(filter={"status": "needs_user"})
status(filter={"created_by": "asa", "has_misconception": True})
status(filter={"source_document": "ch02.tex"})
```

### update

```python
update(id, mode="confirm", choice=0)            # pick candidates[0]
update(id, mode="repoint", doi="10.1023/A:…")   # user-corrected DOI
update(id, mode="flag", code="retracted",
       evidence="Retraction Watch 2024-08-12")
update(id, mode="priority", priority=5)
update(id, mode="cancel")
```

## Statuses

| Status | Meaning |
|--------|---------|
| `queued` | Accepted, not yet fetched |
| `resolving` | Metadata lookup in progress (transient) |
| `found_in_store` | Dedup hit — slug in `resolved.ref` |
| `needs_user` | Disambiguation or manual fetch required |
| `fetching` | Runner has claimed and is downloading |
| `ingesting` | PDF in inbox, waiting for `acatome-extract watch` |
| `ingested` | Extraction done, slug in `resolved.ref` |
| `extract_failed` | PDF delivered but extraction failed |
| `failed` | All sources exhausted |
| `cancelled` | `update(mode=cancel)` was called |

## Misconception codes

| Code | Severity | Trigger |
|------|----------|---------|
| `doi_invalid` | major | Crossref 404 or syntactically malformed |
| `doi_truncated` | major | 404, but `doi + digit` resolves |
| `doi_title_mismatch` | critical | DOI resolves but title fuzz < 60 vs request |
| `title_not_found` | critical | No S2/Crossref hit (fabrication suspect) |
| `duplicate_of` | minor | Already in store under another slug |
| `retracted` | critical | S2 / Retraction Watch flag |
| `preprint_of` | info | arXiv preprint of a later journal paper |

## Architecture

```text
 agent ──submit()──► acatome-quest-mcp (FastMCP, stdio)
                            │
                            ▼
              cluster.papers.requests (Postgres)
                            │
                            ▼
           acatome-quest-runner (launchd, poll 30 s)
                  │
          fetch: arxiv → unpaywall → …
                  │
                  ▼
          ~/.acatome/inbox/<slug>__<hash>.pdf
                  │
                  ▼
          acatome-extract watch  →  acatome-store
                  ▲
                  └── runner polls by DOI, flips to `ingested`
```

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `DATABASE_URL` | `postgresql://localhost/cluster` | Postgres DSN |
| `QUEST_SCHEMA` | `papers` | Schema name for the `requests` table |
| `ACATOME_INBOX` | `~/.acatome/inbox` | Drop directory watched by `acatome-extract` |
| `UNPAYWALL_EMAIL` | *(required at runner start)* | Polite-pool contact |
| `ACATOME_CROSSREF_MAILTO` | *(recommended)* | Crossref polite pool |
| `SEMANTIC_SCHOLAR_API_KEY` | *(optional)* | Raises S2 rate limit |
| `QUEST_POLL_INTERVAL` | `30` | Runner tick seconds |
| `QUEST_MAX_CONCURRENT` | `4` | Max parallel fetches |
| `QUEST_INGEST_TIMEOUT` | `900` | Seconds to wait for ingest after PDF drop |
| `QUEST_MAX_OPEN_PER_AGENT` | `50` | Per-`created_by` cap |

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy src tests
```

## License

GPL-3.0-or-later.  See [LICENSE](LICENSE).
