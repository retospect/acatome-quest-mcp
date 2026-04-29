# acatome-quest-mcp — Plan

Status: **approved, Phase 1 in build**
Owner: cluster ops
Last updated: 2026-04-19

## Purpose

Close the gap between `precis-mcp` (navigates what's already in the store) and
`acatome-extract watch` (ingests whatever appears in the inbox).  Quest is the
queue in between:

- An LLM or user says *"I want this paper"* (DOI, arXiv, title, free-form
  citation, or a whole `.bib` file).
- Quest checks if we already have it (dedup against `acatome-store`).
- Quest resolves the metadata (`acatome-meta` cascade: Crossref → S2 → arXiv).
- Quest flags **misconceptions** (invalid DOI, DOI↔title mismatch, duplicate of
  existing slug, fabrication suspect, retracted).
- A background runner fetches the PDF from **legitimate OA sources only**
  (arXiv, Unpaywall, OpenAlex, Europe PMC, S2 OA) and drops it in
  `~/.acatome/inbox/`, where `acatome-extract watch` takes over.
- The MCP call returns a **request id** in milliseconds — nothing blocks on a
  slow Marker extraction.

## Non-goals

- **No paywall circumvention.**  No Sci-Hub, no LibGen, no cookie theft.  This
  is a policy, written into the README, not a configurable switch.
- **No replacement for `acatome-extract`.**  Quest hands off to the existing
  watch pipeline and polls for completion; it does not duplicate extraction.
- **Not a campaign engine.**  One request = one paper.  Multi-step work goes
  through `sortie-mcp`.

## MCP tool surface (3 tools, by design)

| Tool | Signature | What it does |
|------|-----------|--------------|
| `submit` | `submit(ref, *, dry_run=False, source=None, priority=0, created_by=None)` | Resolve + optionally queue.  Idempotent: same DOI while still open → same `id`. |
| `status` | `status(id=None, *, filter=None)` | Read one (by `id`) or many (by filter). |
| `update` | `update(id, mode, **kwargs)` | Mutate.  Modes: `confirm`, `repoint`, `flag`, `priority`, `cancel`. |

Mirrors `precis.put()`'s single-verb-with-modes pattern.  Never mixes "request"
as both a noun and a verb in the same name.

### `submit` contract

```python
submit(ref={"doi": "10.1021/jacs.2c01234"})
submit(ref={"title": "Anion exchange membranes for NOx reduction",
            "authors": ["Feng, Z."], "year": 2024})
submit(ref={"raw": "Feng et al. 2024, Adv. Funct. Mater. 34, 2300512"})
submit(ref={"doi": "10.1234/x"}, dry_run=True)      # resolve only
submit(ref={"doi": "10.1234/x"},
       source={"document": "ch02.tex", "line": 147})
```

Returns:

```json
{
  "id": "9f…",
  "status": "found_in_store | queued | needs_user | ...",
  "resolved": {"doi": "…", "title": "…", "authors": [...], "year": 2024},
  "candidates": [ {...}, {...} ],
  "misconceptions": [ {"code": "doi_title_mismatch", "severity": "critical",
                       "evidence": "DOI resolves to Mohd Riyaz 2025"} ]
}
```

### `status` contract

```python
status(id="9f…")
status(filter={"status": "needs_user"})
status(filter={"created_by": "asa", "has_misconception": True})
status(filter={"source_document": "ch02.tex"})
```

Returns a request card or a list of cards.

### `update` contract

```python
update(id, mode="confirm", choice=0)            # pick candidates[0]
update(id, mode="repoint", doi="10.1023/A:…")   # user-corrected DOI
update(id, mode="flag", code="retracted", evidence="…")
update(id, mode="priority", priority=5)
update(id, mode="cancel")
```

## Data model

Postgres `cluster` DB on caspar, new schema `papers`.

```sql
CREATE SCHEMA IF NOT EXISTS papers;

CREATE TABLE papers.requests (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_by      text,                      -- agent slug or user id
    source          jsonb,                     -- {document, line, note}
    -- input (what was asked for)
    input_doi       text,
    input_arxiv     text,
    input_pmid      text,
    input_title     text,
    input_authors   text[],
    input_year      int,
    input_raw       text,
    -- resolved (what it actually is)
    resolved_doi    text,
    resolved_arxiv  text,
    resolved_title  text,
    resolved_authors text[],
    resolved_year   int,
    resolved_ref    text,                      -- acatome-store slug once ingested
    resolved_score  real,                      -- 0..1 confidence
    candidates      jsonb DEFAULT '[]',
    -- lifecycle
    status          text NOT NULL DEFAULT 'queued',
    misconceptions  jsonb DEFAULT '[]',
    attempts        jsonb DEFAULT '[]',
    priority        int  NOT NULL DEFAULT 0,
    not_before      timestamptz NOT NULL DEFAULT now(),
    supersedes      uuid REFERENCES papers.requests(id),
    -- ops
    pdf_hash        text,
    pdf_path        text,
    last_error      text
);

CREATE INDEX ON papers.requests (status) WHERE status IN ('queued','fetching','ingesting');
CREATE INDEX ON papers.requests (created_by);
CREATE INDEX ON papers.requests (input_doi);
CREATE INDEX ON papers.requests (resolved_doi);
CREATE INDEX ON papers.requests (resolved_ref);
CREATE INDEX ON papers.requests (not_before) WHERE status = 'queued';
```

### Status enum

```
queued         - accepted, not yet fetched
resolving      - metadata lookup in progress (transient)
found_in_store - dedup hit, slug in resolved_ref, done
needs_user     - disambiguation or manual fetch required
fetching       - runner has claimed and is downloading
ingesting      - PDF in inbox, waiting for acatome-extract watch
ingested       - slug in resolved_ref, done
extract_failed - inbox picked up PDF but extraction failed
failed         - unrecoverable (all sources exhausted)
cancelled      - user/agent called update(mode=cancel)
```

### Misconception codes (shared Python enum)

| Code | Severity | Trigger |
|------|----------|---------|
| `doi_invalid` | major | Crossref 404 or syntactically malformed |
| `doi_truncated` | major | 404, but `doi + digit` resolves |
| `doi_title_mismatch` | **critical** | DOI resolves but title fuzz < 60 vs request |
| `title_not_found` | critical | No S2/Crossref hit for the title (fabrication suspect) |
| `duplicate_of` | minor | Already in store under another slug (evidence = slug) |
| `retracted` | critical | S2 / Retraction Watch flag set |
| `preprint_of` | info | arXiv preprint of a later journal version |

## Architecture

```
┌─────────────┐    submit()         ┌────────────────────┐
│  agent /    │ ──────────────────► │ acatome-quest-mcp  │
│  user CLI   │                     │  (FastMCP stdio)   │
└─────────────┘ ◄───── id ──────────└─────────┬──────────┘
                                              │
                                              ▼
                              ┌───────────────────────────┐
                              │  cluster.papers.requests  │
                              └───────┬───────────────────┘
                                      │
                                      ▼
                 ┌─────────────────────────────────────────┐
                 │  acatome-quest-runner (launchd, 30 s)   │
                 │                                         │
                 │  claim queued ── fetch (arxiv, unpay…)  │
                 │       │                │                │
                 │       │                ▼                │
                 │       │         ~/.acatome/inbox/*.pdf  │
                 │       │                │                │
                 │       │                ▼                │
                 │       │     acatome-extract watch       │
                 │       │                │                │
                 │       │                ▼                │
                 │       └────── poll acatome-store ──────►│
                 │              (by doi → set ingested)    │
                 └─────────────────────────────────────────┘
```

## Fetch source order (all OA)

1. **arXiv** (if `arxiv_id` known) — direct PDF URL.
2. **Unpaywall** — `api.unpaywall.org/v2/<doi>?email=<polite>`.
3. **OpenAlex** — `api.openalex.org/works/doi:<doi>` → `best_oa_location.pdf_url`.
4. **Europe PMC / PMC** — if PMCID known.
5. **Semantic Scholar** `openAccessPdf` — already a dependency.
6. **Crossref** `link` array — rare but sometimes yields OA.

Failure after all sources → `needs_user` with publisher URL + DOI URL for
manual retrieval.

Per-source token buckets.  Polite-pool email (`UNPAYWALL_EMAIL`, Crossref
mailto) required at start-up — runner refuses to start without them.

## Key operational invariants

- **Idempotency.**  Same DOI submitted twice while open → same `id`.  After a
  terminal state, new submission gets a new `id` with `supersedes` pointing at
  the old one.
- **Per-agent rate limit.**  Max 50 open requests per `created_by` (configurable).
  Prevents runaway loops.
- **Retry / backoff.**  Per-source `not_before` with exponential backoff; cap 5
  total attempts → `needs_user`.
- **Ingest detection.**  Runner computes SHA-256 of the PDF it writes, then
  polls `acatome-store` by DOI.  Once a ref appears, flip to `ingested` with
  the slug.  If no match after `QUEST_INGEST_TIMEOUT` (default 15 min) →
  `extract_failed`.
- **Reconciliation.**  On every tick, scan `acatome-store.refs` for DOIs
  matching `needs_user` requests.  Closes the loop when a user drops a PDF
  into the inbox manually.
- **Retention.**  Terminal rows are kept forever (audit trail).  If `attempts`
  or `candidates` bloat, split to sidecar tables (Phase 3).

## Agent-prompt guardrail

Without this, the MCP enables a new failure mode.  Every agent that loads
quest must include in its system prompt:

> Before citing a paper by DOI or title, call `quest.submit`.  If it returns
> `found_in_store`, fetch text via `precis.get(<slug>)`.  If it returns
> `queued` or `needs_user`, the paper is **not yet available** — cite only the
> DOI, never invent quotes from the title or abstract.  Surface any
> `misconceptions` to the user.

Added to `grimoire/agents/researcher.md` and
`grimoire/agents/writer.md`; also to `grimoire/review/review-citations.md`
so reviewers consume the misconception feed.

## Phasing

**Phase 1 (this PR) — MVP:**

- Package skeleton, tests harness.
- Schema + CRUD.
- `submit` / `status` / `update` tools.
- Dedup against acatome-store (DOI + arXiv id).
- Resolution via acatome-meta (DOI → Crossref, title → S2, arXiv id → S2).
- arXiv + Unpaywall fetchers.
- Misconception codes: `doi_invalid`, `doi_title_mismatch`, `duplicate_of`.
- CLI: `quest submit`, `quest status`, `quest runner`, `quest reconcile`.
- Unit tests with mocked HTTP (Crossref / Unpaywall / S2).

**Phase 2 — Robustness:**

- OpenAlex, Europe PMC, S2 OA PDF fallbacks.
- DOI-truncation repair heuristic.
- Retraction Watch integration.
- Per-source token-bucket rate limiter.
- `quest submit-bib` CLI (batch from `.bib`).
- Ansible role `roles/acatome_quest/`, playbook `23-acatome-quest.yml`,
  launchd plist on balthazar.

**Phase 3 — Agent UX:**

- Agent prompt updates (research / writing / review-citations).
- precis URI-scheme plugin: `get(id='req:<id>')`, `get(id='req:/pending')`,
  `get(id='req:/misconceptions')`.
- Discord slash command `/papers pending`.

## Open questions deferred

- Retraction Watch has rate-limited APIs — worth the integration cost?
  (Deferred to Phase 2.)
- Should we cache Crossref/S2 responses (e.g. in `papers.metadata_cache`) to
  reduce re-resolution cost?  (Probably yes in Phase 3.)
- Batch-submit from DOCX / `.tex` with auto-extraction of citations — nice-to-
  have but depends on precis walking a document's citation list.
