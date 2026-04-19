# Security policy

## Supported versions

The latest released minor version receives security fixes.

## Reporting a vulnerability

Please report security issues privately via the GitHub security advisory
interface on this repository.  Do not open public issues for vulnerabilities.

## Scope notes

- acatome-quest-mcp fetches papers from **open-access sources only**.  It
  ships no hooks for paywall circumvention and will refuse PRs that add
  them.  If you believe an "OA" source is not genuinely open, please file a
  report so we can remove it.
- Fetched PDFs are written to a user-configurable directory
  (`ACATOME_INBOX`).  Writes are sandboxed to that directory; no path
  traversal is accepted in filenames.
- Metadata and URLs returned by Crossref / Unpaywall / OpenAlex / S2 are
  treated as untrusted input and HTML-escaped before surfacing to MCP
  clients.
