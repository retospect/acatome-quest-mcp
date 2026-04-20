"""Command-line interface — ``acatome-quest``.

Subcommands::

    acatome-quest submit <DOI|arxiv|--title ...>
    acatome-quest status [<id> | --filter status=queued] [--count]
    acatome-quest update <id> <mode> [args...]
    acatome-quest submit-file (--url URL | --path PATH) (--request-id ID | ...)
    acatome-quest report [--status needs_user ...] [--document ch02.tex]
    acatome-quest runner [--once]
    acatome-quest reconcile
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any

from .db import DB
from .models import PaperRequest, RequestStatus, UpdateMode
from .report import render_report
from .runner import Runner
from .service import NotFoundError, QuestService, RateLimitError


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="acatome-quest")
    p.add_argument("--log-level", default=os.environ.get("QUEST_LOG_LEVEL", "INFO"))
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit", help="Submit a paper request")
    s.add_argument("identifier", nargs="?", help="DOI, arXiv id, or raw citation")
    s.add_argument("--doi")
    s.add_argument("--arxiv")
    s.add_argument("--title")
    s.add_argument("--author", action="append", default=[], dest="authors")
    s.add_argument("--year", type=int)
    s.add_argument("--raw")
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--priority", type=int, default=0)
    s.add_argument("--created-by", default=os.environ.get("USER"))
    s.add_argument("--document", help="Source document (e.g. ch02.tex)")
    s.add_argument("--line", type=int)

    st = sub.add_parser("status", help="Read one or many requests")
    st.add_argument("id", nargs="?")
    st.add_argument(
        "--filter",
        action="append",
        default=[],
        metavar="KEY=VAL",
        help="Filter (repeatable). Keys: status, created_by, has_misconception, "
        "source_document, limit",
    )
    st.add_argument(
        "--count",
        action="store_true",
        help="Print only the number of matching requests (for shell callers).",
    )

    u = sub.add_parser("update", help="Mutate a request")
    u.add_argument("id")
    u.add_argument("mode", choices=[m.value for m in UpdateMode])
    u.add_argument("--choice", type=int)
    u.add_argument("--doi")
    u.add_argument("--code")
    u.add_argument("--severity")
    u.add_argument("--evidence")
    u.add_argument("--priority", type=int)

    sf = sub.add_parser(
        "submit-file",
        help="Attach a user-supplied PDF to a request (URL or local path)",
    )
    sf_src = sf.add_mutually_exclusive_group(required=True)
    sf_src.add_argument("--url", help="HTTP(S) URL to fetch the PDF from")
    sf_src.add_argument("--path", help="Local file path to read the PDF from")
    sf_target = sf.add_mutually_exclusive_group(required=True)
    sf_target.add_argument(
        "--request-id",
        help="Attach to an existing request id",
    )
    sf_target.add_argument(
        "--doi",
        dest="sf_doi",
        help="Create a new request for this DOI and attach",
    )
    sf_target.add_argument(
        "--arxiv",
        dest="sf_arxiv",
        help="Create a new request for this arXiv id and attach",
    )
    sf_target.add_argument(
        "--title",
        dest="sf_title",
        help="Create a new request for this title and attach",
    )
    sf.add_argument("--filename", help="Filename hint for the written file")
    sf.add_argument("--created-by", default=os.environ.get("USER"))

    rp = sub.add_parser(
        "report",
        help="Render a markdown exception report for papers needing manual action",
    )
    rp.add_argument(
        "--status",
        action="append",
        default=None,
        help="Statuses to include (repeatable). Default: needs_user, failed, "
        "extract_failed.",
    )
    rp.add_argument("--created-by", help="Only include requests from this agent.")
    rp.add_argument("--document", help="Only include requests for this source file.")
    rp.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
    )
    rp.add_argument(
        "--title",
        default="Papers needing manual acquisition",
        help="Report heading.",
    )

    r = sub.add_parser("runner", help="Run the background fetcher daemon")
    r.add_argument("--once", action="store_true")

    sub.add_parser("reconcile", help="Close requests whose paper is now in the store")

    return p


async def _amain(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL", "postgresql://localhost/cluster")
    schema = os.environ.get("QUEST_SCHEMA", "papers")
    db = DB(dsn, schema=schema)
    await db.connect()
    await db.migrate()

    try:
        if args.cmd == "submit":
            return await _cmd_submit(db, args)
        if args.cmd == "status":
            return await _cmd_status(db, args)
        if args.cmd == "update":
            return await _cmd_update(db, args)
        if args.cmd == "submit-file":
            return await _cmd_submit_file(db, args)
        if args.cmd == "report":
            return await _cmd_report(db, args)
        if args.cmd == "runner":
            runner = Runner(db)
            try:
                if args.once:
                    n = await runner.tick()
                    print(f"Processed {n} request(s)")
                else:
                    await runner.run_forever()
            finally:
                await runner.close()
            return 0
        if args.cmd == "reconcile":
            runner = Runner(db)
            try:
                await runner._reconcile()
                await runner._escalate_timeouts()
            finally:
                await runner.close()
            return 0
        raise ValueError(f"unknown command: {args.cmd}")
    finally:
        await db.close()


async def _cmd_submit(db: DB, args: argparse.Namespace) -> int:
    svc = QuestService(db)
    ref: dict[str, Any] = {
        "doi": args.doi,
        "arxiv": args.arxiv,
        "title": args.title,
        "authors": args.authors,
        "year": args.year,
        "raw": args.raw,
    }
    if args.identifier:
        ident = args.identifier
        if ident.lower().startswith(("10.", "https://doi.org/", "doi:")):
            ref["doi"] = ident
        elif "arxiv" in ident.lower() or _looks_like_arxiv(ident):
            ref["arxiv"] = ident
        else:
            ref["raw"] = ident
    source = {}
    if args.document:
        source["document"] = args.document
    if args.line is not None:
        source["line"] = args.line
    try:
        req = await svc.submit(
            ref,
            dry_run=args.dry_run,
            source=source or None,
            priority=args.priority,
            created_by=args.created_by,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except RateLimitError as exc:
        print(f"rate limit: {exc}", file=sys.stderr)
        return 3
    print(json.dumps(req.to_dict(), indent=2, default=str))
    return 0


async def _cmd_status(db: DB, args: argparse.Namespace) -> int:
    svc = QuestService(db)
    if args.id:
        try:
            req = await svc.status(args.id)
        except NotFoundError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        # When id is given, status() returns a single request, not a list.
        assert not isinstance(req, list)
        if args.count:
            print(1)
            return 0
        print(json.dumps(req.to_dict(), indent=2, default=str))
        return 0
    f: dict[str, Any] = {}
    for pair in args.filter:
        if "=" not in pair:
            print(f"bad filter: {pair}", file=sys.stderr)
            return 2
        k, v = pair.split("=", 1)
        if k == "has_misconception":
            f[k] = v.lower() in ("1", "true", "yes")
        elif k == "limit":
            f[k] = int(v)
        else:
            f[k] = v
    out = await svc.status(filter=f)
    if args.count:
        print(len(out) if isinstance(out, list) else 1)
        return 0
    if isinstance(out, list):
        print(json.dumps([r.to_dict() for r in out], indent=2, default=str))
    else:
        print(json.dumps(out.to_dict(), indent=2, default=str))
    return 0


_REPORT_DEFAULT_STATUSES = (
    RequestStatus.NEEDS_USER,
    RequestStatus.FAILED,
    RequestStatus.EXTRACT_FAILED,
)

_REPORT_STATUS_ORDER = {s: i for i, s in enumerate(_REPORT_DEFAULT_STATUSES)}


async def _cmd_report(db: DB, args: argparse.Namespace) -> int:
    svc = QuestService(db)
    if args.status:
        try:
            statuses = [RequestStatus(s) for s in args.status]
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    else:
        statuses = list(_REPORT_DEFAULT_STATUSES)

    all_reqs: list[PaperRequest] = []
    seen: set[Any] = set()
    for s in statuses:
        f: dict[str, Any] = {"status": s.value, "limit": 500}
        if args.created_by:
            f["created_by"] = args.created_by
        if args.document:
            f["source_document"] = args.document
        out = await svc.status(filter=f)
        assert isinstance(out, list)
        for r in out:
            if r.id not in seen:
                seen.add(r.id)
                all_reqs.append(r)

    all_reqs.sort(key=lambda r: (_REPORT_STATUS_ORDER.get(r.status, 99), r.created_at))

    if args.format == "json":
        print(json.dumps([r.to_dict() for r in all_reqs], indent=2, default=str))
    else:
        print(render_report(all_reqs, title=args.title))
    return 0


async def _cmd_submit_file(db: DB, args: argparse.Namespace) -> int:
    svc = QuestService(db)
    content: bytes | None = None
    url: str | None = None
    if args.path:
        from pathlib import Path as _Path

        content = _Path(args.path).read_bytes()
    else:
        url = args.url

    ref: dict[str, Any] | None = None
    if not args.request_id:
        ref = {
            "doi": args.sf_doi,
            "arxiv": args.sf_arxiv,
            "title": args.sf_title,
        }
        # Drop empty keys so service sees a clean ref.
        ref = {k: v for k, v in ref.items() if v}
        if not ref:
            print(
                "error: submit-file needs --request-id or one of --doi / --arxiv / --title",
                file=sys.stderr,
            )
            return 2

    try:
        req = await svc.submit_file(
            url=url,
            content=content,
            filename=args.filename or (args.path.split("/")[-1] if args.path else None),
            request_id=args.request_id,
            ref=ref,
            created_by=args.created_by,
        )
    except NotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except RateLimitError as exc:
        print(f"rate limit: {exc}", file=sys.stderr)
        return 3
    print(json.dumps(req.to_dict(), indent=2, default=str))
    return 0


async def _cmd_update(db: DB, args: argparse.Namespace) -> int:
    svc = QuestService(db)
    kwargs: dict[str, Any] = {}
    for k in ("choice", "doi", "code", "severity", "evidence", "priority"):
        v = getattr(args, k)
        if v is not None:
            kwargs[k] = v
    try:
        req = await svc.update(args.id, args.mode, **kwargs)
    except NotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(req.to_dict(), indent=2, default=str))
    return 0


def _looks_like_arxiv(s: str) -> bool:
    import re

    return bool(re.fullmatch(r"\d{4}\.\d{4,5}(v\d+)?", s))


def main() -> None:
    sys.exit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
