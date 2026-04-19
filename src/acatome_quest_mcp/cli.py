"""Command-line interface — ``acatome-quest``.

Subcommands::

    acatome-quest submit <DOI|arxiv|--title ...>
    acatome-quest status [<id> | --filter status=queued]
    acatome-quest update <id> <mode> [args...]
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
from .models import UpdateMode
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

    u = sub.add_parser("update", help="Mutate a request")
    u.add_argument("id")
    u.add_argument("mode", choices=[m.value for m in UpdateMode])
    u.add_argument("--choice", type=int)
    u.add_argument("--doi")
    u.add_argument("--code")
    u.add_argument("--severity")
    u.add_argument("--evidence")
    u.add_argument("--priority", type=int)

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
    if isinstance(out, list):
        print(json.dumps([r.to_dict() for r in out], indent=2, default=str))
    else:
        print(json.dumps(out.to_dict(), indent=2, default=str))
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
