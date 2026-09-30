"""
Find Greenhouse boards to poll — not limited to any company list.

Sources:
  --commoncrawl N   board tokens from Common Crawl's URL index (N index pages)
  --from-jobs       tokens from Greenhouse links already seen in Mongo jobs
  --tokens a,b      explicit tokens
Every token is validated against the Job Board API before use.

    python -m job_pipeline.sources.discover_boards --commoncrawl 2            # dry run
    python -m job_pipeline.sources.discover_boards --commoncrawl 2 --write    # save to ats_boards
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import requests

from job_pipeline.ats_identity import parse_posting_identity
from job_pipeline.sources.greenhouse import API, HEADERS

logger = logging.getLogger(__name__)
CC_INDEX = "https://index.commoncrawl.org"
_TOKEN = re.compile(r"^[a-z0-9][a-z0-9_-]{1,60}$")
_NOT_BOARDS = {"embed", "api", "v1", "jobs", "job_app", "static", "assets", "favicon.ico", "robots.txt"}


def token_from_url(url: str) -> str | None:
    """https://job-boards.greenhouse.io/<token>/jobs/… → token."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    if not (parsed.hostname or "").endswith("greenhouse.io"):
        return None
    first = next((p for p in parsed.path.split("/") if p), "").lower()
    return first if _TOKEN.match(first) and first not in _NOT_BOARDS else None


def from_commoncrawl(pages: int, session: requests.Session) -> set[str]:
    latest = session.get(f"{CC_INDEX}/collinfo.json", timeout=30).json()[0]["id"]
    tokens: set[str] = set()
    for host in ("job-boards.greenhouse.io", "boards.greenhouse.io"):
        for page in range(pages):
            res = session.get(f"{CC_INDEX}/{latest}-index", params={"url": f"{host}/*", "output": "json", "fl": "url", "page": page}, timeout=120)
            if res.status_code != 200:
                break
            for line in res.text.splitlines():
                try:
                    token = token_from_url(json.loads(line)["url"])
                except (ValueError, KeyError):
                    continue
                if token:
                    tokens.add(token)
    return tokens


def from_jobs() -> set[str]:
    from job_pipeline.storage import get_db
    tokens: set[str] = set()
    for url in get_db()["jobs"].distinct("job_url_direct", {"job_url_direct": {"$regex": "greenhouse\\.io"}}):
        ident = parse_posting_identity(url)
        if ident and ident.get("ats_board"):
            tokens.add(ident["ats_board"])
    return tokens


def validate(tokens: set[str], session: requests.Session, workers: int = 8) -> dict[str, str]:
    """token → company name, for boards the Job Board API recognizes."""
    def one(token: str) -> tuple[str, str | None]:
        try:
            res = session.get(f"{API}/{token}", headers=HEADERS, timeout=15)
            return token, (res.json().get("name") or token) if res.ok else None
        except requests.RequestException:
            return token, None
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return {t: name for t, name in pool.map(one, sorted(tokens)) if name}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commoncrawl", type=int, default=0, metavar="PAGES")
    parser.add_argument("--from-jobs", action="store_true")
    parser.add_argument("--tokens", default="")
    parser.add_argument("--write", action="store_true", help="save to Mongo ats_boards (default: dry run)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    session = requests.Session()
    found: dict[str, set[str]] = {}
    if args.commoncrawl:
        found["commoncrawl"] = from_commoncrawl(args.commoncrawl, session)
    if args.from_jobs:
        found["linkedin_apply_url"] = from_jobs()
    if args.tokens:
        found["manual"] = {t.strip().lower() for t in args.tokens.split(",") if t.strip()}
    all_tokens = set().union(*found.values()) if found else set()
    print(f"{len(all_tokens)} candidate tokens; validating…")
    valid = validate(all_tokens, session)
    print(f"{len(valid)} valid Greenhouse boards")
    if not args.write:
        for token, name in list(valid.items())[:25]:
            print(f"  {token:<28} {name}")
        print("(dry run — add --write to save)")
        return
    from job_pipeline import ats_boards
    for via, tokens in found.items():
        for token in tokens:
            if token in valid:
                ats_boards.upsert_discovered("greenhouse", token, valid[token], via)
    print(f"Saved {len(valid)} boards to ats_boards.")


if __name__ == "__main__":
    main()
