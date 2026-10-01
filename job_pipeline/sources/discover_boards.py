"""
Find ATS boards (Greenhouse, Lever, Ashby) to poll — not limited to any company list.

Sources:
  --commoncrawl N   board tokens from Common Crawl's URL index (N index pages)
  --from-jobs       tokens from Greenhouse links already seen in Mongo jobs
  --tokens a,b      explicit tokens
Every token is validated against the Job Board API before use.

    python -m job_pipeline.sources.discover_boards --commoncrawl 2                       # dry run
    python -m job_pipeline.sources.discover_boards --ats lever --commoncrawl 2 --write   # save to ats_boards
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import requests

from job_pipeline.ats_identity import parse_posting_identity
from job_pipeline.sources.greenhouse import API, HEADERS

logger = logging.getLogger(__name__)
CC_INDEX = "https://index.commoncrawl.org"
_TOKEN = re.compile(r"^[a-z0-9][a-z0-9_-]{1,60}$")
_NOT_BOARDS = {"embed", "api", "v1", "jobs", "job_app", "static", "assets", "favicon.ico", "robots.txt"}


HOSTS = {
    "greenhouse": ("job-boards.greenhouse.io", "boards.greenhouse.io"),
    "lever": ("jobs.lever.co",),
    "ashby": ("jobs.ashbyhq.com",),
}
VALIDATE_URL = {
    "greenhouse": API + "/{token}",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json&limit=1",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
}


def token_from_url(url: str, ats: str = "greenhouse") -> str | None:
    """https://job-boards.greenhouse.io/<token>/jobs/… (or jobs.lever.co/<token>/…) → token."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    if (parsed.hostname or "").lower() not in HOSTS[ats]:
        return None
    first = next((p for p in parsed.path.split("/") if p), "").lower()
    return first if _TOKEN.match(first) and first not in _NOT_BOARDS else None


def _cc_get(session: requests.Session, url: str, params: dict, tries: int = 4) -> requests.Response | None:
    """The index often answers 503/504 under load; retry with backoff instead of reading that as "no boards"."""
    for attempt in range(tries):
        try:
            res = session.get(url, params=params, timeout=120)
            if res.status_code == 200:
                return res
            logger.info("Common Crawl %s → %s (attempt %d)", params.get("url"), res.status_code, attempt + 1)
        except requests.RequestException as exc:  # the index is large and occasionally drops connections
            logger.info("Common Crawl %s: %s (attempt %d)", params.get("url"), exc, attempt + 1)
        time.sleep(5 * 2 ** attempt)
    return None


def from_commoncrawl(pages: int, session: requests.Session, ats: str = "greenhouse", snapshots: int = 4) -> set[str]:
    """Board tokens from the last `snapshots` crawls (each crawl sees different pages), up to `pages` index pages per host."""
    ids = [c["id"] for c in session.get(f"{CC_INDEX}/collinfo.json", timeout=30).json()[:max(1, snapshots)]]
    tokens: set[str] = set()
    for cc_id in ids:
        for host in HOSTS[ats]:
            url = f"{CC_INDEX}/{cc_id}-index"
            info = _cc_get(session, url, {"url": f"{host}/*", "output": "json", "showNumPages": "true"})
            total = info.json().get("pages", pages) if info is not None else pages
            for page in range(min(pages, total)):
                res = _cc_get(session, url, {"url": f"{host}/*", "output": "json", "fl": "url", "page": page})
                if res is None:
                    break
                for line in res.text.splitlines():
                    try:
                        token = token_from_url(json.loads(line)["url"], ats)
                    except (ValueError, KeyError):
                        continue
                    if token:
                        tokens.add(token)
        logger.info("Common Crawl %s: %d %s tokens so far", cc_id, len(tokens), ats)
    return tokens


def from_jobs(ats: str = "greenhouse") -> set[str]:
    from job_pipeline.storage import get_db
    tokens: set[str] = set()
    for url in get_db()["jobs"].distinct("job_url_direct", {"job_url_direct": {"$regex": "|".join(h.replace(".", "\\.") for h in HOSTS[ats])}}):
        ident = parse_posting_identity(url)
        if ident and ident.get("ats") == ats and ident.get("ats_board"):
            tokens.add(ident["ats_board"])
    return tokens


def validate(tokens: set[str], session: requests.Session, workers: int = 8, ats: str = "greenhouse") -> dict[str, str]:
    """token → company name, for boards the ATS's public API recognizes."""
    def one(token: str) -> tuple[str, str | None]:
        try:
            res = session.get(VALIDATE_URL[ats].format(token=token), headers=HEADERS, timeout=15)
            if not res.ok:
                return token, None
            body = res.json()
            if ats == "greenhouse":
                return token, body.get("name") or token
            if ats == "lever" and not isinstance(body, list):
                return token, None
            return token, token.replace("-", " ").title()
        except requests.RequestException:
            return token, None
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return {t: name for t, name in pool.map(one, sorted(tokens)) if name}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ats", choices=sorted(HOSTS), default="greenhouse")
    parser.add_argument("--commoncrawl", type=int, default=0, metavar="PAGES")
    parser.add_argument("--snapshots", type=int, default=4, help="Common Crawl snapshots to read (default 4)")
    parser.add_argument("--from-jobs", action="store_true")
    parser.add_argument("--tokens", default="")
    parser.add_argument("--write", action="store_true", help="save to Mongo ats_boards (default: dry run)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    session = requests.Session()
    found: dict[str, set[str]] = {}
    if args.commoncrawl:
        found["commoncrawl"] = from_commoncrawl(args.commoncrawl, session, args.ats, snapshots=args.snapshots)
    if args.from_jobs:
        found["linkedin_apply_url"] = from_jobs(args.ats)
    if args.tokens:
        found["manual"] = {t.strip().lower() for t in args.tokens.split(",") if t.strip()}
    all_tokens = set().union(*found.values()) if found else set()
    from job_pipeline.ats_boards import _col
    known = {d["token"] for d in _col().find({"ats": args.ats}, {"token": 1})}
    print(f"{len(all_tokens)} candidate tokens, {len(all_tokens - known)} not registered yet; validating those…")
    all_tokens -= known
    valid = validate(all_tokens, session, ats=args.ats)
    print(f"{len(valid)} new valid {args.ats} boards")
    if not args.write:
        for token, name in list(valid.items())[:25]:
            print(f"  {token:<28} {name}")
        print("(dry run — add --write to save)")
        return
    from job_pipeline import ats_boards
    for via, tokens in found.items():
        for token in tokens:
            if token in valid:
                ats_boards.upsert_discovered(args.ats, token, valid[token], via)
    print(f"Saved {len(valid)} new boards to ats_boards.")


if __name__ == "__main__":
    main()
