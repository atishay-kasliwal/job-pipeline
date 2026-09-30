"""
Backfill job_url_direct (and ATS identity) for LinkedIn jobs stored before the
column existed, by reading LinkedIn's public job page the same way JobSpy does.
Polite: one request every --delay seconds, --limit per run.

    python -m job_pipeline.backfill_apply_urls --limit 50            # dry run
    python -m job_pipeline.backfill_apply_urls --limit 50 --write
"""
from __future__ import annotations

import argparse
import re
import time
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from job_pipeline.ats_identity import IDENTITY_COLUMNS, parse_posting_identity
from job_pipeline.storage import get_db

_ID = re.compile(r"/jobs/view/(?:[^/?#]*-)?(\d+)")
_REDIRECT = re.compile(r'"(https?://[^"]+)"')
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129 Safari/537.36"}


def apply_url_from_html(html: str) -> str | None:
    """Same extraction as JobSpy: <code id="applyUrl"> holds a LinkedIn redirect with ?url=<external>."""
    code = BeautifulSoup(html, "html.parser").find("code", id="applyUrl")
    if not code:
        return None
    m = _REDIRECT.search(code.decode_contents())
    if not m:
        return None
    # Read the ?url= parameter properly so LinkedIn's own params (urlHash, trk) never leak in.
    return (parse_qs(urlparse(m.group(1)).query).get("url") or [None])[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    jobs = get_db()["jobs"]
    urls = [u for u in jobs.distinct("job_url", {"site": "linkedin", "job_url_direct": {"$exists": False}}) if _ID.search(u or "")][: args.limit]
    session = requests.Session()
    found = 0
    for url in urls:
        job_id = _ID.search(url).group(1)
        try:
            res = session.get(f"https://www.linkedin.com/jobs/view/{job_id}", headers=HEADERS, timeout=10)
            direct = apply_url_from_html(res.text) if res.ok and "linkedin.com/signup" not in res.url else None
        except requests.RequestException:
            direct = None
        ident = parse_posting_identity(direct) or {}
        print(f"{'✓' if direct else '·'} {url} → {direct or 'no external link (Easy Apply or unavailable)'}")
        if direct:
            found += 1
        if args.write:
            update = {"job_url_direct": direct, **{c: ident.get(c) for c in IDENTITY_COLUMNS}}
            jobs.update_many({"job_url": url}, {"$set": update})
        time.sleep(args.delay)
    print(f"{found}/{len(urls)} had an external apply link{'' if args.write else ' (dry run — add --write)'}")


if __name__ == "__main__":
    main()
