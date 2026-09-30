"""
Greenhouse discovery via the public Job Board API (no auth).

Per board: list open jobs (light), keep only jobs first published since the
board's last poll, apply the existing role filter to titles, then fetch details
(description, company) for the survivors only. Rows are JobSpy-shaped with
``site="greenhouse"`` and the hosted posting URL as ``job_url``.

Dry run (read-only, no Mongo):
    python -m job_pipeline.sources.greenhouse --boards discord,figma --hours 72
"""
from __future__ import annotations

import argparse
import html
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests

from job_pipeline.filters import filter_by_role

logger = logging.getLogger(__name__)

API = "https://boards-api.greenhouse.io/v1/boards"
HEADERS = {"User-Agent": "atriveo-job-pipeline (personal job search)", "Accept": "application/json"}
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"[ \t]+")


def html_to_text(content: str | None) -> str:
    """Greenhouse `content` is HTML-escaped HTML: unescape, strip tags, unescape entities."""
    text = html.unescape(str(content or ""))
    text = re.sub(r"</(p|div|li|h\d|br)\s*>|<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = html.unescape(_TAG.sub(" ", text))
    lines = [_WS.sub(" ", ln).strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


def _parse_time(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None
    except ValueError:
        return None


def list_jobs(session: requests.Session, token: str, timeout: float) -> tuple[str, list[dict]]:
    res = session.get(f"{API}/{token}/jobs", headers=HEADERS, timeout=timeout)
    if res.status_code == 404:
        return "not_found", []
    if res.status_code == 429:
        return "rate_limited", []
    res.raise_for_status()
    return "ok", res.json().get("jobs", [])


def job_detail(session: requests.Session, token: str, job_id: int, timeout: float) -> dict | None:
    """The posting's detail (description). A slow or failed request only costs this posting its description."""
    try:
        res = session.get(f"{API}/{token}/jobs/{job_id}", headers=HEADERS, timeout=timeout)
        return res.json() if res.ok else None
    except (requests.RequestException, ValueError) as exc:
        logger.info("greenhouse %s/%s detail: %s", token, job_id, exc)
        return None


def to_row(token: str, job: dict, detail: dict | None, company: str | None) -> dict[str, Any]:
    url = f"https://job-boards.greenhouse.io/{token}/jobs/{job['id']}"
    location = (job.get("location") or {}).get("name") or ""
    published = _parse_time(job.get("first_published")) or _parse_time(job.get("updated_at"))
    return {
        "site": "greenhouse",
        "search_term": "ats:greenhouse",
        "title": job.get("title"),
        "company": (detail or {}).get("company_name") or job.get("company_name") or company or token,
        "location": location,
        "job_url": url,
        "job_url_direct": url,
        "date_posted": published.date().isoformat() if published else None,
        "is_remote": "remote" in location.lower(),
        "description": html_to_text((detail or {}).get("content")),
    }


def collect_boards(
    boards: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    first_poll_hours: int = 24,
    workers: int = 8,
    timeout_s: float = 15,
    session: requests.Session | None = None,
    on_polled=None,
) -> pd.DataFrame:
    """
    ``boards``: dicts with ``token`` and optional ``company_name`` / ``last_polled_at``.
    Emits jobs first published after the board's last poll (or within
    ``first_poll_hours`` on a first poll), so rotating polls never miss a posting.
    """
    now = now or datetime.now(tz=timezone.utc)
    session = session or requests.Session()

    def one(board: dict[str, Any]) -> list[dict[str, Any]]:
        token = board["token"]
        last = board.get("last_polled_at")
        if isinstance(last, datetime) and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        cutoff = last if isinstance(last, datetime) else now - timedelta(hours=first_poll_hours)
        try:
            status, jobs = list_jobs(session, token, timeout_s)
        except requests.RequestException as exc:
            logger.info("greenhouse %s: %s", token, exc)
            if on_polled:
                on_polled(token, "error", 0, 0)
            return []
        fresh = [j for j in jobs if (_parse_time(j.get("first_published")) or _parse_time(j.get("updated_at")) or now) > cutoff]
        titled = filter_by_role(pd.DataFrame([{"title": j.get("title", "")} for j in fresh])) if fresh else pd.DataFrame()
        keep = [fresh[i] for i in titled.index] if not titled.empty else []
        rows = [to_row(token, j, job_detail(session, token, j["id"], timeout_s), board.get("company_name")) for j in keep]
        if on_polled:
            on_polled(token, status, len(jobs), len(rows))
        return rows

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        rows = [r for batch in pool.map(one, boards) for r in batch]
    logger.info("Greenhouse: %d boards polled → %d new matching postings", len(boards), len(rows))
    return pd.DataFrame(rows)


def collect_from_registry(max_boards_per_run: int = 300, **kwargs: Any) -> pd.DataFrame:
    """Hourly entry point: poll the next boards from the ats_boards registry and record each poll."""
    from job_pipeline import ats_boards

    now = datetime.now(tz=timezone.utc)
    boards = ats_boards.boards_for_run("greenhouse", max_boards_per_run, now)
    if not boards:
        logger.info("Greenhouse: registry empty — run python -m job_pipeline.sources.discover_boards --write")
        return pd.DataFrame()
    return collect_boards(
        boards, now=now,
        on_polled=lambda token, status, open_jobs, matched: ats_boards.record_poll("greenhouse", token, status, open_jobs, matched, now),
        **kwargs,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Greenhouse source dry run (read-only)")
    parser.add_argument("--boards", required=True, help="comma-separated board tokens")
    parser.add_argument("--hours", type=int, default=24, help="look-back window")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    df = collect_boards([{"token": t.strip()} for t in args.boards.split(",") if t.strip()], first_poll_hours=args.hours)
    print(f"{len(df)} matching postings")
    if not df.empty:
        print(df[["company", "title", "location", "date_posted", "job_url"]].to_string(index=False, max_colwidth=60))


if __name__ == "__main__":
    main()
