"""
Ashby discovery via the public Posting API: https://api.ashbyhq.com/posting-api/job-board/<board>
"""
from __future__ import annotations

from typing import Any

import requests

from job_pipeline.sources.common import HEADERS, collect_from_registry as _from_registry, iso_to_dt, poll_boards

API = "https://api.ashbyhq.com/posting-api/job-board"


def fetch(session: requests.Session, token: str, timeout: float) -> tuple[str, list[dict]]:
    res = session.get(f"{API}/{token}", headers=HEADERS, timeout=timeout)
    if res.status_code == 404:
        return "not_found", []
    if res.status_code == 429:
        return "rate_limited", []
    res.raise_for_status()
    return "ok", [j for j in res.json().get("jobs", []) if j.get("isListed", True)]


def to_row(token: str, j: dict, company: str | None) -> dict[str, Any]:
    location = j.get("location") or ""
    published = iso_to_dt(j.get("publishedAt"))
    return {
        "site": "ashby",
        "search_term": "ats:ashby",
        "title": j.get("title"),
        "company": company or token,
        "location": location,
        "job_url": j.get("jobUrl") or f"https://jobs.ashbyhq.com/{token}/{j.get('id')}",
        "job_url_direct": j.get("applyUrl") or f"https://jobs.ashbyhq.com/{token}/{j.get('id')}/application",
        "date_posted": published.date().isoformat() if published else None,
        "is_remote": bool(j.get("isRemote")) or "remote" in location.lower(),
        "description": j.get("descriptionPlain") or "",
    }


def collect_boards(boards: list[dict[str, Any]], **kwargs: Any):
    return poll_boards("ashby", boards, fetch, lambda j: iso_to_dt(j.get("publishedAt")), lambda j: j.get("title", ""), to_row, **kwargs)


def collect_from_registry(**kwargs: Any):
    return _from_registry("ashby", collect_boards, **kwargs)
