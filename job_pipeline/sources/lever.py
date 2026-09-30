"""
Lever discovery via the public Postings API: https://api.lever.co/v0/postings/<company>?mode=json
Each posting includes its plain-text description, so one request per board.
"""
from __future__ import annotations

from typing import Any

import requests

from job_pipeline.sources.common import HEADERS, collect_from_registry as _from_registry, ms_to_dt, poll_boards

API = "https://api.lever.co/v0/postings"


def fetch(session: requests.Session, token: str, timeout: float) -> tuple[str, list[dict]]:
    res = session.get(f"{API}/{token}", params={"mode": "json"}, headers=HEADERS, timeout=timeout)
    if res.status_code == 404:
        return "not_found", []
    if res.status_code == 429:
        return "rate_limited", []
    res.raise_for_status()
    data = res.json()
    return "ok", data if isinstance(data, list) else []


def to_row(token: str, p: dict, company: str | None) -> dict[str, Any]:
    location = (p.get("categories") or {}).get("location") or ""
    created = ms_to_dt(p.get("createdAt"))
    description = "\n".join(x for x in [p.get("descriptionPlain"), *[f"{l.get('text', '')}\n{l.get('content', '')}" for l in p.get("lists") or []], p.get("additionalPlain")] if x)
    return {
        "site": "lever",
        "search_term": "ats:lever",
        "title": p.get("text"),
        "company": company or token,
        "location": location,
        "job_url": p.get("hostedUrl") or f"https://jobs.lever.co/{token}/{p.get('id')}",
        "job_url_direct": p.get("applyUrl") or f"https://jobs.lever.co/{token}/{p.get('id')}/apply",
        "date_posted": created.date().isoformat() if created else None,
        "is_remote": (p.get("workplaceType") == "remote") or "remote" in location.lower(),
        "description": description,
    }


def collect_boards(boards: list[dict[str, Any]], **kwargs: Any):
    return poll_boards("lever", boards, fetch, lambda p: ms_to_dt(p.get("createdAt")), lambda p: p.get("text", ""), to_row, **kwargs)


def collect_from_registry(**kwargs: Any):
    return _from_registry("lever", collect_boards, **kwargs)
