"""
Registry of known ATS job boards (Mongo collection ``ats_boards``).

One document per board: ``{_id: "greenhouse:<token>", ats, token, company_name,
discovered_via[], first_seen_at, last_polled_at, last_status, open_jobs,
last_matched_at}``. Filled by ``python -m job_pipeline.sources.discover_boards``.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from job_pipeline.storage import get_db

COLLECTION = "ats_boards"


def _col():
    return get_db()[COLLECTION]


def upsert_discovered(ats: str, token: str, company_name: str | None, via: str, now: datetime | None = None) -> None:
    now = now or datetime.now(tz=timezone.utc)
    _col().update_one(
        {"_id": f"{ats}:{token}"},
        {
            "$setOnInsert": {"ats": ats, "token": token, "first_seen_at": now, "last_polled_at": None, "status": "active"},
            "$set": {"company_name": company_name} if company_name else {},
            "$addToSet": {"discovered_via": via},
        } if company_name else {
            "$setOnInsert": {"ats": ats, "token": token, "first_seen_at": now, "last_polled_at": None, "status": "active"},
            "$addToSet": {"discovered_via": via},
        },
        upsert=True,
    )


def boards_for_run(ats: str, limit: int, now: datetime | None = None) -> list[dict[str, Any]]:
    """Boards with a match in the last 30 days first (up to half the budget), then least recently polled."""
    now = now or datetime.now(tz=timezone.utc)
    col = _col()
    recent = list(col.find(
        {"ats": ats, "status": "active", "last_matched_at": {"$gte": now - timedelta(days=30)}},
        sort=[("last_polled_at", 1)], limit=max(1, limit // 2),
    ))
    seen = {b["_id"] for b in recent}
    rest = list(col.find(
        {"ats": ats, "status": "active", "_id": {"$nin": list(seen)}},
        sort=[("last_polled_at", 1)], limit=max(0, limit - len(recent)),
    ))
    return recent + rest


def record_poll(ats: str, token: str, status: str, open_jobs: int, matched: int, now: datetime | None = None) -> None:
    now = now or datetime.now(tz=timezone.utc)
    update: dict[str, Any] = {"last_polled_at": now, "last_status": status, "open_jobs": open_jobs}
    if matched:
        update["last_matched_at"] = now
    if status == "not_found":
        update["status"] = "gone"
    _col().update_one({"_id": f"{ats}:{token}"}, {"$set": update})
