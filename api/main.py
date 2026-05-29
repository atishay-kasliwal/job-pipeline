from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MONGO_URI: str = os.environ["MONGO_URI"]
DB_NAME = "job_pipeline"

_client: MongoClient | None = None


def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    return _client[DB_NAME]


def jobs_col() -> Collection:
    return get_db()["jobs"]


def swipes_col() -> Collection:
    return get_db()["job_swipes"]


app = FastAPI(title="Atriveo Swipe API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _serialize(doc: dict) -> dict:
    out = {}
    for k, v in doc.items():
        if k == "_id":
            continue
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


_DASHBOARD_TZ = ZoneInfo(os.getenv("DASHBOARD_TZ", "America/New_York"))


def _date_to_utc_range(date: str):
    """Convert a YYYY-MM-DD local date to a [start, end) UTC datetime pair."""
    from datetime import timedelta
    day = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=_DASHBOARD_TZ)
    return day.astimezone(timezone.utc), (day + timedelta(days=1)).astimezone(timezone.utc)


def _fetch_jobs_for_date(date: str) -> list[dict]:
    """Return deduplicated jobs for a given local date, sorted by score desc."""
    db = get_db()
    start_utc, end_utc = _date_to_utc_range(date)
    sessions = list(db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": start_utc, "$lt": end_utc}},
        {"session_id": 1},
    ))
    sids = [s["session_id"] for s in sessions]
    if not sids:
        return []
    pipeline = [
        {"$match": {"session_id": {"$in": sids}}},
        {"$sort": {"score": DESCENDING}},
        {"$group": {"_id": "$job_url", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"score": DESCENDING}},
    ]
    return list(db["jobs"].aggregate(pipeline))


def _swiped_urls(date: str) -> set[str]:
    return {
        s["job_url"]
        for s in swipes_col().find({"date": date}, {"job_url": 1, "_id": 0})
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/swipe-queue")
def get_swipe_queue(date: Optional[str] = None):
    """Return today's unswiped jobs, sorted by score desc."""
    today = date or _today_utc()
    all_jobs = _fetch_jobs_for_date(today)
    swiped = _swiped_urls(today)
    queue = [
        _serialize(j) for j in all_jobs
        if j.get("job_url") and j["job_url"] not in swiped
    ]
    return {"date": today, "count": len(queue), "jobs": queue}


@app.get("/api/job-description")
def get_job_description(url: str):
    """Return the full description for a single job by URL."""
    doc = get_db()["descriptions"].find_one(
        {"job_url": url},
        {"_id": 0, "description": 1},
    )
    return {"job_url": url, "description": (doc or {}).get("description")}


class SwipeIn(BaseModel):
    job_url: str
    direction: str  # "right" | "left"
    date: Optional[str] = None


@app.post("/api/swipe")
def record_swipe(body: SwipeIn):
    if body.direction not in ("right", "left"):
        raise HTTPException(400, "direction must be 'right' or 'left'")
    today = body.date or _today_utc()
    update: dict = {
        "$set": {
            "job_url": body.job_url,
            "direction": body.direction,
            "date": today,
            "swiped_at": datetime.now(tz=timezone.utc),
        }
    }
    # First-time right-swipes initialize resume_created=False
    if body.direction == "right":
        update["$setOnInsert"] = {"resume_created": False}
    swipes_col().update_one(
        {"job_url": body.job_url, "date": today},
        update,
        upsert=True,
    )
    return {"ok": True}


@app.get("/api/swipes")
def get_swipes(direction: Optional[str] = None, date: Optional[str] = None):
    """Return swiped jobs for a date, optionally filtered by direction."""
    today = date or _today_utc()
    query: dict = {"date": today}
    if direction:
        query["direction"] = direction

    swipe_docs = list(swipes_col().find(query, {"_id": 0}))
    if not swipe_docs:
        return {"date": today, "direction": direction, "count": 0, "jobs": []}

    swiped_urls = [s["job_url"] for s in swipe_docs]
    direction_map = {s["job_url"]: s["direction"] for s in swipe_docs}
    swipe_time_map = {
        s["job_url"]: s["swiped_at"].isoformat() if isinstance(s.get("swiped_at"), datetime) else None
        for s in swipe_docs
    }

    start_utc, end_utc = _date_to_utc_range(today)
    sessions = list(get_db()["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": start_utc, "$lt": end_utc}},
        {"session_id": 1},
    ))
    sids = [s["session_id"] for s in sessions]
    pipeline = [
        {"$match": {"job_url": {"$in": swiped_urls}, "session_id": {"$in": sids} if sids else {"$exists": True}}},
        {"$sort": {"score": DESCENDING}},
        {"$group": {"_id": "$job_url", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    jobs_map = {j["job_url"]: _serialize(j) for j in jobs_col().aggregate(pipeline)}

    jobs = []
    for url in swiped_urls:
        if url in jobs_map:
            job = jobs_map[url]
            job["swipe_direction"] = direction_map[url]
            job["swiped_at"]       = swipe_time_map.get(url)
            jobs.append(job)

    if direction == "right":
        jobs.sort(key=lambda j: j.get("score") or 0, reverse=True)

    return {"date": today, "direction": direction, "count": len(jobs), "jobs": jobs}


@app.post("/api/picks/today")
def update_picks_today(date: Optional[str] = None):
    """
    Backfill `resume_created: False` on any of today's right-swiped picks
    that don't have the field yet, then return the picks with the full job
    document AND full description, sorted by score desc.
    Each pick includes `swiped_at` and `resume_created`.
    """
    today = date or _today_utc()
    db = get_db()

    # 0. Ensure every right-swipe for the date has resume_created (default False).
    #    $exists:False match means existing values (e.g. True after a resume is
    #    generated) are preserved — this only initializes missing ones.
    swipes_col().update_many(
        {"date": today, "direction": "right", "resume_created": {"$exists": False}},
        {"$set": {"resume_created": False}},
    )

    # 1. Today's right-swipes
    swipe_docs = list(swipes_col().find(
        {"date": today, "direction": "right"},
        {"_id": 0, "job_url": 1, "swiped_at": 1, "resume_created": 1},
    ))
    if not swipe_docs:
        return {"date": today, "count": 0, "picks": []}

    swiped_urls = [s["job_url"] for s in swipe_docs]
    swipe_time_map = {
        s["job_url"]: s["swiped_at"].isoformat() if isinstance(s.get("swiped_at"), datetime) else None
        for s in swipe_docs
    }
    resume_created_map = {s["job_url"]: bool(s.get("resume_created", False)) for s in swipe_docs}

    # 2. Job documents for those URLs (scoped to today's sessions, like the queue)
    start_utc, end_utc = _date_to_utc_range(today)
    sessions = list(db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": start_utc, "$lt": end_utc}},
        {"session_id": 1},
    ))
    sids = [s["session_id"] for s in sessions]

    match_stage: dict = {"job_url": {"$in": swiped_urls}}
    if sids:
        match_stage["session_id"] = {"$in": sids}
    job_pipeline = [
        {"$match": match_stage},
        {"$sort": {"score": DESCENDING}},
        {"$group": {"_id": "$job_url", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    jobs_map = {j["job_url"]: _serialize(j) for j in jobs_col().aggregate(job_pipeline)}

    # 3. Full descriptions for those URLs
    desc_docs = db["descriptions"].find(
        {"job_url": {"$in": swiped_urls}},
        {"_id": 0, "job_url": 1, "description": 1},
    )
    desc_map = {d["job_url"]: d.get("description") for d in desc_docs}

    # 4. Join everything in swipe-order, then sort by score desc
    picks: list[dict] = []
    for url in swiped_urls:
        job = jobs_map.get(url)
        if not job:
            # job no longer in today's sessions — still include a stub
            job = {"job_url": url}
        picks.append({
            **job,
            "description":     desc_map.get(url),
            "swiped_at":       swipe_time_map.get(url),
            "resume_created":  resume_created_map.get(url, False),
        })

    picks.sort(key=lambda p: p.get("score") or 0, reverse=True)
    return {"date": today, "count": len(picks), "picks": picks}
