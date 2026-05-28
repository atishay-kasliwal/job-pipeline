from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import DESCENDING, MongoClient
from pymongo.collection import Collection

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


def _fetch_jobs_for_date(date: str) -> list[dict]:
    """Return deduplicated jobs for a given date, sorted by score desc."""
    pipeline = [
        {"$match": {"scraped_date": date}},
        {"$sort": {"score": DESCENDING}},
        {"$group": {"_id": "$job_url", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"score": DESCENDING}},
    ]
    return list(jobs_col().aggregate(pipeline))


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


class SwipeIn(BaseModel):
    job_url: str
    direction: str  # "right" | "left"
    date: Optional[str] = None


@app.post("/api/swipe")
def record_swipe(body: SwipeIn):
    if body.direction not in ("right", "left"):
        raise HTTPException(400, "direction must be 'right' or 'left'")
    today = body.date or _today_utc()
    swipes_col().update_one(
        {"job_url": body.job_url, "date": today},
        {"$set": {
            "job_url": body.job_url,
            "direction": body.direction,
            "date": today,
            "swiped_at": datetime.now(tz=timezone.utc),
        }},
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

    pipeline = [
        {"$match": {"job_url": {"$in": swiped_urls}, "scraped_date": today}},
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
            jobs.append(job)

    if direction == "right":
        jobs.sort(key=lambda j: j.get("score") or 0, reverse=True)

    return {"date": today, "direction": direction, "count": len(jobs), "jobs": jobs}
