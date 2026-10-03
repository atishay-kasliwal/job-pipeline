"""
Backfill application priority (job_pipeline/priority.py) on jobs stored before it existed, or under an
older priority version. Reads only score_pct, location, title and min_exp; writes priority_group,
priority_tags and priority_version. Nothing is deleted or re-scored.

    python -m job_pipeline.backfill_priority            # dry run: counts and a sample
    python -m job_pipeline.backfill_priority --write
"""
from __future__ import annotations

import argparse
from collections import Counter

from pymongo import UpdateOne

from job_pipeline.priority import PRIORITY_VERSION, job_priority
from job_pipeline.storage import get_db

BATCH = 500


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    jobs = get_db()["jobs"]
    stale = {"priority_version": {"$ne": PRIORITY_VERSION}}
    cursor = jobs.find(stale, {"score_pct": 1, "location": 1, "title": 1, "min_exp": 1})
    groups: Counter[int] = Counter()
    ops: list[UpdateOne] = []
    seen = written = 0
    for doc in cursor:
        p = job_priority(doc.get("score_pct"), doc.get("location"), doc.get("title"), doc.get("min_exp"))
        groups[p["priority_group"]] += 1
        seen += 1
        if seen <= 5:
            print(f"  {doc.get('title', '')[:50]!r:52} {doc.get('location', '')!s:28.28} → group {p['priority_group']:2} {p['priority_tags']}")
        if args.write:
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": p}))
            if len(ops) >= BATCH:
                written += jobs.bulk_write(ops, ordered=False).modified_count
                ops = []
    if args.write and ops:
        written += jobs.bulk_write(ops, ordered=False).modified_count
    print(f"{seen} job(s) without priority v{PRIORITY_VERSION}; by group: {dict(sorted(groups.items()))}")
    print(f"{written} updated" if args.write else "dry run — add --write")


if __name__ == "__main__":
    main()
