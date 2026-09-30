"""Shared polling loop for ATS board sources (Lever, Ashby)."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import pandas as pd
import requests

from job_pipeline.filters import filter_by_role

logger = logging.getLogger(__name__)
HEADERS = {"User-Agent": "atriveo-job-pipeline (personal job search)", "Accept": "application/json"}

# fetch(session, token, timeout) -> (status, postings); to_row(token, posting, company) -> row | None
Fetch = Callable[[requests.Session, str, float], "tuple[str, list[dict]]"]


def poll_boards(
    ats: str,
    boards: list[dict[str, Any]],
    fetch: Fetch,
    published: Callable[[dict], datetime | None],
    title_of: Callable[[dict], str],
    to_row: Callable[[str, dict, str | None], dict[str, Any]],
    *,
    now: datetime | None = None,
    first_poll_hours: int = 24,
    workers: int = 8,
    timeout_s: float = 15,
    session: requests.Session | None = None,
    on_polled=None,
) -> pd.DataFrame:
    """Postings first published since each board's last poll, titles filtered by the existing role filter."""
    now = now or datetime.now(tz=timezone.utc)
    session = session or requests.Session()

    def one(board: dict[str, Any]) -> list[dict[str, Any]]:
        token = board["token"]
        last = board.get("last_polled_at")
        if isinstance(last, datetime) and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        cutoff = last if isinstance(last, datetime) else now - timedelta(hours=first_poll_hours)
        try:
            status, postings = fetch(session, token, timeout_s)
        except (requests.RequestException, ValueError) as exc:
            logger.info("%s %s: %s", ats, token, exc)
            if on_polled:
                on_polled(token, "error", 0, 0)
            return []
        fresh = [p for p in postings if (published(p) or now) > cutoff]
        titled = filter_by_role(pd.DataFrame([{"title": title_of(p)} for p in fresh])) if fresh else pd.DataFrame()
        rows = [to_row(token, fresh[i], board.get("company_name")) for i in titled.index] if not titled.empty else []
        if on_polled:
            on_polled(token, status, len(postings), len(rows))
        return rows

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        rows = [r for batch in pool.map(one, boards) for r in batch]
    logger.info("%s: %d boards polled → %d new matching postings", ats, len(boards), len(rows))
    return pd.DataFrame(rows)


def collect_from_registry(ats: str, collect: Callable[..., pd.DataFrame], max_boards_per_run: int = 300, **kwargs: Any) -> pd.DataFrame:
    from job_pipeline import ats_boards

    now = datetime.now(tz=timezone.utc)
    boards = ats_boards.boards_for_run(ats, max_boards_per_run, now)
    if not boards:
        logger.info("%s: registry empty — run python -m job_pipeline.sources.discover_boards --ats %s --write", ats, ats)
        return pd.DataFrame()
    return collect(boards, now=now,
                   on_polled=lambda token, status, open_jobs, matched: ats_boards.record_poll(ats, token, status, open_jobs, matched, now),
                   **kwargs)


def ms_to_dt(ms: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def iso_to_dt(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None
    except ValueError:
        return None
