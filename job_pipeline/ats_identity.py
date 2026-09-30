"""
Posting identity from ATS URLs.

Mirrors the application engine's parser (playatriveo
src/application/identity/applicationKey.ts): the same posting reached through
LinkedIn or through the ATS itself gets the same ``application_key``.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
IDENTITY_COLUMNS = ["ats", "ats_board", "ats_posting_id", "application_key"]


def _identity(ats: str, board: str | None, posting_id: str, key: str) -> dict[str, Any]:
    return {"ats": ats, "ats_board": board, "ats_posting_id": posting_id, "application_key": key}


def parse_posting_identity(url: Any) -> dict[str, Any] | None:
    raw = str(url or "").strip()
    if not raw.lower().startswith(("http://", "https://")):
        return None
    try:
        parsed = urlparse(raw)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower()
    parts = [unquote(p) for p in parsed.path.split("/") if p]
    query = parse_qs(parsed.query)
    first = lambda k: (query.get(k) or [None])[0]  # noqa: E731

    gh_jid = first("gh_jid")
    if host == "greenhouse.io" or host.endswith(".greenhouse.io"):
        if parts and parts[0] == "embed":
            token = first("token") or gh_jid
            if token and token.isdigit():
                return _identity("greenhouse", first("for"), token, f"greenhouse:{token}")
            return None
        if "jobs" in parts:
            i = parts.index("jobs")
            job_id = parts[i + 1] if i + 1 < len(parts) else ""
            if job_id.isdigit():
                return _identity("greenhouse", parts[0].lower() if i > 0 else None, job_id, f"greenhouse:{job_id}")
        return None
    if gh_jid and gh_jid.isdigit():
        return _identity("greenhouse", None, gh_jid, f"greenhouse:{gh_jid}")

    if host in ("jobs.lever.co", "jobs.eu.lever.co") and len(parts) >= 2 and _UUID.search(parts[1]):
        pid = _UUID.search(parts[1]).group(0).lower()
        return _identity("lever", parts[0].lower(), pid, f"lever:{pid}")

    if host == "jobs.ashbyhq.com" and len(parts) >= 2 and _UUID.search(parts[1]):
        pid = _UUID.search(parts[1]).group(0).lower()
        return _identity("ashby", parts[0].lower(), pid, f"ashby:{pid}")
    ashby = first("ashby_jid")
    if ashby and _UUID.search(ashby):
        pid = _UUID.search(ashby).group(0).lower()
        return _identity("ashby", None, pid, f"ashby:{pid}")

    if re.search(r"\.(myworkdayjobs|myworkdaysite|myworkday)\.com$", host) and "job" in parts:
        slug = next((p for p in parts[parts.index("job") + 1:] if re.search(r"_[A-Za-z0-9-]+$", p)), None)
        if slug:
            req = re.search(r"_([A-Za-z0-9-]+)$", slug).group(1).upper()
            tenant = host.split(".")[0] if host.endswith(".myworkdayjobs.com") else (parts[1] if len(parts) > 1 else host)
            return _identity("workday", tenant.lower(), req, f"workday:{tenant.lower()}:{req}")

    if host in ("jobs.smartrecruiters.com", "careers.smartrecruiters.com") and len(parts) >= 2:
        m = re.match(r"^(\d{6,})", parts[1])
        if m:
            return _identity("smartrecruiters", parts[0].lower(), m.group(1), f"smartrecruiters:{m.group(1)}")
    return None


def add_ats_identity(df: pd.DataFrame) -> pd.DataFrame:
    """Add ats / ats_board / ats_posting_id / application_key from job_url or job_url_direct."""
    if df.empty:
        for col in IDENTITY_COLUMNS:
            df[col] = pd.Series(dtype="object")
        return df
    out = df.copy()

    def _row(row: pd.Series) -> pd.Series:
        ident = parse_posting_identity(row.get("job_url")) or parse_posting_identity(row.get("job_url_direct"))
        return pd.Series(ident or {c: None for c in IDENTITY_COLUMNS})

    out[IDENTITY_COLUMNS] = out.apply(_row, axis=1)[IDENTITY_COLUMNS]
    return out
