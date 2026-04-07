"""
Job identity helpers used for deduplication across runs.

Goal:
- Treat the same posting (including URL variants with tracking params) as one job.
- Keep distinct openings in different locations as separate entries.
"""
from __future__ import annotations

import re
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse, urlunparse

_WS_RE = re.compile(r"\s+")
_LINKEDIN_PATH_ID_RE = re.compile(r"/jobs/view/(?:[^/?#]*-)?(\d+)(?:/|$)", re.IGNORECASE)


def _norm_text(value: Any) -> str:
    """Lowercase + collapse whitespace for stable string comparison."""
    return _WS_RE.sub(" ", str(value or "").strip().lower())


def _norm_location(value: Any) -> str:
    """Normalize location text while ignoring punctuation differences."""
    s = _norm_text(value)
    s = re.sub(r"[^\w\s]", " ", s)
    return _WS_RE.sub(" ", s).strip()


def canonical_job_url(url: Any) -> str:
    """
    Return a stable URL identity.

    - LinkedIn: extract numeric job id when possible.
    - Other sites: strip query/fragment and trailing slash.
    """
    raw = str(url or "").strip()
    if not raw:
        return ""

    try:
        parsed = urlparse(raw)
    except Exception:
        return _norm_text(raw)

    host = parsed.netloc.lower()
    path = parsed.path or ""
    query = parse_qs(parsed.query)

    if "linkedin.com" in host:
        m = _LINKEDIN_PATH_ID_RE.search(path)
        if m:
            return f"linkedin:{m.group(1)}"
        for key in ("currentJobId", "currentjobid", "jobId", "jobid"):
            val = query.get(key, [None])[0]
            if val and str(val).isdigit():
                return f"linkedin:{val}"

    scheme = (parsed.scheme or "https").lower()
    clean_path = re.sub(r"/+$", "", path) or "/"
    return urlunparse((scheme, host, clean_path, "", "", ""))


def job_identity_key(job: Mapping[str, Any]) -> str:
    """
    Compute a dedup key for one job-like mapping/row.

    Preference:
    1) Canonical URL identity.
    2) Fallback fingerprint with location included (so different locations stay).
    """
    canon = canonical_job_url(job.get("job_url"))
    location = _norm_location(job.get("location"))
    if canon:
        return f"url:{canon}|loc:{location}"

    title = _norm_text(job.get("title"))
    company = _norm_text(job.get("company"))
    site = _norm_text(job.get("site"))
    posted = _norm_text(job.get("date_posted"))[:10]
    return f"f:{title}|{company}|{location}|{site}|{posted}"
