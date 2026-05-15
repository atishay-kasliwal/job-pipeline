"""
Deploy pipeline output to the GitHub Pages dashboard.

After each pipeline run this module pushes the three JSON files
(jobs.json, important_jobs.json, metadata.json) directly to
atriveo-airflow/atriveo-airflow.github.io via the GitHub Contents API.

No GitHub Actions runner needed — works from a local machine or OpenShift.

Authentication
--------------
Uses the ``gh`` CLI token automatically (no extra setup needed if you
are already logged in with ``gh auth login``).
Alternatively, set the ``GITHUB_TOKEN`` environment variable.

Usage
-----
Called automatically by the standard and important pipelines when
``deploy=True`` (the default).  Can also be run standalone:

    python -m job_pipeline.deploy
"""
from __future__ import annotations
import base64
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

DASHBOARD_REPO = "atriveo-airflow/atriveo-airflow.github.io"
GITHUB_API = "https://api.github.com"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DOCS_SOURCE_DIR = Path(__file__).resolve().parent.parent / "docs"


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_token() -> str:
    """
    Resolve a GitHub token from the environment or the ``gh`` CLI.

    Priority:
    1. ``GITHUB_TOKEN`` env var
    2. ``gh auth token`` (uses existing gh CLI session)
    """
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token

    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True, text=True, check=True,
        )
        token = result.stdout.strip()
        if token:
            return token
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    raise RuntimeError(
        "No GitHub token found. "
        "Run `gh auth login` or set the GITHUB_TOKEN environment variable."
    )


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


# ── GitHub Contents API helpers ───────────────────────────────────────────────

def _get_sha(path: str, hdrs: dict) -> str:
    """Return the current blob SHA for a file (required by the PUT endpoint)."""
    r = requests.get(
        f"{GITHUB_API}/repos/{DASHBOARD_REPO}/contents/{path}",
        headers=hdrs,
        timeout=15,
    )
    if r.status_code == 200:
        return r.json().get("sha", "")
    return ""


def _put_file(path: str, content: str, message: str, hdrs: dict) -> bool:
    """Create or update a single file in the dashboard repo."""
    sha = _get_sha(path, hdrs)
    payload: dict[str, Any] = {
        "message": message,
        "content": base64.b64encode(content.encode()).decode(),
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(
        f"{GITHUB_API}/repos/{DASHBOARD_REPO}/contents/{path}",
        headers=hdrs,
        json=payload,
        timeout=20,
    )

    if r.status_code in (200, 201):
        logger.info("  ✓  docs/%s", path.split("/")[-1])
        return True

    logger.error("  ✗  %s — %s %s", path, r.status_code, r.text[:200])
    return False


# ── Public API ────────────────────────────────────────────────────────────────

def _read_json(path: Path) -> list:
    """Read a JSON file as a list, returning [] if the file doesn't exist."""
    return json.loads(path.read_text()) if path.exists() else []


def _put_local_file(local_path: Path, remote_path: str, message: str, hdrs: dict) -> bool:
    """
    Push a local static file (e.g., dashboard HTML) if present.
    Missing files are skipped without failing deploy.
    """
    if not local_path.exists():
        logger.warning("Skipping missing local file: %s", local_path)
        return True
    return _put_file(remote_path, local_path.read_text(), message, hdrs)


def _push_run_snapshots(hdrs: dict, message: str, max_snapshots: int = 48) -> list[bool]:
    """
    Push per-run snapshot files from ``output/runs/`` to ``docs/runs/``.

    Only the newest ``max_snapshots`` files are pushed (oldest are kept locally
    but not re-deployed so the GitHub Pages repo stays bounded).
    """
    runs_dir = OUTPUT_DIR / "runs"
    if not runs_dir.exists():
        return []

    snapshots = sorted(runs_dir.glob("*.json"))[-max_snapshots:]
    results = []
    for snap in snapshots:
        results.append(
            _put_file(f"docs/runs/{snap.name}", snap.read_text(), message, hdrs)
        )
    return results


def deploy_output() -> bool:
    """
    Read the latest JSON files from ``output/`` and push them to the
    GitHub Pages dashboard repo.

    Returns:
        True if all files were updated successfully.
    """
    return _deploy(
        jobs        = _read_json(OUTPUT_DIR / "jobs.json"),
        important   = _read_json(OUTPUT_DIR / "important_jobs.json"),
        top500      = _read_json(OUTPUT_DIR / "top500_jobs.json"),
        h1b2026     = _read_json(OUTPUT_DIR / "h1b2026_jobs.json"),
        keywords    = _read_json(OUTPUT_DIR / "keywords_jobs.json"),
        run_history = _read_json(OUTPUT_DIR / "run_history.json"),
        today       = _read_json(OUTPUT_DIR / "today_jobs.json"),
        yesterday   = _read_json(OUTPUT_DIR / "yesterday_jobs.json"),
    )


def _deploy(
    jobs: list,
    important: list,
    top500: list | None = None,
    h1b2026: list | None = None,
    keywords: list | None = None,
    run_history: list | None = None,
    today: list | None = None,
    yesterday: list | None = None,
) -> bool:
    """Push all job data files to the dashboard repo."""
    logger.info("Deploying to https://atriveo-airflow.github.io/ …")

    hdrs = _headers()
    now  = datetime.now(tz=timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    message   = f"chore: update job data {timestamp} [skip ci]"

    metadata = {
        "last_updated":    now.isoformat(),
        "standard_count":  len(jobs),
        "important_count": len(important),
        "top500_count":    len(top500 or []),
        "h1b2026_count":   len(h1b2026 or []),
        "keywords_count":  len(keywords or []),
        "today_count":     len(today or []),
        "yesterday_count": len(yesterday or []),
    }

    def _enc(obj: list) -> str:
        return json.dumps(obj, indent=2, default=str)

    results = [
        _put_local_file(DOCS_SOURCE_DIR / "index.html",         "docs/index.html",         message, hdrs),
        _put_local_file(DOCS_SOURCE_DIR / "weekly.html",        "docs/weekly.html",        message, hdrs),
        _put_local_file(DOCS_SOURCE_DIR / "unclicked_100.html", "docs/unclicked_100.html", message, hdrs),
        _put_file("docs/jobs.json",           _enc(jobs),              message, hdrs),
        _put_file("docs/important_jobs.json", _enc(important),         message, hdrs),
        _put_file("docs/top500_jobs.json",    _enc(top500 or []),      message, hdrs),
        _put_file("docs/h1b2026_jobs.json",   _enc(h1b2026 or []),     message, hdrs),
        _put_file("docs/keywords_jobs.json",  _enc(keywords or []),    message, hdrs),
        _put_file("docs/run_history.json",    _enc(run_history or []), message, hdrs),
        _put_file("docs/today_jobs.json",     _enc(today or []),       message, hdrs),
        _put_file("docs/yesterday_jobs.json", _enc(yesterday or []),   message, hdrs),
        _put_file("docs/metadata.json",       json.dumps(metadata, indent=2), message, hdrs),
    ]

    # Push per-run snapshots (non-fatal if any fail)
    snapshot_results = _push_run_snapshots(hdrs, message)
    if snapshot_results:
        logger.info("Pushed %d run snapshot(s).", len(snapshot_results))

    success = all(results)
    if success:
        logger.info("Dashboard live → https://atriveo-airflow.github.io/")
    else:
        logger.error("Deploy partially failed — check logs above.")
    return success


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    ok = deploy_output()
    sys.exit(0 if ok else 1)
