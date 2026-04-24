"""
Trigger the update-pages GitHub Actions workflow via the GitHub API.
Called at the end of every scraper run so data is exported immediately
instead of waiting for the :30 cron schedule (which can be delayed 30-60 min).
"""
import logging
import os
import subprocess

import httpx

logger = logging.getLogger(__name__)

GITHUB_REPO = "atishay-kasliwal/job-pipeline"
WORKFLOW_FILE = "update-pages.yml"
GITHUB_API = "https://api.github.com"


def _resolve_token() -> str:
    """
    Resolve a GitHub token from the environment or the gh CLI.

    Priority:
    1. GITHUB_TOKEN env var
    2. gh auth token (uses existing gh CLI session)
    """
    token = os.getenv("GITHUB_TOKEN", "").strip()
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

    return ""


def trigger_export() -> bool:
    """
    Dispatch the update-pages workflow. Returns True on success.
    Uses GITHUB_TOKEN env var or falls back to gh CLI token.
    """
    token = _resolve_token()
    if not token:
        logger.warning("No GitHub token found — skipping export trigger. Run `gh auth login` or set GITHUB_TOKEN.")
        return False

    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    try:
        resp = httpx.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"ref": "main"},
            timeout=15,
        )
        if resp.status_code == 204:
            logger.info("Export workflow triggered successfully.")
            return True
        else:
            logger.warning("Export trigger failed: HTTP %s — %s", resp.status_code, resp.text)
            return False
    except Exception as exc:
        logger.warning("Export trigger exception (non-fatal): %s", exc)
        return False
