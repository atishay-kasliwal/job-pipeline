"""
Trigger the update-pages GitHub Actions workflow via the GitHub API.
Called at the end of every scraper run so data is exported immediately
instead of waiting for the :30 cron schedule (which can be delayed 30-60 min).
"""
import logging
import os

import httpx

logger = logging.getLogger(__name__)

GITHUB_REPO = "atishay-kasliwal/job-pipeline"
WORKFLOW_FILE = "update-pages.yml"
GITHUB_API = "https://api.github.com"


def trigger_export() -> bool:
    """
    Dispatch the update-pages workflow. Returns True on success.
    Requires GITHUB_TOKEN env var with workflow dispatch permissions.
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        logger.warning("GITHUB_TOKEN not set — skipping export trigger.")
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
