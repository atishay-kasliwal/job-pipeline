"""
High-priority pipeline: scrape → deduplicate → role filter →
important filter (top company + sponsorship-ok) → priority score.

Run as a module:
    python -m job_pipeline.more_important
    python -m job_pipeline.more_important --hours-old 3 --results 100
    python -m job_pipeline.more_important --no-save
"""
import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from job_pipeline import config
from job_pipeline.filters import deduplicate, filter_by_role
from job_pipeline.important_filter import apply_important_filter, load_top_companies
from job_pipeline.scraper import scrape
from job_pipeline.storage import insert_run

logger = logging.getLogger(__name__)

# Columns in the important-pipeline output
_OUTPUT_COLUMNS: list[str] = [
    "title",
    "company",
    "location",
    "job_url",
    "date_posted",
    "priority_score",
]


# ── Scoring ───────────────────────────────────────────────────────────────────

def _calculate_priority_score(row: pd.Series) -> int:
    """
    Lightweight priority scorer for the high-priority pipeline.

    Boosts come from config.PRIORITY_SCORE_BOOSTS and target
    new-grad / entry-level / backend language in title+description.
    """
    text = " ".join(
        str(row.get(col, "") or "") for col in ("title", "description")
    ).lower()

    return sum(
        boost
        for keyword, boost in config.PRIORITY_SCORE_BOOSTS.items()
        if keyword in text
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run_important_pipeline(
    scraper_overrides: dict[str, Any] | None = None,
    output_csv: Path = config.IMPORTANT_OUTPUT_CSV,
    output_json: Path = config.IMPORTANT_OUTPUT_JSON,
    save: bool = True,
    store: bool = True,
) -> pd.DataFrame:
    """
    Execute the high-priority pipeline end-to-end.

    Pipeline steps
    --------------
    1. **Scrape**           — fetch raw jobs from LinkedIn via JobSpy.
    2. **Deduplicate**      — drop exact URL and title+company duplicates.
    3. **Role filter**      — keep only matching entry-level engineering titles.
    4. **Important filter** — keep only top-company + sponsorship-ok rows.
    5. **Priority score**   — add ``priority_score`` column and sort descending.

    Note: The standard location / sponsorship filters are *not* applied here
    because the important filter already handles sponsorship and top companies
    are assumed to be US-based.

    Args:
        scraper_overrides: Passed to :func:`scraper.scrape`.
        output_csv:        Destination path for important_jobs.csv.
        output_json:       Destination path for important_jobs.json.
        save:              When False, skip writing files.

    Returns:
        DataFrame sorted by ``priority_score`` descending.
    """
    logger.info("=" * 55)
    logger.info("  Important Pipeline — START")
    logger.info("=" * 55)

    # 1. Scrape
    df = scrape(scraper_overrides)
    if df.empty:
        logger.warning("No jobs returned by scraper — aborting pipeline.")
        return df

    # 2. Deduplicate
    df = deduplicate(df)

    # 3. Role filter
    df = filter_by_role(df)
    if df.empty:
        logger.warning("No jobs passed the role filter.")
        return df

    # 4. Important filter
    top_companies = load_top_companies()
    df = apply_important_filter(df, top_companies)
    if df.empty:
        logger.warning("No jobs passed the important filter.")
        return df

    # 5. Priority score
    df = df.copy()
    df["priority_score"] = df.apply(_calculate_priority_score, axis=1)
    df = df.sort_values("priority_score", ascending=False).reset_index(drop=True)

    df_out = df[[c for c in _OUTPUT_COLUMNS if c in df.columns]].copy()

    if save:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(output_csv, index=False)
        df_out.to_json(output_json, orient="records", indent=2)
        logger.info("Saved %d important jobs → %s", len(df_out), output_csv)
        logger.info("Saved %d important jobs → %s", len(df_out), output_json)

    if store:
        try:
            insert_run(df_out, pipeline="important")
        except Exception as exc:
            logger.error("MongoDB insert failed (non-fatal): %s", exc)

    logger.info("=" * 55)
    logger.info("  Important Pipeline — DONE  (%d jobs)", len(df_out))
    logger.info("=" * 55)
    return df_out


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="High-priority job pipeline — top companies + visa-friendly roles.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--hours-old",
        type=int,
        default=config.SCRAPER["hours_old"],
        metavar="N",
        help="Include only jobs posted within the last N hours.",
    )
    parser.add_argument(
        "--results",
        type=int,
        default=config.SCRAPER["results_wanted"],
        metavar="N",
        help="Number of LinkedIn results to request.",
    )
    parser.add_argument(
        "--search",
        type=str,
        default=config.SCRAPER["search_term"],
        help="Job search term.",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write output files; print results only.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    overrides: dict[str, Any] = {
        "hours_old": args.hours_old,
        "results_wanted": args.results,
        "search_term": args.search,
    }

    result = run_important_pipeline(
        scraper_overrides=overrides,
        save=not args.no_save,
    )

    if result.empty:
        print("No important jobs found.")
    else:
        pd.set_option("display.max_colwidth", 60)
        print(result.to_string(index=False))
