"""
Central configuration for the job pipeline.

All tuneable values live here — update freely without touching business logic.
No secrets; credentials or API keys should be supplied via environment variables.
"""
import os
from pathlib import Path

# Auto-load .env if python-dotenv is installed (local dev convenience).
# On OpenShift the Secret injects MONGO_URI directly as an env var.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass  # dotenv not installed — rely on env vars being set externally

# ── Project paths ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

TOP_COMPANIES_CSV = DATA_DIR / "top_companies.csv"
H1B_SPONSORS_CSV = DATA_DIR / "h1b_sponsors.csv"

# ── Scraper settings ──────────────────────────────────────────────────────────
SCRAPER: dict = {
    "site_name": ["linkedin"],
    "search_term": "software engineer",
    "location": "United States",
    "hours_old": 1,
    "results_wanted": 200,
    "linkedin_fetch_description": True,
}

# ── Role filter ───────────────────────────────────────────────────────────────
ROLE_INCLUDE_KEYWORDS: list[str] = [
    "software engineer",
    "backend",
    "sde",
    "software developer",
]

ROLE_EXCLUDE_KEYWORDS: list[str] = [
    "senior",
    "staff",
    "principal",
    "manager",
    "lead",
    "director",
    "architect",
    "vp ",
    "vice president",
    "head of",
]

# ── Location filter ───────────────────────────────────────────────────────────
ALLOWED_STATES: list[str] = [
    # Full names
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming",
    # Two-letter abbreviations
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    # Catch-all
    "United States", "Remote", "USA",
]

# ── Sponsorship filter ────────────────────────────────────────────────────────
SPONSORSHIP_REJECT_PHRASES: list[str] = [
    "no sponsorship",
    "will not sponsor",
    "cannot sponsor",
    "does not sponsor",
    "sponsorship not available",
    "sponsorship is not available",
    "unable to sponsor",
    "not able to sponsor",
    "us citizen only",
    "u.s. citizen only",
    "citizens only",
    "must be a us citizen",
    "must be a u.s. citizen",
    "gc holder",
    "green card holder",
    "green card only",
    "must be authorized to work",
    "no visa",
    "security clearance required",
    "active secret clearance",
    "top secret clearance",
]

SPONSORSHIP_PREFER_PHRASES: list[str] = [
    "h1b",
    "h-1b",
    "visa sponsorship",
    "will sponsor",
    "sponsorship available",
    "sponsorship provided",
    "willing to sponsor",
    "open to sponsorship",
]

# ── Remote filter ─────────────────────────────────────────────────────────────
REMOTE_ONLY: bool = False  # Set True to keep only remote positions

# ── Scoring weights ───────────────────────────────────────────────────────────
SCORE_BOOSTS: dict[str, int] = {
    "aws": 2,
    "spring": 2,
    "kubernetes": 2,
    "docker": 1,
    "java": 1,
    "new grad": 3,
    "new graduate": 3,
    "entry level": 2,
    "entry-level": 2,
    "0-2 years": 2,
    "0 to 2 years": 2,
    "junior": 1,
}

SCORE_PENALTIES: dict[str, int] = {
    "stale_job": -2,   # posted > 24 hours ago
    "big_tech": -2,    # big-tech companies attract more applicants
}

# Companies that attract large applicant pools (higher competition)
BIG_TECH_COMPANIES: list[str] = [
    "google",
    "amazon",
    "meta",
    "apple",
    "microsoft",
    "netflix",
    "uber",
    "airbnb",
    "twitter",
    "x corp",
]

# ── Priority score boosts (more_important pipeline) ───────────────────────────
PRIORITY_SCORE_BOOSTS: dict[str, int] = {
    "new grad": 3,
    "new graduate": 3,
    "entry level": 2,
    "entry-level": 2,
    "backend": 2,
    "aws": 2,
    "0-2 years": 2,
}

# ── Output paths ──────────────────────────────────────────────────────────────
OUTPUT_CSV = OUTPUT_DIR / "jobs.csv"
OUTPUT_JSON = OUTPUT_DIR / "jobs.json"
IMPORTANT_OUTPUT_CSV = OUTPUT_DIR / "important_jobs.csv"
IMPORTANT_OUTPUT_JSON = OUTPUT_DIR / "important_jobs.json"

# ── MongoDB ───────────────────────────────────────────────────────────────────
# Set MONGO_URI in your environment (or .env file) — never hardcode credentials.
# Format: mongodb+srv://<user>:<password>@atriveoairflow.lsyhpr5.mongodb.net/?appName=AtriveoAirflow
MONGO_URI: str = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://<db_username>:<db_password>@atriveoairflow.lsyhpr5.mongodb.net/?appName=AtriveoAirflow",
)
MONGO_DB_NAME: str = "job_pipeline"

# ── Archival ──────────────────────────────────────────────────────────────────
ARCHIVE_DIR = BASE_DIR / "archives"   # ./archives/YYYY-MM-DD/<session>.csv
ARCHIVE_RETENTION_DAYS: int = 7       # sessions older than this get archived
