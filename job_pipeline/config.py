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
    # Seniority
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
    # QA / Testing roles — different discipline, not target roles
    "sdet",
    "test automation",
    "automation engineer",
    "qa engineer",
    "quality assurance",
    "quality engineer",
    "test engineer",
    "testing engineer",
]

# ── Location filter ───────────────────────────────────────────────────────────
# All 50 US states — location chips on the dashboard handle frontend filtering.
ALLOWED_STATES: list[str] = [
    # Full state names
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia",
    "Washington", "West Virginia", "Wisconsin", "Wyoming",
    # Two-letter abbreviations
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    # Catch-all — keep remote / unspecified rows
    "United States", "Remote", "USA",
]

# ── Sponsorship filter ────────────────────────────────────────────────────────
SPONSORSHIP_REJECT_PHRASES: list[str] = [
    # Sponsorship rejections
    "no sponsorship",
    "will not sponsor",
    "cannot sponsor",
    "does not sponsor",
    "sponsorship not available",
    "sponsorship is not available",
    "sponsorship: not available",
    "not available for this role",
    "sponsorship not provided",
    "sponsorship is not provided",
    "unable to sponsor",
    "not able to sponsor",
    "not offer sponsorship",
    "not offering sponsorship",
    "no visa",
    "visa: not available",
    # Citizenship / permanent residency requirements
    "us citizen only",
    "u.s. citizen only",
    "citizens only",
    "must be a us citizen",
    "must be a u.s. citizen",
    "must be a united states citizen",
    "us citizenship required",
    "u.s. citizenship required",
    "united states citizenship required",
    "citizenship is required",
    "gc holder",
    "green card holder",
    "green card only",
    "must be authorized to work",
    "must have authorization to work",
    "authorization to work in the us",
    "authorization to work in the united states",
    "authorization to work in the us or canada without sponsorship",
    "authorized to work in the us without sponsorship",
    "authorized to work in the united states without sponsorship",
    "work authorization without sponsorship",
    "work in the us without sponsorship",
    "eligible to work in the us without sponsorship",
    "without the need for sponsorship",
    "without requiring sponsorship",
    "permanent resident",
    "lawful permanent resident",
    # Security clearance — any mention of requiring one is a disqualifier
    "security clearance required",
    "clearance required",
    "active clearance",
    "active secret clearance",
    "active top secret",
    "top secret clearance",
    "top-secret clearance",
    "ts/sci",
    "ts sci",
    "top secret/sci",
    "sci eligibility",
    "sci access",
    "dod clearance",
    "dod secret",
    "government clearance",
    "public trust clearance",
    "secret clearance",
    "must have clearance",
    "must hold clearance",
    "ability to obtain a clearance",
    "ability to obtain and maintain",
    "obtain a government clearance",
    "obtain a security clearance",
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
    # Career level (highest priority — directly relevant roles)
    "new grad": 4,
    "new graduate": 4,
    "entry level": 3,
    "entry-level": 3,
    "0-2 years": 3,
    "0 to 2 years": 3,
    "1-3 years": 2,
    "1 to 3 years": 2,
    "junior": 2,
    # Core stack — Java/Spring (3 years experience)
    "spring boot": 4,
    "spring": 3,
    "java": 3,
    "microservices": 2,
    "distributed systems": 2,
    "rest api": 2,
    "restful": 1,
    # Python stack (research + FastAPI work)
    "fastapi": 4,
    "python": 3,
    "django": 1,
    "flask": 1,
    # Cloud / infra (AWS heavy)
    "aws": 3,
    "lambda": 2,
    "ecs": 2,
    "kubernetes": 3,
    "docker": 2,
    "terraform": 2,
    "kafka": 3,
    "ci/cd": 2,
    # Data / secondary skills
    "postgresql": 2,
    "typescript": 2,
    "react": 1,
    "graphql": 2,
    "airflow": 2,
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
    "new grad": 4,
    "new graduate": 4,
    "entry level": 3,
    "entry-level": 3,
    "0-2 years": 3,
    "1-3 years": 2,
    "backend": 2,
    "spring boot": 4,
    "java": 3,
    "python": 3,
    "fastapi": 4,
    "aws": 3,
    "kubernetes": 3,
    "kafka": 3,
    "microservices": 2,
    "distributed": 2,
}

# ── Output paths ──────────────────────────────────────────────────────────────
OUTPUT_CSV = OUTPUT_DIR / "jobs.csv"
OUTPUT_JSON = OUTPUT_DIR / "jobs.json"
IMPORTANT_OUTPUT_CSV = OUTPUT_DIR / "important_jobs.csv"
IMPORTANT_OUTPUT_JSON = OUTPUT_DIR / "important_jobs.json"
TOP500_OUTPUT_CSV = OUTPUT_DIR / "top500_jobs.csv"
TOP500_OUTPUT_JSON = OUTPUT_DIR / "top500_jobs.json"
H1B2026_OUTPUT_CSV = OUTPUT_DIR / "h1b2026_jobs.csv"
H1B2026_OUTPUT_JSON = OUTPUT_DIR / "h1b2026_jobs.json"
KEYWORDS_OUTPUT_CSV = OUTPUT_DIR / "keywords_jobs.csv"
KEYWORDS_OUTPUT_JSON = OUTPUT_DIR / "keywords_jobs.json"

# Keywords that match the target candidate's tech stack
# Jobs scoring ≥ KEYWORDS_MIN_SCORE are included in the keywords pipeline
RESUME_KEYWORDS: list[str] = [
    # Java / Spring (strongest match)
    "java", "spring boot", "spring", "kotlin",
    # Python (FastAPI / research work)
    "python", "fastapi", "django", "flask",
    # Cloud / infra
    "aws", "lambda", "ecs", "kubernetes", "docker", "terraform",
    # Data / messaging
    "kafka", "airflow", "postgresql", "postgres", "graphql",
    # TypeScript / frontend secondary
    "typescript", "react",
    # Architecture keywords
    "microservices", "distributed systems", "distributed",
    "rest api", "restful",
]
KEYWORDS_MIN_SCORE: int = 3   # job must match at least 3 keyword points to appear

TOP_500_COMPANIES_CSV = DATA_DIR / "top_500_companies.csv"
H1B_2026_CSV = DATA_DIR / "h1b_2026.csv"

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
