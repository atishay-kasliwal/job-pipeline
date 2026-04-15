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
    "search_term": "software engineer",  # overridden per-term in scraper.py
    "location": "United States",
    "hours_old": 4,
    "results_wanted": 500,   # per search term; deduplicated before filtering
    "linkedin_fetch_description": True,
    # Boards tested and currently broken in jobspy:
    #   glassdoor  — 400 bot-protection
    #   google     — returns 0 results (scraper breaks when Google changes layout)
    #   zip_recruiter — returns 0 results
}

# Multiple search terms — scraper runs once per term and merges results.
SEARCH_TERMS: list[str] = [
    "software engineer",
    "new grad",
    "backend engineer",
    "machine learning engineer",
    "data scientist",
    "AI engineer",
]

# ── Role filter ───────────────────────────────────────────────────────────────
# Matched case-insensitively as substring of the job title.
ROLE_INCLUDE_KEYWORDS: list[str] = [
    "software",
    "python",
    "backend",
    "new grad",
    "machine learning",
    "data scientist",
    "data engineer",
    "ml engineer",
    "ai engineer",
    "applied scientist",
]

# Companies to exclude — job aggregators, spam boards, low-quality sources.
# Matched case-insensitively as substring of the company field.
COMPANY_EXCLUDE: list[str] = [
    "dice",
    "remotehunter",
    "jobs via dice",
    "jobot",
    "cybercoders",
    "lancesoft",
    "haystack",
    "supermicro",
    "turing",
    "micro1",
    "hackajob",
    "sundayy",
    "jobright.ai"
]

ROLE_EXCLUDE_KEYWORDS: list[str] = [
    # Seniority
    "senior",
    "sr",
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
    # Non-target disciplines
    "physical therapist",
    "physical therapy",
    "therapist",
    "electrical engineer",
    "electrical engineering",
    "nurse",
    "nursing",
    "registered nurse",
    "clinical nurse",
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
    "no need for visa sponsorship",
    "must have the right to work in the uk without current or future sponsorship",
    "will not sponsor",
    "cannot sponsor",
    "does not sponsor",
    "not eligible for sponsorship",
    "not eligible for visa sponsorship",
    "not eligible for immigration sponsorship",
    "not eligible for u.s. immigration sponsorship",
    "not eligible for us immigration sponsorship",
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
    "legally authorized to work for any company in the united states without sponsorship",
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

# ── Scoring: personal tech stack ──────────────────────────────────────────────
# Each keyword earns points when found in title + description (binary, not frequency).
PERSONAL_STACK: dict[str, dict[str, int]] = {
    "core": {
        # Java
        "java": 5, "spring": 6, "spring boot": 8,
        # Python
        "python": 7, "fastapi": 7, "django": 6, "flask": 5,
        "pydantic": 5, "asyncio": 5, "celery": 4, "sqlalchemy": 4,
    },
    "cloud": {
        "aws": 7, "lambda": 5, "ecs": 5, "sqs": 4, "sns": 4, "api gateway": 5,
        "gcp": 4, "google cloud": 4,
    },
    "backend": {
        "microservices": 7, "rest": 6, "api": 6, "distributed systems": 8,
        "grpc": 5, "graphql": 4,
    },
    "devops": {
        "docker": 6, "kubernetes": 7, "ci/cd": 6, "jenkins": 4, "terraform": 5,
    },
    "data": {
        "postgresql": 4, "postgres": 4, "kafka": 5,
        "redis": 4, "airflow": 4, "pandas": 3,
    },
}

# Bonus points when a full tech combo appears together (order-independent).
SYNERGY_COMBOS: list[tuple[list[str], int]] = [
    # Java
    (["java", "spring", "aws"],                10),
    (["microservices", "docker", "kubernetes"], 10),
    # Python
    (["python", "fastapi", "aws"],             10),
    (["python", "django", "aws"],               8),
    (["python", "fastapi", "postgresql"],       8),
    (["python", "celery", "kafka"],             7),
    # General
    (["rest", "api", "backend"],               10),
]

# Level keywords matched against the job title.
LEVEL_SCORES: dict[str, int] = {
    "new grad":  100,
    "entry":     6,
    "associate": 8,
    "mid":       10,
    "sde2":      10,
    "senior":    -3,
}

# H1B sponsor bonus — applied when the company appears in h1b_2026.csv.
H1B_SCORE_BONUS: int = 8

# Practical maximum raw score used for % normalisation.
# Represents a near-perfect job match (not every keyword firing at once).
SCORE_MAX_RAW: int = 130

# Companies that attract large applicant pools (higher competition estimate).
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
KEYWORDS_MIN_SCORE: int = 15  # job must score ≥15 keyword points (≈2-3 strong stack matches)

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
