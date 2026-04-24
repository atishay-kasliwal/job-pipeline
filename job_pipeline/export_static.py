"""
Static export: reads the latest session from MongoDB and writes JSON files
to docs/ so GitHub Pages can serve them without a backend.

Called by the GitHub Actions workflow every hour (30 min after the scraper).

Output files
------------
docs/jobs.json           — latest standard pipeline results
docs/important_jobs.json — latest important pipeline results
docs/metadata.json       — last_updated timestamp + counts
"""
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"
DEFAULT_DASHBOARD_TZ = "America/New_York"

_tz_name = os.getenv("DASHBOARD_TZ", DEFAULT_DASHBOARD_TZ).strip() or DEFAULT_DASHBOARD_TZ
try:
    DASHBOARD_TZ = ZoneInfo(_tz_name)
except Exception:  # pragma: no cover - fallback only if host tz data is missing
    logger.warning("Invalid DASHBOARD_TZ='%s'; falling back to UTC.", _tz_name)
    DASHBOARD_TZ = timezone.utc
    _tz_name = "UTC"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serialise(obj: Any) -> Any:
    """Make a MongoDB document JSON-safe (convert datetime → ISO string)."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialise(i) for i in obj]
    return obj


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, default=str))
    logger.info("Wrote %s", path)


def _local_day_bounds_utc(days_ago: int) -> tuple[datetime, datetime]:
    """
    Return [start_utc, end_utc) for a local calendar day in ``DASHBOARD_TZ``.
    """
    now_local = datetime.now(tz=DASHBOARD_TZ)
    day_local = now_local.date() - timedelta(days=days_ago)
    start_local = datetime.combine(day_local, datetime.min.time(), tzinfo=DASHBOARD_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


# ── Export logic ──────────────────────────────────────────────────────────────

def export_pipeline(pipeline: str) -> list[dict]:
    """
    Fetch the most recent non-archived session for *pipeline* and return
    its job documents as a list of plain dicts (MongoDB _id stripped).
    """
    from job_pipeline.storage import get_db
    db = get_db()

    session = db["sessions"].find_one(
        {"pipeline": pipeline, "archived": False},
        sort=[("run_at", -1)],
    )

    if not session:
        logger.warning("No active session found for pipeline '%s'.", pipeline)
        return []

    sid = session["session_id"]
    jobs = list(
        db["jobs"].find(
            {"session_id": sid},
            {"_id": 0, "run_at": 0},          # drop internal fields
        )
    )

    jobs = [_serialise(j) for j in jobs]
    logger.info(
        "Exported %d jobs from session '%s' (pipeline=%s).",
        len(jobs), sid, pipeline,
    )
    return jobs


def export_today_jobs() -> list[dict]:
    """
    Fetch all standard-pipeline jobs from today in ``DASHBOARD_TZ`` across all sessions.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    day_start, day_end = _local_day_bounds_utc(days_ago=0)

    # Get all session_ids from today
    sessions = db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": day_start, "$lt": day_end}},
        {"session_id": 1},
    )
    sids = [s["session_id"] for s in sessions]

    if not sids:
        return []

    jobs = list(db["jobs"].find(
        {"session_id": {"$in": sids}},
        {"_id": 0, "run_at": 0},
    ))
    # Deduplicate by job_url
    seen, unique = set(), []
    for j in jobs:
        key = j.get("job_url") or f"{j.get('title')}-{j.get('company')}"
        if key not in seen:
            seen.add(key)
            unique.append(j)
    return [_serialise(j) for j in unique]


def export_yesterday_jobs() -> list[dict]:
    """
    Fetch all standard-pipeline jobs from yesterday in ``DASHBOARD_TZ`` across all sessions.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    day_start, day_end = _local_day_bounds_utc(days_ago=1)

    sessions = db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": day_start, "$lt": day_end}},
        {"session_id": 1},
    )
    sids = [s["session_id"] for s in sessions]

    if not sids:
        return []

    jobs = list(db["jobs"].find(
        {"session_id": {"$in": sids}},
        {"_id": 0, "run_at": 0},
    ))
    seen, unique = set(), []
    for j in jobs:
        key = j.get("job_url") or f"{j.get('title')}-{j.get('company')}"
        if key not in seen:
            seen.add(key)
            unique.append(j)
    return [_serialise(j) for j in unique]


def export_week_jobs() -> list[dict]:
    """
    Fetch all standard-pipeline jobs from the last 7 days across all sessions.
    Deduplicates by job_url (keeps first/oldest occurrence).
    Adds a ``scraped_date`` field (YYYY-MM-DD in ``DASHBOARD_TZ``) so the
    frontend can filter by day.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)

    # Oldest-first so dedup keeps the earliest occurrence of each job
    sessions = list(db["sessions"].find(
        {"pipeline": "standard", "archived": False, "run_at": {"$gte": cutoff}},
        {"session_id": 1, "run_at": 1},
        sort=[("run_at", 1)],
    ))

    if not sessions:
        return []

    sid_to_date: dict[str, str] = {}
    for s in sessions:
        run_at = s["run_at"]
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=timezone.utc)
        sid_to_date[s["session_id"]] = run_at.astimezone(DASHBOARD_TZ).strftime("%Y-%m-%d")

    sid_order = {s["session_id"]: i for i, s in enumerate(sessions)}
    sids = list(sid_order.keys())

    jobs = list(db["jobs"].find(
        {"session_id": {"$in": sids}},
        {"_id": 0, "run_at": 0},
    ))

    # Sort by session chronological order so dedup picks the oldest session
    jobs.sort(key=lambda j: sid_order.get(j.get("session_id", ""), 999_999))

    seen: set[str] = set()
    unique: list[dict] = []
    for j in jobs:
        key = j.get("job_url") or f"{j.get('title')}-{j.get('company')}"
        if key not in seen:
            seen.add(key)
            j["scraped_date"] = sid_to_date.get(j.get("session_id", ""), "")
            unique.append(j)

    # Newest date first, then highest score
    unique.sort(
        key=lambda j: (j.get("scraped_date", ""), j.get("score") or 0),
        reverse=True,
    )

    logger.info("Exported %d unique jobs for weekly view (last 7 days).", len(unique))
    return [_serialise(j) for j in unique]


# ── Skills summary ────────────────────────────────────────────────────────────

_SKILL_CATEGORIES: dict[str, dict] = {
    "Languages": {"color": "#3b82f6", "skills": [
        ("Python", ["python"]), ("Java", ["\\bjava\\b"]), ("JavaScript", ["javascript"]),
        ("TypeScript", ["typescript"]), ("Go", ["golang", "go developer", "written in go"]),
        ("Scala", ["\\bscala\\b"]), ("Kotlin", ["kotlin"]), ("C#", ["\\bc#\\b", "csharp"]),
        ("C++", ["c\\+\\+", "\\bcpp\\b"]), ("Rust", ["\\brust\\b"]), ("Ruby", ["\\bruby\\b"]),
        (".NET", ["\\.net\\b", "dotnet"]), ("Swift", ["\\bswift\\b"]), ("PHP", ["\\bphp\\b"]),
        ("SQL", ["\\bsql\\b"]), ("Bash/Shell", ["\\bbash\\b", "shell scripting"]),
        ("HTML/CSS", ["\\bhtml\\b", "\\bcss\\b"]),
    ]},
    "Frameworks & Libraries": {"color": "#8b5cf6", "skills": [
        ("Spring Boot", ["spring boot"]), ("Spring", ["\\bspring\\b"]), ("FastAPI", ["fastapi"]),
        ("Django", ["django"]), ("Flask", ["\\bflask\\b"]), ("Express", ["express\\.js", "\\bexpress\\b"]),
        ("NestJS", ["nestjs"]), ("React", ["\\breact\\b", "react\\.js"]),
        ("Next.js", ["next\\.js", "nextjs"]), ("Vue", ["\\bvue\\b"]), ("Angular", ["angular"]),
        ("Node.js", ["node\\.js", "nodejs"]), ("GraphQL", ["graphql"]),
        ("Rails", ["ruby on rails", "\\brails\\b"]), ("LangChain", ["langchain"]),
        ("Pandas", ["pandas"]), ("NumPy", ["numpy"]), ("Scikit-learn", ["scikit.learn", "sklearn"]),
        ("PyTorch", ["pytorch"]), ("TensorFlow", ["tensorflow"]),
        ("Playwright", ["playwright"]), ("Selenium", ["selenium"]),
    ]},
    "Cloud": {"color": "#f59e0b", "skills": [
        ("AWS", ["\\baws\\b", "amazon web services"]), ("GCP", ["\\bgcp\\b", "google cloud"]),
        ("Azure", ["\\bazure\\b"]), ("Lambda", ["\\blambda\\b"]), ("ECS", ["\\becs\\b"]),
        ("EKS", ["\\beks\\b"]), ("EC2", ["\\bec2\\b"]), ("S3", ["\\bs3\\b"]),
        ("DynamoDB", ["dynamodb"]), ("SQS", ["\\bsqs\\b"]), ("Kinesis", ["kinesis"]),
        ("CloudWatch", ["cloudwatch"]), ("API Gateway", ["api gateway"]),
        ("Cloud Run", ["cloud run"]), ("GKE", ["\\bgke\\b"]), ("Serverless", ["serverless"]),
    ]},
    "Backend & Architecture": {"color": "#0ea5e9", "skills": [
        ("Microservices", ["microservices"]), ("REST API", ["rest api", "restful", "\\brest\\b"]),
        ("Distributed Systems", ["distributed systems"]), ("Event-Driven", ["event.driven"]),
        ("Kafka", ["kafka"]), ("RabbitMQ", ["rabbitmq"]), ("gRPC", ["\\bgrpc\\b"]),
        ("WebSocket", ["websocket"]), ("System Design", ["system design"]),
        ("Scalability", ["scalab", "horizontal scaling"]),
        ("Caching", ["\\bcaching\\b", "cache layer"]), ("API Design", ["api design"]),
        ("Load Balancing", ["load balanc"]), ("Service Mesh", ["service mesh", "\\bistio\\b"]),
    ]},
    "DevOps & Infrastructure": {"color": "#10b981", "skills": [
        ("Docker", ["docker"]), ("Kubernetes", ["kubernetes", "\\bk8s\\b"]),
        ("Terraform", ["terraform"]),
        ("CI/CD", ["ci/cd", "continuous integration", "continuous deploy"]),
        ("GitHub Actions", ["github actions"]), ("Jenkins", ["jenkins"]),
        ("Helm", ["\\bhelm\\b"]), ("ArgoCD", ["argocd"]), ("Ansible", ["ansible"]),
        ("Prometheus", ["prometheus"]), ("Grafana", ["grafana"]), ("Datadog", ["datadog"]),
        ("OpenTelemetry", ["opentelemetry", "\\botel\\b"]), ("Linux", ["linux"]),
        ("Git", ["\\bgit\\b"]),
    ]},
    "Data & Storage": {"color": "#f43f5e", "skills": [
        ("PostgreSQL", ["postgresql", "postgres"]), ("MySQL", ["mysql"]),
        ("MongoDB", ["mongodb"]), ("Redis", ["redis"]),
        ("Elasticsearch", ["elasticsearch", "opensearch"]), ("Cassandra", ["cassandra"]),
        ("BigQuery", ["bigquery"]), ("Snowflake", ["snowflake"]), ("ClickHouse", ["clickhouse"]),
        ("Spark", ["apache spark", "pyspark", "\\bspark\\b"]), ("Airflow", ["airflow"]),
        ("dbt", ["\\bdbt\\b"]), ("Databricks", ["databricks"]), ("Kafka", ["kafka"]),
        ("Flink", ["\\bflink\\b"]),
        ("ETL/ELT", ["\\betl\\b", "\\belt\\b", "data pipeline", "data ingestion"]),
        ("Vector DB", ["vector database", "vector db", "pinecone", "weaviate", "qdrant"]),
    ]},
    "AI & Machine Learning": {"color": "#ec4899", "skills": [
        ("LLM", ["\\bllm\\b", "large language model"]), ("GenAI", ["generative ai", "\\bgenai\\b"]),
        ("RAG", ["\\brag\\b", "retrieval.augmented"]), ("OpenAI", ["openai", "gpt-4"]),
        ("PyTorch", ["pytorch"]), ("TensorFlow", ["tensorflow"]),
        ("Hugging Face", ["hugging face", "huggingface", "transformers"]),
        ("LangChain", ["langchain"]),
        ("Machine Learning", ["machine learning"]), ("Deep Learning", ["deep learning"]),
        ("NLP", ["\\bnlp\\b", "natural language processing"]), ("MLflow", ["mlflow"]),
        ("Agents", ["ai agent", "llm agent", "agentic"]),
        ("Fine-tuning", ["fine.tun", "\\brlhf\\b"]),
        ("Embeddings", ["embeddings", "vector embeddings"]),
        ("Prompt Eng", ["prompt engineering"]),
        ("Vertex AI", ["vertex ai"]), ("SageMaker", ["sagemaker"]),
    ]},
    "Security": {"color": "#6366f1", "skills": [
        ("OAuth/OIDC", ["oauth", "\\boidc\\b"]), ("JWT", ["\\bjwt\\b"]),
        ("TLS/SSL", ["\\btls\\b", "\\bssl\\b"]),
        ("IAM", ["aws iam", "iam roles", "iam policies", "identity.*access management"]),
        ("Zero Trust", ["zero trust"]), ("SOC 2", ["soc 2", "soc2"]),
        ("GDPR", ["gdpr"]), ("RBAC", ["\\brbac\\b"]), ("Encryption", ["encrypt"]),
    ]},
    "Soft Skills & Process": {"color": "#64748b", "skills": [
        ("Agile/Scrum", ["agile", "scrum"]), ("Cross-functional", ["cross.functional"]),
        ("Mentorship", ["mentor"]), ("Code Review", ["code review"]),
        ("On-call", ["on.call", "oncall"]), ("Leadership", ["tech lead", "technical lead"]),
    ]},
}


def _clean(text: str) -> str:
    text = re.sub(r"\*+", " ", text)
    text = re.sub(r"\\-", "-", text)
    text = text.lower()
    return re.sub(r"\s+", " ", text)


def compute_job_scores(resume_text: str, job_urls: list[str]) -> dict[str, dict]:
    """
    Compute ATS + Fit scores for the given job URLs against the stored resume.

    ATS Score  — % of the JD's skill keywords that appear in the resume.
    Fit Score  — TF-IDF cosine similarity between resume and full JD text (0-100).

    Returns {job_url: {"ats_score": int, "fit_score": int}}.
    """
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
    except ImportError:
        logger.warning("scikit-learn not installed — skipping job score computation.")
        return {}

    from job_pipeline.storage import get_db
    db = get_db()

    docs = list(db["descriptions"].find(
        {"job_url": {"$in": job_urls}},
        {"_id": 0, "job_url": 1, "description": 1},
    ))
    if not docs:
        logger.info("No descriptions found for scoring — skipping.")
        return {}

    url_to_desc = {d["job_url"]: d["description"] for d in docs}
    urls = list(url_to_desc.keys())
    resume_lower = resume_text.lower()

    # TF-IDF fit scores
    texts = [resume_text] + [url_to_desc[u] for u in urls]
    vectorizer = TfidfVectorizer(max_features=8000, stop_words="english", ngram_range=(1, 2))
    tfidf = vectorizer.fit_transform(texts)
    similarities = cosine_similarity(tfidf[0:1], tfidf[1:])[0]

    result: dict[str, dict] = {}
    for i, url in enumerate(urls):
        desc_lower = url_to_desc[url].lower()

        # ATS score: keyword overlap between JD skills and resume skills
        jd_skills: set[str] = set()
        matched: set[str] = set()
        for cat in _SKILL_CATEGORIES.values():
            for canonical, patterns in cat["skills"]:
                compiled = [re.compile(p) for p in patterns]
                if any(c.search(desc_lower) for c in compiled):
                    jd_skills.add(canonical)
                    if any(c.search(resume_lower) for c in compiled):
                        matched.add(canonical)
        ats = int(len(matched) / len(jd_skills) * 100) if jd_skills else 0

        fit = int(round(float(similarities[i]) * 100))
        result[url] = {"ats_score": ats, "fit_score": fit}

    logger.info("Computed scores for %d jobs (ATS + Fit).", len(result))
    return result


def export_skills_summary() -> dict:
    """
    Build skill frequency counts from the MongoDB ``descriptions`` collection.
    Returns a dict ready to write as skills_summary.json.
    """
    from job_pipeline.storage import get_db
    db = get_db()

    docs = list(db["descriptions"].find({}, {"_id": 0, "description": 1}))
    texts = [_clean(d["description"]) for d in docs if d.get("description")]
    total = len(texts)
    logger.info("Building skills summary from %d descriptions.", total)

    categories_out: dict = {}
    for cat, meta in _SKILL_CATEGORIES.items():
        skills_out: dict[str, int] = {}
        for canonical, patterns in meta["skills"]:
            compiled = [re.compile(p) for p in patterns]
            count = sum(1 for t in texts if any(c.search(t) for c in compiled))
            if count > 0:
                skills_out[canonical] = count
        skills_out = dict(sorted(skills_out.items(), key=lambda x: x[1], reverse=True))
        categories_out[cat] = {"color": meta["color"], "skills": skills_out}

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_analyzed": total,
        "categories": categories_out,
    }


def export_run_history() -> list[dict]:
    """Fetch recent run history from MongoDB sessions in the format the frontend expects."""
    from job_pipeline.storage import get_db
    db = get_db()

    sessions = list(db["sessions"].find(
        {"pipeline": "standard", "archived": False},
        {"_id": 0, "session_id": 1, "run_at": 1, "total_jobs": 1, "snapshot_file": 1},
        sort=[("run_at", -1)],
        limit=48,
    ))
    return [_serialise(s) for s in sessions]


def run_export() -> None:
    """
    Export both pipelines to docs/ and write a metadata file.
    Creates docs/ if it does not exist.
    """
    import os
    from job_pipeline.storage import get_resume, save_resume

    DOCS_DIR.mkdir(exist_ok=True)

    # Allow resume to be injected via env var (e.g. from workflow_dispatch input)
    resume_input = os.getenv("RESUME_TEXT", "").strip()
    if resume_input:
        save_resume(resume_input)

    standard_jobs   = export_pipeline("standard")
    important_jobs  = export_pipeline("important")
    today_jobs      = export_today_jobs()
    yesterday_jobs  = export_yesterday_jobs()
    week_jobs       = export_week_jobs()
    run_history     = export_run_history()
    skills_summary  = export_skills_summary()

    # Attach ATS + Fit scores if a resume is available
    resume = get_resume()
    if resume:
        all_urls = list({
            j["job_url"]
            for jobs_list in [today_jobs, yesterday_jobs, week_jobs]
            for j in jobs_list
            if j.get("job_url")
        })
        scores = compute_job_scores(resume, all_urls)
        if scores:
            for jobs_list in [standard_jobs, important_jobs, today_jobs, yesterday_jobs, week_jobs]:
                for job in jobs_list:
                    s = scores.get(job.get("job_url", ""))
                    if s:
                        job["ats_score"] = s["ats_score"]
                        job["fit_score"] = s["fit_score"]
    else:
        logger.info("No resume stored — skipping ATS/Fit scoring.")

    metadata = {
        "last_updated":    datetime.now(tz=timezone.utc).isoformat(),
        "standard_count":  len(standard_jobs),
        "important_count": len(important_jobs),
        "today_count":     len(today_jobs),
        "week_count":      len(week_jobs),
        "dashboard_tz":    _tz_name,
    }

    _write_json(DOCS_DIR / "jobs.json",             standard_jobs)
    _write_json(DOCS_DIR / "important_jobs.json",   important_jobs)
    _write_json(DOCS_DIR / "today_jobs.json",       today_jobs)
    _write_json(DOCS_DIR / "yesterday_jobs.json",   yesterday_jobs)
    _write_json(DOCS_DIR / "week_jobs.json",        week_jobs)
    _write_json(DOCS_DIR / "run_history.json",      run_history)
    _write_json(DOCS_DIR / "metadata.json",         metadata)
    _write_json(DOCS_DIR / "skills_summary.json",   skills_summary)

    logger.info(
        "Export complete — %d standard, %d important, %d weekly jobs.",
        len(standard_jobs), len(important_jobs), len(week_jobs),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    run_export()
