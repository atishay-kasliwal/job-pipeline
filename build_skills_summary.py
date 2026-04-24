"""
One-shot script: reads output/descriptions.json + today_jobs titles,
counts keyword occurrences across all job descriptions, and writes
docs/skills_summary.json for the Atriveo Skills page.

Run from the job-pipeline repo root:
  python build_skills_summary.py
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

# ── Keyword dictionary ─────────────────────────────────────────────────────────
# Each category: list of (canonical_name, [patterns]) tuples.
# Patterns are matched as whole-word / phrase boundaries.

SKILL_CATEGORIES: dict[str, dict] = {
    "Languages": {
        "color": "#3b82f6",
        "skills": [
            ("Python",       ["python"]),
            ("Java",         ["\\bjava\\b"]),
            ("JavaScript",   ["javascript"]),
            ("TypeScript",   ["typescript"]),
            ("Go",           ["golang", "\\bgo lang\\b", "go developer", "written in go", "using go"]),
            ("Scala",        ["\\bscala\\b"]),
            ("Kotlin",       ["kotlin"]),
            ("C#",           ["\\bc#\\b", "csharp", "c sharp"]),
            ("C++",          ["c\\+\\+", "\\bcpp\\b"]),
            ("Rust",         ["\\brust\\b"]),
            ("Ruby",         ["\\bruby\\b"]),
            (".NET",         ["\\.net\\b", "dotnet"]),
            ("Swift",        ["\\bswift\\b"]),
            ("PHP",          ["\\bphp\\b"]),
            ("SQL",          ["\\bsql\\b"]),
            ("Bash/Shell",   ["\\bbash\\b", "shell scripting"]),
            ("HTML/CSS",     ["\\bhtml\\b", "\\bcss\\b"]),
            ("Elixir",       ["elixir"]),
            ("Clojure",      ["clojure"]),
        ],
    },
    "Frameworks & Libraries": {
        "color": "#8b5cf6",
        "skills": [
            ("Spring Boot",  ["spring boot"]),
            ("Spring",       ["\\bspring\\b"]),
            ("FastAPI",      ["fastapi"]),
            ("Django",       ["django"]),
            ("Flask",        ["\\bflask\\b"]),
            ("Express",      ["express\\.js", "expressjs", "\\bexpress\\b"]),
            ("NestJS",       ["nestjs"]),
            ("React",        ["\\breact\\b", "react\\.js", "reactjs"]),
            ("Next.js",      ["next\\.js", "nextjs"]),
            ("Vue",          ["\\bvue\\b", "vue\\.js"]),
            ("Angular",      ["angular"]),
            ("Node.js",      ["node\\.js", "nodejs"]),
            ("gRPC",         ["\\bgrpc\\b"]),
            ("Gin",          ["\\bgin\\b framework", "gin-gonic"]),
            ("Fiber",        ["\\bfiber\\b framework"]),
            ("Rails",        ["ruby on rails", "\\brails\\b"]),
            ("GraphQL",      ["graphql"]),
            ("Pydantic",     ["pydantic"]),
            ("Celery",       ["celery"]),
            ("SQLAlchemy",   ["sqlalchemy"]),
            ("Hibernate",    ["hibernate"]),
            ("JUnit",        ["junit"]),
            ("pytest",       ["pytest"]),
            ("Jest",         ["\\bjest\\b"]),
            ("Selenium",     ["selenium"]),
            ("Cypress",      ["cypress"]),
            ("Playwright",   ["playwright"]),
            ("Pandas",       ["pandas"]),
            ("NumPy",        ["numpy"]),
            ("Scikit-learn", ["scikit.learn", "sklearn"]),
            ("LangChain",    ["langchain"]),
        ],
    },
    "Cloud": {
        "color": "#f59e0b",
        "skills": [
            ("AWS",           ["\\baws\\b", "amazon web services"]),
            ("GCP",           ["\\bgcp\\b", "google cloud"]),
            ("Azure",         ["\\bazure\\b", "microsoft azure"]),
            ("Lambda",        ["\\blambda\\b"]),
            ("ECS",           ["\\becs\\b"]),
            ("EKS",           ["\\beks\\b"]),
            ("EC2",           ["\\bec2\\b"]),
            ("S3",            ["\\bs3\\b"]),
            ("RDS",           ["\\brds\\b"]),
            ("DynamoDB",      ["dynamodb"]),
            ("SQS",           ["\\bsqs\\b"]),
            ("SNS",           ["\\bsns\\b"]),
            ("Kinesis",       ["kinesis"]),
            ("CloudWatch",    ["cloudwatch"]),
            ("API Gateway",   ["api gateway"]),
            ("Cloud Run",     ["cloud run"]),
            ("GKE",           ["\\bgke\\b"]),
            ("Pub/Sub",       ["pub/sub", "pubsub"]),
            ("CloudFlare",    ["cloudflare"]),
            ("Serverless",    ["serverless"]),
            ("Cloud Storage", ["cloud storage"]),
            ("Step Functions",["step functions"]),
        ],
    },
    "Backend & Architecture": {
        "color": "#0ea5e9",
        "skills": [
            ("Microservices",      ["microservices", "micro.services"]),
            ("REST API",           ["rest api", "restful", "\\brest\\b"]),
            ("Distributed Systems",["distributed systems", "distributed computing"]),
            ("Event-Driven",       ["event.driven", "event driven"]),
            ("Message Queue",      ["message queue", "message broker", "\\bmq\\b"]),
            ("Kafka",              ["kafka"]),
            ("RabbitMQ",           ["rabbitmq"]),
            ("gRPC",               ["\\bgrpc\\b"]),
            ("WebSocket",          ["websocket", "web socket"]),
            ("System Design",      ["system design"]),
            ("High Availability",  ["high availability", "\\bha\\b cluster", "fault toleran"]),
            ("Scalability",        ["scalab", "horizontal scaling", "vertical scaling"]),
            ("Caching",            ["\\bcaching\\b", "cache layer", "caching strategy", "in.memory cache"]),
            ("Rate Limiting",      ["rate limit"]),
            ("API Design",         ["api design", "api development"]),
            ("CQRS",               ["\\bcqrs\\b"]),
            ("Event Sourcing",     ["event sourcing"]),
            ("Service Mesh",       ["service mesh", "\\bistio\\b"]),
            ("Load Balancing",     ["load balanc"]),
            ("Circuit Breaker",    ["circuit breaker"]),
        ],
    },
    "DevOps & Infrastructure": {
        "color": "#10b981",
        "skills": [
            ("Docker",         ["docker"]),
            ("Kubernetes",     ["kubernetes", "\\bk8s\\b"]),
            ("Terraform",      ["terraform"]),
            ("CI/CD",          ["ci/cd", "continuous integration", "continuous deployment", "continuous delivery"]),
            ("GitHub Actions", ["github actions"]),
            ("GitLab CI",      ["gitlab ci", "gitlab.ci"]),
            ("CircleCI",       ["circleci"]),
            ("Jenkins",        ["jenkins"]),
            ("Helm",           ["\\bhelm\\b"]),
            ("ArgoCD",         ["argocd", "argo cd"]),
            ("Ansible",        ["ansible"]),
            ("Prometheus",     ["prometheus"]),
            ("Grafana",        ["grafana"]),
            ("Datadog",        ["datadog"]),
            ("New Relic",      ["new relic"]),
            ("OpenTelemetry",  ["opentelemetry", "otel"]),
            ("Nginx",          ["nginx"]),
            ("Istio",          ["\\bistio\\b"]),
            ("Vault",          ["\\bvault\\b"]),
            ("Pulumi",         ["pulumi"]),
            ("Linux",          ["linux", "ubuntu", "debian"]),
            ("Git",            ["\\bgit\\b"]),
        ],
    },
    "Data & Storage": {
        "color": "#f43f5e",
        "skills": [
            ("PostgreSQL",   ["postgresql", "postgres", "\\bpg\\b"]),
            ("MySQL",        ["mysql"]),
            ("MongoDB",      ["mongodb"]),
            ("Redis",        ["redis"]),
            ("Elasticsearch",["elasticsearch", "opensearch"]),
            ("Cassandra",    ["cassandra"]),
            ("BigQuery",     ["bigquery"]),
            ("Snowflake",    ["snowflake"]),
            ("ClickHouse",   ["clickhouse"]),
            ("Spark",        ["apache spark", "pyspark", "\\bspark\\b"]),
            ("Airflow",      ["airflow"]),
            ("dbt",          ["\\bdbt\\b"]),
            ("Kafka",        ["kafka"]),
            ("Flink",        ["\\bflink\\b"]),
            ("Databricks",   ["databricks"]),
            ("Hive",         ["\\bhive\\b"]),
            ("Presto/Trino", ["presto", "trino"]),
            ("ETL/ELT",      ["\\betl\\b", "\\belt\\b", "data pipeline", "data ingestion"]),
            ("Vector DB",    ["vector database", "vector db", "pinecone", "weaviate", "chroma", "qdrant", "milvus"]),
            ("SQLite",       ["sqlite"]),
        ],
    },
    "AI & Machine Learning": {
        "color": "#ec4899",
        "skills": [
            ("LLM",            ["\\bllm\\b", "large language model"]),
            ("GenAI",          ["generative ai", "gen ai", "\\bgenai\\b"]),
            ("RAG",            ["\\brag\\b", "retrieval.augmented"]),
            ("OpenAI",         ["openai", "gpt-4", "gpt-3", "chatgpt"]),
            ("PyTorch",        ["pytorch"]),
            ("TensorFlow",     ["tensorflow"]),
            ("Hugging Face",   ["hugging face", "huggingface", "transformers"]),
            ("LangChain",      ["langchain"]),
            ("Machine Learning",["machine learning", "\\bml\\b model"]),
            ("Deep Learning",  ["deep learning"]),
            ("NLP",            ["\\bnlp\\b", "natural language processing"]),
            ("Computer Vision",["computer vision", "\\bcv\\b model"]),
            ("MLflow",         ["mlflow"]),
            ("Agents",         ["ai agent", "llm agent", "agentic"]),
            ("Fine-tuning",    ["fine.tun", "rlhf", "instruction tuning"]),
            ("Embeddings",     ["embeddings", "vector embeddings"]),
            ("Prompt Eng",     ["prompt engineering"]),
            ("Vertex AI",      ["vertex ai"]),
            ("SageMaker",      ["sagemaker"]),
            ("Claude/Anthropic",["anthropic", "\\bclaude\\b"]),
        ],
    },
    "Security": {
        "color": "#6366f1",
        "skills": [
            ("OAuth/OIDC",   ["oauth", "\\boidc\\b", "openid connect"]),
            ("JWT",          ["\\bjwt\\b"]),
            ("SAML",         ["\\bsaml\\b"]),
            ("TLS/SSL",      ["\\btls\\b", "\\bssl\\b"]),
            ("IAM",          ["aws iam", "iam roles", "iam policies", "identity.*access management"]),
            ("Zero Trust",   ["zero trust"]),
            ("SOC 2",        ["soc 2", "soc2"]),
            ("GDPR",         ["gdpr"]),
            ("RBAC",         ["\\brbac\\b", "role.based access"]),
            ("Pen Testing",  ["penetration test", "pen test"]),
            ("SAST/DAST",    ["\\bsast\\b", "\\bdast\\b", "static analysis"]),
            ("Encryption",   ["encrypt"]),
        ],
    },
    "Soft Skills & Process": {
        "color": "#64748b",
        "skills": [
            ("Agile/Scrum",     ["agile", "scrum", "\\bsprint\\b"]),
            ("Cross-functional",["cross.functional"]),
            ("Mentorship",      ["mentor"]),
            ("Technical Design",["technical design", "design document", "rfc"]),
            ("Code Review",     ["code review"]),
            ("On-call",         ["on.call", "oncall", "pagerduty"]),
            ("Communication",   ["strong communication", "excellent communication"]),
            ("Leadership",      ["tech lead", "technical lead", "engineering lead"]),
        ],
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    """Strip markdown, lowercase, normalise whitespace."""
    text = re.sub(r"\*+", " ", text)         # bold/italic markers
    text = re.sub(r"\\-", "-", text)          # escaped hyphens
    text = re.sub(r"\\n", " ", text)          # escaped newlines
    text = re.sub(r"[#>|`]", " ", text)       # heading / blockquote / code markers
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text


def count_in_text(patterns: list[str], text: str) -> bool:
    """Return True if any pattern matches in text."""
    for pat in patterns:
        if re.search(pat, text):
            return True
    return False


# ── Main ─────────────────────────────────────────────────────────────────────

def build_summary(desc_file: Path) -> dict:
    with desc_file.open() as f:
        data: dict[str, str] = json.load(f)

    # Clean all descriptions once
    texts = [clean_text(v) for v in data.values()]
    total = len(texts)
    print(f"Loaded {total:,} descriptions")

    categories_out: dict = {}
    for cat, meta in SKILL_CATEGORIES.items():
        skills_out: dict[str, int] = {}
        for canonical, patterns in meta["skills"]:
            count = sum(1 for t in texts if count_in_text(patterns, t))
            if count > 0:
                skills_out[canonical] = count
        # Sort by count descending
        skills_out = dict(sorted(skills_out.items(), key=lambda x: x[1], reverse=True))
        categories_out[cat] = {
            "color": meta["color"],
            "skills": skills_out,
        }
        top = list(skills_out.items())[:3]
        print(f"  {cat}: {len(skills_out)} skills, top={top}")

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_analyzed": total,
        "categories": categories_out,
    }


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent
    desc_file = repo_root / "output" / "descriptions.json"
    out_file  = repo_root / "docs" / "skills_summary.json"

    if not desc_file.exists():
        print(f"ERROR: {desc_file} not found")
        raise SystemExit(1)

    summary = build_summary(desc_file)
    out_file.parent.mkdir(exist_ok=True)
    out_file.write_text(json.dumps(summary, indent=2))
    print(f"\nWrote {out_file} ({out_file.stat().st_size / 1024:.1f} KB)")
