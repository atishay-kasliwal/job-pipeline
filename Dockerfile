# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

LABEL org.opencontainers.image.title="job-pipeline" \
      org.opencontainers.image.description="LinkedIn job aggregation, filtering & ranking pipeline"

# Non-root user — required by OpenShift's restricted SCC
RUN useradd --uid 1001 --no-create-home --shell /sbin/nologin appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY job_pipeline/ ./job_pipeline/
COPY data/          ./data/

# Directories written at runtime — must be owned by appuser
# On OpenShift these paths are backed by PersistentVolumeClaims
RUN mkdir -p output archives && chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

# MONGO_URI must be injected at runtime via a Secret (see openshift/secret.yaml)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Default: run both pipelines. Override CMD in the CronJob manifest as needed.
CMD ["python", "-m", "job_pipeline.main", "--pipeline", "both"]
