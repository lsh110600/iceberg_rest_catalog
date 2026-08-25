FROM python:3.13.7-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN groupadd --system icebergops && useradd --system --gid icebergops icebergops
COPY requirements.txt .
COPY pyproject.toml .
RUN pip install --no-cache-dir -r requirements.txt

COPY alembic.ini ./
COPY alembic alembic
COPY app app

FROM base AS test
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY tests tests
CMD ["pytest", "-q"]

FROM base AS runtime
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh && chown -R icebergops:icebergops /app

USER icebergops
EXPOSE 8080
ENTRYPOINT ["/app/docker-entrypoint.sh"]
