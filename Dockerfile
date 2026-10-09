# Local, reproducible runtime: Python 3.11 + OpenJDK 17 + PySpark.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=UTC

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 app
WORKDIR /app

COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
RUN pip install -r requirements.txt && pip install --no-deps .

COPY conf ./conf
COPY tests ./tests
RUN chown -R app:app /app
USER app

# Default: generate synthetic data, then run the pipeline (Parquet, fully offline).
CMD ["sh", "-c", "python -m lakehouse.generate --out data/landing && python -m lakehouse.pipeline --config conf/pipeline.toml"]
