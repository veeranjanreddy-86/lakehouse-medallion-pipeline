PYTHON ?= python3.11
VENV   ?= .venv
BIN    := $(VENV)/bin
export TZ := UTC

.PHONY: help venv install lint format test generate run run-delta rerun clean docker-build docker-run

help:  ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

venv:  ## Create a local virtualenv
	$(PYTHON) -m venv $(VENV)

install: venv  ## Install runtime + dev dependencies (editable)
	$(BIN)/pip install -q --upgrade pip
	$(BIN)/pip install -q -r requirements.txt
	$(BIN)/pip install -q -e . --no-deps

lint:  ## Ruff lint + format check
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format:  ## Auto-format with ruff
	$(BIN)/ruff check --fix .
	$(BIN)/ruff format .

test:  ## Run the pytest suite (local Spark)
	$(BIN)/pytest

generate:  ## Generate synthetic landing data (seeded)
	$(BIN)/python -m lakehouse.generate --out data/landing --seed 42

run:  ## Run the pipeline on data/landing (Parquet)
	$(BIN)/python -m lakehouse.pipeline --config conf/pipeline.toml

run-delta:  ## Run the pipeline writing Delta tables (needs Delta JARs)
	$(BIN)/python -m lakehouse.pipeline --config conf/pipeline.toml --format delta --base-dir data/lakehouse_delta

rerun: run  ## Re-run to demonstrate idempotency (no new files -> no changes)

clean:  ## Remove generated data and caches
	rm -rf data spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

docker-build:  ## Build the container image
	docker build -t lakehouse-medallion-pipeline .

docker-run:  ## Generate + run inside the container
	docker run --rm lakehouse-medallion-pipeline
