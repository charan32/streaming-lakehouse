# Local mode runs without Docker: Iceberg catalog in SQLite, files in ./.local.
LOCAL_DIR := $(CURDIR)/.local
export PYICEBERG_CATALOG__LAKEHOUSE__TYPE ?= sql
export PYICEBERG_CATALOG__LAKEHOUSE__URI ?= sqlite:///$(LOCAL_DIR)/catalog.db
export PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE ?= file://$(LOCAL_DIR)/warehouse
PY ?= python
SESSIONS ?= 3000

.PHONY: install test local ingest-local dbt-local seed up down logs clean

install:            ## Install Python and dbt dependencies
	$(PY) -m pip install -r requirements-dev.txt

test:               ## Unit and integration tests
	$(PY) -m pytest -q

local: clean ingest-local dbt-local   ## Full pipeline without Docker: generate -> Iceberg -> dbt

ingest-local:
	mkdir -p $(LOCAL_DIR)/warehouse
	$(PY) -m lakehouse.local_run --sessions $(SESSIONS)

dbt-local:
	cd dbt && dbt build --profiles-dir . --target dev
	$(PY) -m lakehouse.health --window-minutes 1000000

seed:               ## Regenerate dbt/seeds/products.csv from the product catalog
	$(PY) -m lakehouse.catalog_data dbt/seeds/products.csv

up:                 ## Start the full streaming stack in Docker
	docker compose up -d --build

down:               ## Stop the stack and delete its data
	docker compose down -v

logs:
	docker compose logs -f ingestor producer

clean:
	rm -rf $(LOCAL_DIR) dbt/target dbt/logs
