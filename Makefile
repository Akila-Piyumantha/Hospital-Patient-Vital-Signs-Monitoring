# Convenience targets. On Windows without `make`, run the docker compose commands directly
# (see README) - every target below is a one-liner.
COMPOSE ?= docker compose
PY      ?= python

.PHONY: help up down reset logs ps test test-alerts lint format e2e dashboards replay-day dag-runs

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

up: ## build and start the whole stack
	$(COMPOSE) up -d --build

down: ## stop containers (keeps data volumes)
	$(COMPOSE) down

reset: ## stop containers, delete volumes and generated data (simulated time restarts at day 1)
	$(COMPOSE) down -v
	rm -rf data/state data/landing data/lake data/checkpoints data/reports data/alerts data/ground_truth

logs: ## follow logs of the stack
	$(COMPOSE) logs -f --tail=50

ps: ## container status
	$(COMPOSE) ps

test: ## run unit tests
	$(PY) -m pytest

test-alerts: ## unit-test the Prometheus alert rules with promtool (needs Docker)
	docker run --rm --entrypoint promtool -v "$(CURDIR)/observability:/etc/prometheus:ro" \
		prom/prometheus:v2.53.0 test rules /etc/prometheus/alert_rules_test.yml

lint: ## static checks
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

format: ## auto-format
	$(PY) -m ruff format .
	$(PY) -m ruff check --fix .

e2e: ## smoke test against the running stack (see scripts/e2e_smoke.py)
	$(PY) scripts/e2e_smoke.py

dashboards: ## regenerate Grafana dashboard JSON
	$(PY) observability/grafana/build_dashboards.py

replay-day: ## re-run the batch DAG for one simulated day, e.g. make replay-day DAY=3 (idempotent)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger daily_lab_risk_report -c '{"sim_day": $(DAY)}'

dag-runs: ## last runs of the batch DAG
	$(COMPOSE) exec airflow-scheduler airflow dags list-runs -d daily_lab_risk_report -o table
