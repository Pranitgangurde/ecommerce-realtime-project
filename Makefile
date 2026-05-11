.PHONY: help install up down logs ps clean produce consume test lint format check

help:
	@echo "Available commands:"
	@echo "  make install       - Install Python deps with uv"
	@echo "  make up            - Start all infra (Kafka, Postgres, Metabase)"
	@echo "  make down          - Stop all infra"
	@echo "  make logs          - Tail logs from all services"
	@echo "  make ps            - Show running containers"
	@echo "  make produce       - Run event generator"
	@echo "  make consume       - Run Spark consumer"
	@echo "  make clean         - Remove containers + volumes (⚠️  data loss)"
	@echo "  make test          - Run pytest"
	@echo "  make lint          - Run ruff + mypy"
	@echo "  make format        - Auto-format code with ruff"
	@echo "  make check         - Lint + test (CI parity)"

install:
	uv sync --all-extras

up:
	docker compose -f infra/docker/docker-compose.yml up -d
	@echo ""
	@echo "✅ Services starting. Useful URLs:"
	@echo "   Kafka UI:    http://localhost:8080"
	@echo "   Metabase:    http://localhost:3000"
	@echo "   Postgres OLTP:      localhost:5432  (ecommerce_user/changeme_local_only)"
	@echo "   Postgres Analytics: localhost:5433  (analytics_user/changeme_local_only)"

down:
	docker compose -f infra/docker/docker-compose.yml down

logs:
	docker compose -f infra/docker/docker-compose.yml logs -f --tail=50

ps:
	docker compose -f infra/docker/docker-compose.yml ps

clean:
	docker compose -f infra/docker/docker-compose.yml down -v
	rm -rf /tmp/spark-checkpoints

produce:
	uv run python -m src.producers.event_generator.main

consume:
	uv run spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.postgresql:postgresql:42.7.3 \
		src/batch/spark_jobs/clickstream_consumer.py

test:
	uv run pytest -v --cov=src --cov-report=term-missing

lint:
	uv run ruff check src tests
	uv run mypy src

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

check: lint test