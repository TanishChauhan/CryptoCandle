.PHONY: help install dev-install test lint up down logs ps db-init

help:
	@echo "Targets:"
	@echo "  install       Install runtime dependencies"
	@echo "  dev-install   Install runtime + dev dependencies"
	@echo "  test          Run unit tests (no Docker required)"
	@echo "  lint          Run ruff linter"
	@echo "  up            Start full Docker stack"
	@echo "  down          Stop Docker stack"
	@echo "  ps            Show compose service status"
	@echo "  db-init       Apply analytics schema to Postgres (existing volumes)"

install:
	pip install -e .

dev-install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check shared tests services dashboard

up:
	docker compose --env-file .env up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f producer spark-streaming

db-init:
	docker compose exec -T postgres psql -U crypto -d crypto_analytics -f /schema/init.sql
