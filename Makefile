.PHONY: install lint format typecheck test test-integration test-security check kafka-up kafka-down kafka-logs kafka-secure-up kafka-secure-down run inspector

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy src/

test:
	uv run pytest --cov

test-integration:
	uv run pytest -m integration

test-security:
	uv run pytest -m security

check: lint typecheck test

kafka-up:
	docker compose up -d --wait kafka
	docker compose run --rm kafka-init
	docker compose up -d kafka-consumer kafka-ui

kafka-down:
	docker compose down

kafka-logs:
	docker compose logs -f kafka

kafka-secure-up:
	docker/kafka-secure/setup.sh

kafka-secure-down:
	docker compose --profile security down

run:
	uv run streamsage

inspector:
	npx @modelcontextprotocol/inspector uv run --directory "$(CURDIR)" streamsage
