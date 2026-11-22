.PHONY: install lint format typecheck test test-integration check kafka-up kafka-down kafka-logs run inspector

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

check: lint typecheck test

kafka-up:
	docker compose up -d --wait kafka
	docker compose run --rm kafka-init

kafka-down:
	docker compose down

kafka-logs:
	docker compose logs -f kafka

run:
	uv run streamsage

inspector:
	npx @modelcontextprotocol/inspector uv run --directory "$(CURDIR)" streamsage
