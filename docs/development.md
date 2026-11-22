# Development

## Setup

```bash
uv sync                      # creates .venv with runtime + dev dependencies
uv run pre-commit install    # optional: ruff + mypy on commit
```

Development and CI use the latest stable Python, pinned in `.python-version`
(currently 3.15). The package still declares `requires-python = ">=3.12"`;
ruff (`target-version = "py312"`) and mypy (`python_version = "3.12"`) keep the
code from using syntax or standard-library APIs newer than 3.12.

## Quality gate

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/        # strict mode
uv run pytest --cov     # unit + contract + MCP tests; branch coverage ≥ 80%
```

`make check` runs all of the above.

## Test layout

| Path                        | What it covers                                       | Needs Kafka |
| --------------------------- | ---------------------------------------------------- | ----------- |
| `tests/unit/domain`         | Entities, topic-name rules                           | no          |
| `tests/unit/application`    | Use cases with fakes                                 | no          |
| `tests/unit/infrastructure` | Settings, client config, error mapping, adapters     | no          |
| `tests/unit/presentation`   | MCP server in-process: tools, schemas, errors, resources, prompt | no |
| `tests/contract`            | Adapters satisfy ports; no write operations          | no          |
| `tests/integration`         | Real broker, plus end-to-end stdio server            | **yes**     |

Async tests use the anyio pytest plugin (`@pytest.mark.anyio`), which comes
with the MCP SDK's own dependency on anyio.

## Integration tests

```bash
docker compose up -d --wait kafka
docker compose run --rm kafka-init     # sample topics + __consumer_offsets
uv run pytest -m integration
```

The fixtures wait up to 90 s for the broker and the sample topics. Set
`STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS` to point the tests at a different broker.

## Local Kafka

`docker-compose.yml` runs `apache/kafka:4.3.2` as a single combined
broker/controller in KRaft mode:

- Host listener: `localhost:9092`
- Listener for other containers: `kafka:19092`
- Data volume: `kafka-data` (removed with `docker compose down -v`)
- `auto.create.topics.enable=true` on purpose, so tests can prove that
  StreamSage never creates topics.

`docker/kafka/init-topics.sh` creates `orders.created` (3 partitions),
`payments.processed` (2) and `inventory.updated` (1). It also briefly joins a
consumer group so that `__consumer_offsets` exists. This dev-only container
performs writes; StreamSage itself does not.
