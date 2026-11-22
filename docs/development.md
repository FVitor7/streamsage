# Development

## Setup

```bash
uv sync                      # creates .venv with runtime + dev dependencies
uv run pre-commit install    # optional: ruff + mypy on commit
```

The minimum supported version is Python 3.12 (`requires-python = ">=3.12"`).
Development and CI run on that minimum, pinned in `.python-version`, so the
supported floor is always the version under test. ruff
(`target-version = "py312"`) and mypy (`python_version = "3.12"`) target the
same version.

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
| `tests/security`            | SASL/SCRAM and TLS against a secured broker, plus stdio over SASL_SSL | **yes** (secured) |

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

## Security tests (SASL/SCRAM + TLS)

```bash
docker/kafka-secure/setup.sh       # or: make kafka-secure-up
uv run pytest -m security          # or: make test-security
```

`setup.sh` starts a second, dev-only broker (compose profile `security`):

| Port             | Protocol         | Authentication                        |
| ---------------- | ---------------- | ------------------------------------- |
| `localhost:9094` | `SASL_PLAINTEXT` | SCRAM-SHA-512                         |
| `localhost:9095` | `SASL_SSL`       | SCRAM-SHA-512 over TLS (test-only CA) |

On the first run, it generates everything into `docker/kafka-secure/.generated/`
(gitignored): a random SCRAM password for the user `streamsage`, a throwaway
CA (`ca.pem`), and the broker keystore. The certificates are created with the
`keytool` inside the Kafka image, so the host only needs Docker and
`openssl`. CI regenerates all of it on every run. To start from scratch,
delete the folder and run `docker compose --profile security down -v`.

The suite checks that:

- valid credentials work on both listeners, with TLS verified against the
  configured CA;
- a wrong password is reported as an authentication failure, and an
  untrusted certificate as a TLS failure, instead of a generic connection
  error;
- no password appears in errors or logs, including librdkafka's own logs;
- the real `streamsage` process works end to end over stdio with `SASL_SSL`.

## Local Kafka

`docker-compose.yml` runs `apache/kafka:4.3.2` as a single combined
broker/controller in KRaft mode:

- Host listener: `localhost:9092`
- Listener for other containers: `kafka:19092`
- Data volume: `kafka-data` (removed with `docker compose down -v`)
- Web UI: [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) at
  http://localhost:8080, useful for checking what the MCP tools report
- `auto.create.topics.enable=true` on purpose, so tests can prove that
  StreamSage never creates topics.

`docker/kafka/init-topics.sh` creates `orders.created` (3 partitions),
`payments.processed` (2) and `inventory.updated` (1). It also briefly joins a
consumer group so that `__consumer_offsets` exists. This dev-only container
performs writes; StreamSage itself does not.
