# StreamSage — Technical Specification and Implementation Guide

> **About this document.** This is the original specification StreamSage was
> built from, translated from Portuguese. It is kept as a historical record:
> statuses such as "next milestone" reflect the time it was written. All five
> milestones were delivered in [v1.0.0](CHANGELOG.md). Where the
> implementation deliberately deviated from this text (for example, moving
> `detect_stalled_partitions` from M2 to M3), the decision and its rationale
> are recorded in [docs/architecture.md](docs/architecture.md).

**Document:** `PROJECT_SPEC.md`\
**Specification version:** 1.0\
**Status:** Approved for M1 implementation\
**Project:** `FVitor7/streamsage`\
**Name:** StreamSage\
**Tagline:** AI-powered Kafka Observability & Troubleshooting MCP Server\
**Main language:** Python 3.12+\
**Initial broker:** Apache Kafka\
**Architecture:** Clean Architecture + Hexagonal Architecture (Ports and
Adapters)\
**Initial operating model:** Read-only\
**Target audience:** Backend developers, platform engineers and SREs who use
Kafka and MCP-compatible clients.

------------------------------------------------------------------------

# 1. Instructions for Claude Code

This document is the source of truth for implementing StreamSage. Before
changing or creating files:

1.  Read this document in full.
2.  Inspect the current state of the repository before overwriting files.
3.  Follow the architectural decisions and scope limits defined here.
4.  Implement in small, verifiable milestones.
5.  Do not invent MCP SDK APIs. Consult the official documentation and
    validate the API of the version actually installed.
6.  Do not introduce dependencies, abstractions or features outside the
    scope without explaining the need and proposing the change.
7.  Do not implement write operations on Kafka.
8.  Do not put credentials, sensitive data or `.env` files in Git.
9.  Write tests for every relevant behavior.
10. When finishing each milestone, report the files created/changed,
    commands run, test results, limitations and next steps.

## Implementation guideline

The priority is to deliver a small, functional, well-tested and
architecturally correct application. Avoid overengineering. The
architecture must allow evolution, but must not create empty layers or
abstractions without real use.

When an example in this document conflicts with the current official API
of a dependency, the official documentation and the architectural contracts
of this document prevail. Update the code and record the decision.

------------------------------------------------------------------------

# 2. Product vision

StreamSage is an open-source MCP server in Python that lets AI agents and
other MCP-compatible clients explore Apache Kafka clusters, query metadata
and investigate operational problems.

The goal is not only to expose Kafka calls as MCP tools. The product must
offer an application layer and, in later phases, deterministic,
evidence-based diagnostics.

## 2.1 Problem

Information that matters for operating Kafka is spread across topics,
partitions, brokers, consumer groups, offsets, configurations and
command-line tools. Investigations require several queries and operational
knowledge.

StreamSage must offer a consistent interface for those queries, letting an
agent coordinate the tools and present well-founded conclusions.

## 2.2 Example questions the product should support

-   What topics are available in this cluster?
-   Which consumer groups have the highest lag?
-   Is any consumer group experiencing stalled partitions?
-   Which partitions are accumulating the most messages?
-   What is the current metadata health of my Kafka cluster?
-   Can you investigate the `orders.created` topic?

## 2.3 Product principles

-   Read-only by design.
-   Evidence-based diagnostics.
-   Broker-specific code isolated in adapters.
-   Domain independent of MCP and Kafka.
-   Typed contracts and input validation.
-   Explicit timeouts and controlled errors.
-   Security through least privilege.
-   Unit tests without a broker and integration tests with a real Kafka.
-   Reproducible documentation and examples.

------------------------------------------------------------------------

# 3. Scope and approved decisions

These decisions have been made and must be respected in the MVP.

| Topic | Decision |
| --- | --- |
| Name | StreamSage |
| Repository | `FVitor7/streamsage` |
| Language | Python |
| Minimum version | Python 3.12 |
| Initial broker | Apache Kafka |
| Redpanda | Out of initial scope |
| Amazon MSK | Out of initial scope; may be supported through a Kafka-compatible configuration in the future |
| Protocol | MCP |
| SDK | Official MCP SDK for Python |
| Initial transport | stdio |
| Streamable HTTP | Future evolution |
| Architecture | Clean Architecture + Ports and Adapters |
| Kafka client | `confluent-kafka` |
| Configuration | Pydantic Settings + environment variables |
| Dependency manager | uv |
| Tests | pytest |
| Quality | Ruff, mypy, pytest-cov, GitHub Actions |
| Local environment | Docker Compose + Kafka in KRaft mode |
| Internal persistence | None in the MVP |
| Own LLM | No |
| LangChain/LangGraph | No |
| Kafka operations | Read-only |
| Documentation | English in the public repository |
| Proposed license | MIT |

## 3.1 Out of MVP scope

-   Own web interface.
-   Internal database.
-   Writing/publishing messages.
-   Creating or deleting topics.
-   Changing offsets.
-   Changing cluster configuration.
-   Direct integration with OpenAI, Gemini, Anthropic or another LLM.
-   Continuous background monitoring.
-   Alerting system.
-   Specific adapters for Redpanda or other brokers.
-   LLM-based diagnostics inside the server.

The MCP client/LLM is responsible for interpreting and combining the
results. The server provides data and deterministic diagnostics.

------------------------------------------------------------------------

# 4. Architecture

Adopt Clean Architecture combined with Hexagonal Architecture (Ports and
Adapters).

## 4.1 Layers

### Presentation

Responsible for the MCP protocol and for exposing:

-   Tools.
-   Resources.
-   Prompts.

It must not contain business rules or access the Kafka client directly.

### Application

Responsible for:

-   Use cases.
-   Orchestration.
-   Application DTOs.
-   Dependencies through Ports.

Use cases must not import infrastructure classes or the MCP SDK.

### Domain

Responsible for:

-   Entities.
-   Value Objects.
-   Business rules.
-   Diagnostics and severities.
-   Domain exceptions.

The domain cannot depend on Kafka, MCP, Pydantic or infrastructure
libraries.

### Infrastructure

Responsible for:

-   Port implementations.
-   Kafka client.
-   Configuration.
-   External integrations.
-   Converting external models into domain models.

## 4.2 Logical diagram

``` text
AI Clients
  Claude Desktop / Cursor / Other MCP Clients
                 |
                 | MCP (stdio initially)
                 v
Presentation
  Tools / Resources / Prompts
                 |
                 v
Application
  Use Cases / DTOs / Orchestration
                 |
          +------+------+
          |             |
          v             v
        Domain        Ports
      Entities       Interfaces
      Value Objects
      Diagnostic Rules
                        |
                        v
                 Infrastructure
                 Kafka Adapters
                 confluent-kafka
                        |
                        v
                  Apache Kafka
```

## 4.3 Dependency rule

Dependencies must point inward:

-   Presentation depends on Application.
-   Application depends on Domain and declares Ports.
-   Infrastructure implements Ports and depends on Application/Domain.
-   Domain depends on no outer layer.

The Composition Root (`main.py` or an equivalent module) is the place that
instantiates the concrete implementations and injects the dependencies.

## 4.4 Sync versus async decision

The `confluent-kafka` client and its administrative operations are
synchronous. In M1, keep the Kafka operations synchronous and configure
explicit timeouts.

Do not add `asyncio.to_thread`, queues or concurrency complexity without a
demonstrated need. If the MCP API requires asynchronous handlers, adapt the
presentation boundary without contaminating the domain and application.

------------------------------------------------------------------------

# 5. Directory structure

Target structure. Create only the modules needed in each milestone; do not
create empty files just to reproduce the tree.

``` text
streamsage/
├── src/
│   └── streamsage/
│       ├── __init__.py
│       ├── main.py
│       ├── presentation/
│       │   └── mcp/
│       │       ├── __init__.py
│       │       ├── server.py
│       │       ├── dependencies.py
│       │       ├── tools/
│       │       │   ├── cluster_tools.py
│       │       │   ├── topic_tools.py
│       │       │   └── consumer_tools.py
│       │       ├── resources/
│       │       │   └── cluster_resources.py
│       │       └── prompts/
│       │           └── diagnostics_prompts.py
│       ├── application/
│       │   ├── ports/
│       │   │   ├── cluster_reader.py
│       │   │   ├── topic_reader.py
│       │   │   └── consumer_group_reader.py
│       │   ├── use_cases/
│       │   │   ├── get_cluster_info.py
│       │   │   ├── list_topics.py
│       │   │   └── describe_topic.py
│       │   └── dto/
│       │       ├── cluster_dto.py
│       │       └── topic_dto.py
│       ├── domain/
│       │   ├── entities/
│       │   │   ├── cluster.py
│       │   │   ├── topic.py
│       │   │   ├── partition.py
│       │   │   └── consumer_group.py
│       │   ├── value_objects/
│       │   │   ├── cluster_id.py
│       │   │   └── lag.py
│       │   ├── diagnostics/
│       │   │   ├── finding.py
│       │   │   └── severity.py
│       │   └── exceptions.py
│       ├── infrastructure/
│       │   ├── kafka/
│       │   │   ├── admin_client.py
│       │   │   ├── kafka_cluster_adapter.py
│       │   │   ├── kafka_topic_adapter.py
│       │   │   └── kafka_consumer_adapter.py
│       │   └── config/
│       │       └── settings.py
│       └── shared/
│           ├── logging.py
│           └── errors.py
├── tests/
│   ├── unit/
│   │   ├── domain/
│   │   ├── application/
│   │   └── diagnostics/
│   ├── integration/
│   │   └── kafka/
│   └── contract/
├── docker/
│   └── kafka/
│       └── init-topics.sh
├── docs/
│   ├── architecture.md
│   ├── security.md
│   └── development.md
├── examples/
│   └── claude_desktop.json
├── .github/
│   └── workflows/
│       └── ci.yml
├── .env.example
├── .gitignore
├── .python-version
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── pyproject.toml
├── uv.lock
├── README.md
├── LICENSE
└── PROJECT_SPEC.md
```

------------------------------------------------------------------------

# 6. Domain models

Domain models must be pure Python, preferably immutable `dataclasses` where
appropriate. Do not use Pydantic models as domain entities.

Conceptual examples:

## 6.1 Cluster and Broker

``` python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Broker:
    broker_id: int
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class Cluster:
    cluster_id: str
    brokers: tuple[Broker, ...]
    controller_id: int | None
```

## 6.2 Topic and Partition

``` python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Partition:
    partition_id: int
    leader: int | None
    replicas: tuple[int, ...]
    isr: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Topic:
    name: str
    partitions: tuple[Partition, ...]
    internal: bool = False
```

## 6.3 Partition Lag

``` python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PartitionLag:
    topic: str
    partition: int
    committed_offset: int | None
    log_end_offset: int | None
    lag: int | None
```

**Important rule:** a missing committed offset, or missing information
needed to compute lag, must be represented as `None`, never silently
converted to zero.

------------------------------------------------------------------------

# 7. Application Ports

Use `typing.Protocol` to define structural contracts.

## 7.1 ClusterReader

``` python
from typing import Protocol
from streamsage.domain.entities.cluster import Cluster


class ClusterReader(Protocol):
    def get_cluster_info(self) -> Cluster:
        ...
```

## 7.2 TopicReader

``` python
from typing import Protocol
from streamsage.domain.entities.topic import Topic


class TopicReader(Protocol):
    def list_topics(self) -> list[Topic]:
        ...

    def describe_topic(self, name: str) -> Topic:
        ...
```

## 7.3 ConsumerGroupReader

``` python
from typing import Protocol

from streamsage.domain.entities.consumer_group import ConsumerGroup
from streamsage.domain.value_objects.lag import PartitionLag


class ConsumerGroupReader(Protocol):
    def list_consumer_groups(self) -> list[ConsumerGroup]:
        ...

    def describe_consumer_group(
        self,
        group_id: str,
    ) -> ConsumerGroup:
        ...

    def get_consumer_lag(
        self,
        group_id: str,
    ) -> list[PartitionLag]:
        ...
```

Create `ConsumerGroup` and the other models when M2 starts. Implementing the
whole contract in M1 is not required.

------------------------------------------------------------------------

# 8. Application Use Cases

Use cases must be small, explicit and testable with mocks/fakes.

Example:

``` python
from streamsage.application.ports.topic_reader import TopicReader
from streamsage.domain.entities.topic import Topic


class ListTopicsUseCase:
    def __init__(self, topic_reader: TopicReader):
        self._topic_reader = topic_reader

    def execute(self) -> list[Topic]:
        return self._topic_reader.list_topics()
```

Another example:

``` python
from streamsage.application.ports.cluster_reader import ClusterReader
from streamsage.domain.entities.cluster import Cluster


class GetClusterInfoUseCase:
    def __init__(self, cluster_reader: ClusterReader):
        self._cluster_reader = cluster_reader

    def execute(self) -> Cluster:
        return self._cluster_reader.get_cluster_info()
```

Use cases must not import `confluent_kafka`, the MCP SDK or configuration
classes.

------------------------------------------------------------------------

# 9. Kafka infrastructure

## 9.1 Library

Use `confluent-kafka` (Python client based on librdkafka).

The adapter must encapsulate the library's particularities and convert
external results into domain models.

## 9.2 AdminClient Factory

Responsibilities:

-   Create the AdminClient.
-   Receive Settings by injection.
-   Configure bootstrap servers.
-   Configure timeouts.
-   Configure SASL/TLS security when applicable.
-   Avoid exposing secrets in logs or errors.
-   Not spread client creation across the application.

Conceptual example:

``` python
from confluent_kafka.admin import AdminClient


class KafkaAdminClientFactory:
    def __init__(self, settings):
        self._settings = settings

    def create(self) -> AdminClient:
        config = {
            "bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "client.id": "streamsage",
            "security.protocol": self._settings.kafka_security_protocol,
            "socket.timeout.ms": int(
                self._settings.kafka_socket_timeout * 1000
            ),
            "request.timeout.ms": int(
                self._settings.kafka_request_timeout * 1000
            ),
            "allow.auto.create.topics": False,
        }

        # Add SASL/TLS configuration only when needed.
        # Never log username/password or certificate material.

        return AdminClient(config)
```

Check the names and units of the configuration properties supported by the
`confluent-kafka` version actually installed. Do not assume every field in
the example is valid in every version.

## 9.3 Kafka Adapters

Implement:

-   `KafkaClusterAdapter` — implements `ClusterReader`.
-   `KafkaTopicAdapter` — implements `TopicReader`.
-   `KafkaConsumerGroupAdapter` — implements `ConsumerGroupReader`, in M2.

Adapters must:

-   Convert Kafka models into domain models.
-   Normalize library errors into application/domain exceptions.
-   Define timeouts.
-   Handle incomplete metadata.
-   Avoid write operations.
-   Not return internal Kafka library objects to the upper layers.

### Beware of accidental topic creation

Do not use queries targeted at a non-existent topic name as a validation
mechanism without understanding the broker's semantics and configuration.
In some scenarios/configurations, Kafka queries can interact with
auto-creation.

For `describe_topic`, prefer querying known metadata and validating
existence without triggering creation. Test this behavior with the local
broker and document the guarantee.

------------------------------------------------------------------------

# 10. MCP Presentation Layer

## 10.1 SDK

Use the official MCP SDK for Python. Before implementing:

-   Consult the current official documentation.
-   Confirm the available stable version.
-   Confirm the API for creating the server, registering
    tools/resources/prompts and running stdio.
-   Pin a compatible version range in `pyproject.toml` and record the exact
    resolution in `uv.lock`.
-   Do not copy examples from old versions without validation.

MCP must be a thin layer: receive parameters, validate inputs, call use
cases and serialize results.

## 10.2 M1 Tools

| Tool | Input | Result |
| --- | --- | --- |
| `get_cluster_info` | None | ID, brokers and controller when available |
| `list_topics` | `include_internal: bool = false` | List of topics |
| `describe_topic` | `topic_name: str` | Partitions, leader, replicas and ISR |

All tools must have clear descriptions, typed parameters and structured
responses.

## 10.3 Tools planned for M2

| Tool | Input | Result |
| --- | --- | --- |
| `list_consumer_groups` | Optional filters | Consumer groups |
| `describe_consumer_group` | `group_id` | State and members |
| `get_consumer_lag` | `group_id` | Lag per topic/partition |
| `analyze_consumer_health` | `group_id` | Evidence-based classification |
| `detect_stalled_partitions` | `group_id` | Partitions without progress, if there are enough snapshots |

## 10.4 M1 Resources

-   `kafka://cluster/info`
-   `kafka://topics`

Resources must provide queryable data and must not unnecessarily duplicate
the rules of the tools. Reuse use cases/application services.

## 10.5 M1 Prompt

`investigate_kafka_cluster`

The prompt guides the client to:

1.  Query cluster metadata.
2.  List topics.
3.  Investigate relevant details.
4.  Distinguish observed data from hypotheses.
5.  Not claim complete cluster health based only on limited metadata.

## 10.6 Transport

M1: stdio.

Future evolution: Streamable HTTP.

Do not implement HTTP in M1.

------------------------------------------------------------------------

# 11. Configuration

Use Pydantic Settings and environment variables.

`.env.example`:

``` dotenv
STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

STREAMSAGE_KAFKA_SECURITY_PROTOCOL=PLAINTEXT
STREAMSAGE_KAFKA_SASL_MECHANISM=
STREAMSAGE_KAFKA_SASL_USERNAME=
STREAMSAGE_KAFKA_SASL_PASSWORD=

STREAMSAGE_KAFKA_REQUEST_TIMEOUT=10
STREAMSAGE_KAFKA_SOCKET_TIMEOUT=10

STREAMSAGE_LOG_LEVEL=INFO
STREAMSAGE_INCLUDE_INTERNAL_TOPICS=false
```

Initial model:

``` python
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STREAMSAGE_",
        env_file=".env",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"

    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None

    kafka_request_timeout: float = 10
    kafka_socket_timeout: float = 10

    log_level: str = "INFO"
    include_internal_topics: bool = False
```

Requirements:

-   Validate inconsistent configurations.
-   Do not log secrets.
-   Support PLAINTEXT for local development only.
-   Document SASL/TLS configuration.
-   Never version `.env`.
-   Use `SecretStr` for secrets.
-   Do not allow the MCP client to supply arbitrary credentials in each tool
    call.

------------------------------------------------------------------------

# 12. Security

## 12.1 Read-only by design

Allowed:

-   Query cluster metadata.
-   List and describe topics.
-   Query consumer groups.
-   Query offsets and lag.
-   Inspect messages within a limited scope in a future milestone.

Forbidden in the MVP:

-   Creating or deleting topics.
-   Publishing messages.
-   Changing offsets.
-   Changing cluster configuration.
-   Running write administrative operations.

The read-only policy must be enforced both in code and in Kafka
ACLs/credentials.

## 12.2 Principle of least privilege

The Kafka credential must have only the permissions needed for the queries
made available. Do not rely only on the absence of write methods in the
code.

## 12.3 Data protection

-   Do not log message payloads by default.
-   Do not log passwords, tokens or certificates.
-   Limit the number and size of inspected messages.
-   Sanitize sensitive information.
-   Define timeouts and limits for queries.
-   Do not return internal stack traces to MCP clients.
-   Do not expose secret configuration in Resources.

## 12.4 MCP

-   Validate all tool arguments.
-   Do not allow inputs to control shell commands.
-   Do not accept arbitrary file paths.
-   Clearly document the transport and trust considerations.
-   If Streamable HTTP is implemented, add authentication and access
    controls before making it available outside a local environment.

------------------------------------------------------------------------

# 13. Dependencies

Use `uv` for management and the lockfile.

## 13.1 Production

-   `mcp[cli]` — official MCP SDK and CLI, according to the current
    distribution/documentation.
-   `confluent-kafka` — Kafka client.
-   `pydantic` — validation and boundary models.
-   `pydantic-settings` — configuration.

## 13.2 Development

-   `pytest`
-   `pytest-cov`
-   `pytest-asyncio` — only where there is asynchronous code that needs
    tests.
-   `ruff`
-   `mypy`
-   `pre-commit`
-   `testcontainers` — optional for integration tests; evaluate whether
    Docker Compose is simpler for CI.

## 13.3 Do not add in the MVP

-   LangChain.
-   LangGraph.
-   LLM provider SDKs.
-   Database.
-   Additional web framework.
-   Own queue system.

## 13.4 Initial pyproject.toml

The ranges below are initial proposals. Validate the availability and
compatibility of the versions at implementation time. Do not assume a
future or preview version is stable.

``` toml
[project]
name = "streamsage"
version = "0.1.0"
description = "AI-powered Kafka observability and troubleshooting MCP server"
readme = "README.md"
requires-python = ">=3.12"
license = { text = "MIT" }

dependencies = [
    "mcp[cli]>=2.0,<3.0",
    "confluent-kafka>=2.0,<3.0",
    "pydantic>=2.0,<3.0",
    "pydantic-settings>=2.0,<3.0",
]

[project.scripts]
streamsage = "streamsage.main:main"

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-cov>=6.0",
    "pytest-asyncio>=1.0",
    "ruff>=0.14",
    "mypy>=1.0",
    "pre-commit>=4.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/streamsage"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"

[tool.ruff]
line-length = 88
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.mypy]
python_version = "3.12"
strict = true
mypy_path = "src"

[tool.coverage.run]
branch = true
source = ["streamsage"]

[tool.coverage.report]
show_missing = true
fail_under = 80
```

Note: the MCP package range must be adjusted after checking the stable
version and its official API. If the official package does not have the
proposed range, correct it to the real range and record the decision. Do
not force a version that does not exist.

------------------------------------------------------------------------

# 14. Local Kafka environment

Use Apache Kafka in KRaft mode, without ZooKeeper.

M1:

-   One broker.
-   External port `9092`.
-   Appropriate internal address for containers.
-   Healthcheck.
-   Persistent volume.
-   Reproducible configuration via Docker Compose.

M1 does not need to simulate high availability. In a future milestone,
create an environment with three brokers to test replication, ISR and
partition distribution.

`docker-compose.yml` must use an official/trusted Kafka image, an explicit
tag and a documented KRaft configuration. Avoid `latest` for reproducible
builds.

Expected commands:

``` bash
docker compose up -d
docker compose ps
docker compose logs -f kafka
docker compose down
```

The integration test must wait for the broker to become healthy before
running queries.

------------------------------------------------------------------------

# 15. Composition Root

The composition point instantiates Settings, factories, adapters, use cases
and the MCP server.

Conceptual example:

``` python
def build_application():
    settings = Settings()

    factory = KafkaAdminClientFactory(settings)
    admin_client = factory.create()

    topic_reader = KafkaTopicAdapter(admin_client)
    cluster_reader = KafkaClusterAdapter(admin_client)

    list_topics = ListTopicsUseCase(topic_reader)
    get_cluster_info = GetClusterInfoUseCase(cluster_reader)

    return create_server(
        list_topics_use_case=list_topics,
        get_cluster_info_use_case=get_cluster_info,
    )
```

`main.py` must start the server on the stdio transport in M1.

A clear lifecycle for clients/connections and shutdown must also be
defined. Do not leave resources unmanaged.

------------------------------------------------------------------------

# 16. Testing strategy

## 16.1 Unit tests

Test without Kafka:

-   Entities and Value Objects.
-   Domain rules.
-   Use cases using mocks/fakes.
-   Pure conversions and validations.
-   Error normalization.

## 16.2 Integration tests

Run against a real Kafka:

-   Connection and metadata reads.
-   Topic listing.
-   Topic description.
-   Handling an unavailable broker.
-   Behavior for a non-existent topic.
-   Filtering of internal topics.

## 16.3 Contract tests

Verify that the adapters implement the expected Ports and that the
input/output contracts remain consistent.

## 16.4 MCP tests

-   Server startup.
-   Tool registration.
-   Argument validation.
-   Execution of each tool.
-   Response format.
-   Error handling without leaking internal details.

## 16.5 Coverage

Initial target: 80% branch coverage in domain and application code, without
chasing artificial coverage in generated or trivial integration code.

------------------------------------------------------------------------

# 17. CI/CD

GitHub Actions must run on Pull Requests and relevant pushes:

``` bash
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest --cov
```

Add an integration job that starts Kafka and runs the integration tests.

CI must:

-   Use a supported Python version.
-   Use the lockfile.
-   Not use real credentials.
-   Publish test results.
-   Fail on lint, type checking or tests.
-   Not publish the package automatically in M1.

------------------------------------------------------------------------

# 18. Milestones

## M1 — Foundation & Cluster Explorer

**Status:** Next milestone to implement.

Goal: prove the end-to-end flow between an MCP client, the Python
application and a real Kafka.

### Deliverables

1.  Initialize the repository.
2.  Configure `pyproject.toml`, `uv` and the lockfile.
3.  Create Settings.
4.  Create the Kafka AdminClient Factory.
5.  Create Docker Compose with Kafka KRaft.
6.  Create the Cluster, Broker, Topic and Partition entities.
7.  Create the ClusterReader and TopicReader Ports.
8.  Create KafkaClusterAdapter and KafkaTopicAdapter.
9.  Create the use cases.
10. Create the MCP server.
11. Register three tools.
12. Register resources and the initial prompt.
13. Create unit and integration tests.
14. Create the CI workflow.
15. Document local execution.

### M1 Tools

-   `get_cluster_info`
-   `list_topics`
-   `describe_topic`

### M1 Resources

-   `kafka://cluster/info`
-   `kafka://topics`

### M1 Prompt

-   `investigate_kafka_cluster`

### Acceptance criteria

-   The server starts without errors.
-   The connection to the local Kafka works.
-   `get_cluster_info` returns the available metadata.
-   `list_topics` returns topics.
-   Internal topics are filtered by default.
-   `describe_topic` returns partitions, leader, replicas and ISR.
-   A non-existent topic produces a controlled error.
-   An unavailable broker produces a controlled error, bounded by a timeout.
-   Unit tests do not depend on Kafka.
-   Integration tests work with a real Kafka.
-   The MCP Inspector can run the three tools.
-   No write operation is exposed.
-   Secrets do not appear in logs or responses.

## M2 — Consumer Intelligence

-   List and describe consumer groups.
-   Query committed offsets.
-   Query log end offsets.
-   Compute lag per partition and group.
-   Sort partitions by lag.
-   Treat a missing committed offset as missing data, not zero.
-   Create an initial health classification with configurable limits.

## M3 — Diagnostics Engine

-   Structured findings.
-   Configurable severity.
-   Evidence.
-   Hypotheses and recommendations.
-   Insufficient data explicitly flagged.
-   Detection of partitions without progress by comparing snapshots.
-   Do not claim a root cause without sufficient evidence.

**Rule:** high lag does not necessarily mean a stuck consumer. To detect
absence of progress, compare offsets at more than one point in time.

## M4 — Message Intelligence

-   Limited message inspection.
-   Metadata, key, headers, timestamp and offset.
-   Search by key within defined limits.
-   Configurable DLQ, without assuming a universal convention.
-   Quantity and size limits.
-   Payload sanitization.

## M5 — Production Readiness

-   Streamable HTTP.
-   Authentication and authorization for HTTP.
-   TLS/SASL.
-   Structured logging.
-   Server metrics.
-   Security documentation.
-   Release v1.0.

------------------------------------------------------------------------

# 19. README and public presentation

The public repository must have documentation in English.

Suggested README structure:

1.  Project overview.
2.  Problem statement.
3.  Features.
4.  Demo GIF/video.
5.  Architecture diagram.
6.  Requirements.
7.  Quick start.
8.  Kafka configuration.
9.  MCP client configuration.
10. Available tools/resources/prompts.
11. Security model.
12. Development and tests.
13. Roadmap.
14. Contributing.
15. License.

The demo should show an MCP client running tools against a real local
Kafka. Do not present fictional data as if it were real results.

Suggested GitHub description:

`An open-source MCP server for Kafka observability, diagnostics and troubleshooting using AI agents.`

Repository:

`FVitor7/streamsage`

Proposed license: MIT.

------------------------------------------------------------------------

# 20. Engineering rules for Claude Code

When implementing:

-   Prefer small functions and classes.
-   Use type hints.
-   Do not use `Any` without justification.
-   Do not catch `Exception` generically without re-raising/normalizing it
    correctly.
-   Do not return Kafka library objects outside the adapters.
-   Do not import MCP in Application or Domain.
-   Do not import Kafka in Domain.
-   Do not put business rules in tools.
-   Do not scatter global configuration across the code.
-   Do not create global client singletons.
-   Do not add a dependency without need.
-   Do not add write features.
-   Do not expose stack traces to clients.
-   Do not log credentials or sensitive payloads.
-   Document non-obvious technical decisions.
-   Test error and boundary behaviors, not only the happy path.

## Error handling

Define a small, useful hierarchy, for example:

-   `StreamSageError`
-   `KafkaConnectionError`
-   `KafkaOperationTimeoutError`
-   `KafkaResourceNotFoundError`
-   `KafkaMetadataError`
-   `ConfigurationError`

Infrastructure translates library-specific errors into application errors.
The MCP layer converts known errors into safe and useful responses, without
revealing secrets or stack traces.

## Observability of the server itself

Structured logs must record:

-   Operation/tool name.
-   Duration.
-   Result (success/failure).
-   Normalized error type.
-   Correlation identifier, if available.

Do not log passwords, tokens, certificates or message payloads.

------------------------------------------------------------------------

# 21. First task for Claude Code

Start by implementing **only the M1 foundation**, without trying to
anticipate M2–M5.

Work order:

1.  Inspect the repository and confirm whether it is empty or already
    contains files.
2.  Check the current stable versions of Python, the MCP SDK and
    `confluent-kafka` in the official documentation.
3.  Create the minimal `src` and `tests` structure.
4.  Configure `pyproject.toml`, uv and the lockfile with real compatible
    versions.
5.  Configure Ruff, mypy and pytest.
6.  Create `.gitignore`, `.env.example` and `.python-version`.
7.  Create Docker Compose with one Apache Kafka broker in KRaft mode and an
    explicit tag.
8.  Implement Settings and the Kafka AdminClient Factory.
9.  Implement the domain entities and the Ports for Cluster and Topic.
10. Implement the Kafka adapters and the two use cases.
11. Implement the MCP server with `get_cluster_info`, `list_topics` and
    `describe_topic`.
12. Implement the Resources and the Prompt defined for M1, if the official
    API of the selected version allows it.
13. Create unit and integration tests.
14. Run lint, type checking and tests.
15. Update the README with real, verified instructions.

Do not move on to consumer groups, lag or diagnostics in this first cycle.

At the end, present:

-   The actual file tree created.
-   Technical decisions and selected versions.
-   Commands to start Kafka.
-   Commands to install and run StreamSage.
-   How to connect an MCP client or use the MCP Inspector.
-   Test results.
-   Known pending items.
-   Confirmation that no Kafka write operation was implemented.

**Definition of success for the first cycle:** an MCP client can call
`get_cluster_info` and `list_topics` against a real local Kafka broker, with
automated tests and without violating the architecture defined in this
document.
