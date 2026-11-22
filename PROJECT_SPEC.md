# StreamSage --- Especificação Técnica e Guia de Implementação

**Documento:** `PROJECT_SPEC.md`\
**Versão da especificação:** 1.0\
**Status:** Aprovado para implementação do M1\
**Projeto:** `FVitor7/streamsage`\
**Nome:** StreamSage\
**Tagline:** AI-powered Kafka Observability & Troubleshooting MCP
Server\
**Linguagem principal:** Python 3.12+\
**Broker inicial:** Apache Kafka\
**Arquitetura:** Clean Architecture + Hexagonal Architecture (Ports and
Adapters)\
**Modelo de operação inicial:** Read-only\
**Público-alvo:** Desenvolvedores backend, platform engineers e SREs que
utilizam Kafka e clientes compatíveis com MCP.

------------------------------------------------------------------------

# 1. Instruções para Claude Code

Este documento é a fonte de verdade para implementar o StreamSage. Antes
de alterar ou criar arquivos:

1.  Leia este documento integralmente.
2.  Inspecione o estado atual do repositório antes de sobrescrever
    arquivos.
3.  Siga as decisões arquiteturais e os limites de escopo definidos
    aqui.
4.  Implemente em milestones pequenos e verificáveis.
5.  Não invente APIs do SDK MCP. Consulte a documentação oficial e
    valide a API da versão efetivamente instalada.
6.  Não introduza dependências, abstrações ou funcionalidades fora do
    escopo sem explicar a necessidade e propor a alteração.
7.  Não implemente operações de escrita no Kafka.
8.  Não coloque credenciais, dados sensíveis ou arquivos `.env` no Git.
9.  Crie testes para cada comportamento relevante.
10. Ao concluir cada milestone, informe arquivos criados/alterados,
    comandos executados, resultados dos testes, limitações e próximos
    passos.

## Diretriz de implementação

A prioridade é entregar uma aplicação pequena, funcional, bem testada e
arquiteturalmente correta. Evite overengineering. A arquitetura deve
permitir evolução, mas não criar camadas vazias ou abstrações sem uso
real.

Quando houver conflito entre um exemplo de código deste documento e a
API oficial atual de uma dependência, prevalecem a documentação oficial
e os contratos arquiteturais deste documento. Atualize o código e
registre a decisão.

------------------------------------------------------------------------

# 2. Visão do produto

O StreamSage é um servidor MCP open source em Python que permite que
agentes de IA e outros clientes compatíveis com MCP explorem clusters
Apache Kafka, consultem metadados e investiguem problemas operacionais.

O objetivo não é apenas expor chamadas do Kafka como ferramentas MCP. O
produto deve oferecer uma camada de aplicação e, nas próximas fases,
diagnósticos determinísticos baseados em evidências.

## 2.1 Problema

Informações importantes para operar Kafka estão distribuídas entre
tópicos, partições, brokers, consumer groups, offsets, configurações e
ferramentas de linha de comando. Investigações exigem várias consultas e
conhecimento operacional.

O StreamSage deve oferecer uma interface consistente para essas
consultas, permitindo que um agente coordene as ferramentas e apresente
conclusões fundamentadas.

## 2.2 Exemplos de perguntas que o produto deverá suportar

-   What topics are available in this cluster?
-   Which consumer groups have the highest lag?
-   Is any consumer group experiencing stalled partitions?
-   Which partitions are accumulating the most messages?
-   What is the current metadata health of my Kafka cluster?
-   Can you investigate the `orders.created` topic?

## 2.3 Princípios do produto

-   Read-only by design.
-   Evidence-based diagnostics.
-   Broker-specific code isolado em adapters.
-   Domínio independente de MCP e Kafka.
-   Contratos tipados e validação de entradas.
-   Timeouts explícitos e erros controlados.
-   Segurança por menor privilégio.
-   Testes unitários sem broker e testes de integração com Kafka real.
-   Documentação e exemplos reproduzíveis.

------------------------------------------------------------------------

# 3. Escopo e decisões aprovadas

Estas decisões foram tomadas e devem ser respeitadas no MVP.

  -----------------------------------------------------------------------
  Tema                                Decisão
  ----------------------------------- -----------------------------------
  Nome                                StreamSage

  Repositório                         `FVitor7/streamsage`

  Linguagem                           Python

  Versão mínima                       Python 3.12

  Broker inicial                      Apache Kafka

  Redpanda                            Fora do escopo inicial

  Amazon MSK                          Fora do escopo inicial; poderá ser
                                      suportado via configuração Kafka
                                      compatível no futuro

  Protocolo                           MCP

  SDK                                 SDK oficial MCP para Python

  Transporte inicial                  stdio

  Streamable HTTP                     Evolução futura

  Arquitetura                         Clean Architecture + Ports and
                                      Adapters

  Cliente Kafka                       `confluent-kafka`

  Configuração                        Pydantic Settings + variáveis de
                                      ambiente

  Gerenciador de dependências         uv

  Testes                              pytest

  Qualidade                           Ruff, mypy, pytest-cov, GitHub
                                      Actions

  Ambiente local                      Docker Compose + Kafka em modo
                                      KRaft

  Persistência interna                Nenhuma no MVP

  LLM próprio                         Não

  LangChain/LangGraph                 Não

  Operações Kafka                     Somente leitura

  Documentação                        Inglês no repositório público

  Licença proposta                    MIT
  -----------------------------------------------------------------------

## 3.1 Fora do escopo do MVP

-   Interface web própria.
-   Banco de dados interno.
-   Escrita/publicação de mensagens.
-   Criação ou exclusão de tópicos.
-   Alteração de offsets.
-   Alteração de configurações do cluster.
-   Integração direta com OpenAI, Gemini, Anthropic ou outro LLM.
-   Monitoramento contínuo em background.
-   Sistema de alertas.
-   Adaptadores específicos para Redpanda ou outros brokers.
-   Diagnóstico baseado em LLM dentro do servidor.

O cliente MCP/LLM é responsável por interpretar e combinar os
resultados. O servidor fornece dados e diagnósticos determinísticos.

------------------------------------------------------------------------

# 4. Arquitetura

Adotar Clean Architecture combinada com Hexagonal Architecture (Ports
and Adapters).

## 4.1 Camadas

### Presentation

Responsável pelo protocolo MCP e pela exposição de:

-   Tools.
-   Resources.
-   Prompts.

Não deve conter regras de negócio nem acessar diretamente o cliente
Kafka.

### Application

Responsável por:

-   Use cases.
-   Orquestração.
-   DTOs de aplicação.
-   Dependências por meio de Ports.

Os casos de uso não devem importar classes de infraestrutura ou do SDK
MCP.

### Domain

Responsável por:

-   Entidades.
-   Value Objects.
-   Regras de negócio.
-   Diagnósticos e severidades.
-   Exceções de domínio.

O domínio não pode depender de Kafka, MCP, Pydantic ou bibliotecas de
infraestrutura.

### Infrastructure

Responsável por:

-   Implementações dos Ports.
-   Cliente Kafka.
-   Configuração.
-   Integrações externas.
-   Conversão dos modelos externos para modelos de domínio.

## 4.2 Diagrama lógico

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

## 4.3 Regra de dependência

As dependências devem apontar para dentro:

-   Presentation depende de Application.
-   Application depende de Domain e declara Ports.
-   Infrastructure implementa Ports e depende de Application/Domain.
-   Domain não depende de nenhuma camada externa.

O Composition Root (`main.py` ou módulo equivalente) é o ponto que
instancia as implementações concretas e injeta as dependências.

## 4.4 Decisão sobre síncrono e assíncrono

O cliente `confluent-kafka` e suas operações administrativas são
síncronos. No M1, manter as operações Kafka síncronas e configurar
timeouts explícitos.

Não adicionar `asyncio.to_thread`, filas ou complexidade de concorrência
sem uma necessidade demonstrada. Se a API MCP exigir handlers
assíncronos, adaptar a fronteira de apresentação sem contaminar o
domínio e a aplicação.

------------------------------------------------------------------------

# 5. Estrutura de diretórios

Estrutura-alvo. Criar apenas os módulos necessários em cada milestone;
não criar arquivos vazios apenas para reproduzir a árvore.

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

# 6. Modelos de domínio

Os modelos de domínio devem ser Python puro, preferencialmente
`dataclasses` imutáveis quando apropriado. Não utilizar modelos Pydantic
como entidades de domínio.

Exemplos conceituais:

## 6.1 Cluster e Broker

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

## 6.2 Topic e Partition

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

**Regra importante:** ausência de committed offset ou de informação
necessária para calcular lag deve ser representada como `None`, nunca
convertida silenciosamente para zero.

------------------------------------------------------------------------

# 7. Application Ports

Usar `typing.Protocol` para definir contratos estruturais.

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

Criar `ConsumerGroup` e os demais modelos quando o M2 for iniciado. Não
é necessário implementar todo o contrato no M1.

------------------------------------------------------------------------

# 8. Application Use Cases

Use cases devem ser pequenos, explícitos e testáveis com mocks/fakes.

Exemplo:

``` python
from streamsage.application.ports.topic_reader import TopicReader
from streamsage.domain.entities.topic import Topic


class ListTopicsUseCase:
    def __init__(self, topic_reader: TopicReader):
        self._topic_reader = topic_reader

    def execute(self) -> list[Topic]:
        return self._topic_reader.list_topics()
```

Outro exemplo:

``` python
from streamsage.application.ports.cluster_reader import ClusterReader
from streamsage.domain.entities.cluster import Cluster


class GetClusterInfoUseCase:
    def __init__(self, cluster_reader: ClusterReader):
        self._cluster_reader = cluster_reader

    def execute(self) -> Cluster:
        return self._cluster_reader.get_cluster_info()
```

Use cases não devem importar `confluent_kafka`, SDK MCP ou classes de
configuração.

------------------------------------------------------------------------

# 9. Infraestrutura Kafka

## 9.1 Biblioteca

Utilizar `confluent-kafka` (Python client baseado em librdkafka).

O adapter deve encapsular as particularidades da biblioteca e converter
resultados externos em modelos do domínio.

## 9.2 AdminClient Factory

Responsabilidades:

-   Criar o AdminClient.
-   Receber Settings por injeção.
-   Configurar bootstrap servers.
-   Configurar timeouts.
-   Configurar segurança SASL/TLS quando aplicável.
-   Evitar expor segredos em logs ou erros.
-   Não espalhar criação de clientes pela aplicação.

Exemplo conceitual:

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

        # Adicionar configuração SASL/TLS somente quando necessária.
        # Nunca registrar username/password ou material de certificado.

        return AdminClient(config)
```

Verificar os nomes e unidades das configurações suportadas pela versão
efetivamente instalada do `confluent-kafka`. Não assumir que todos os
campos do exemplo são válidos em qualquer versão.

## 9.3 Kafka Adapters

Implementar:

-   `KafkaClusterAdapter` --- implementa `ClusterReader`.
-   `KafkaTopicAdapter` --- implementa `TopicReader`.
-   `KafkaConsumerGroupAdapter` --- implementa `ConsumerGroupReader`, no
    M2.

Adapters devem:

-   Converter modelos Kafka em modelos de domínio.
-   Normalizar erros de biblioteca em exceções da aplicação/domínio.
-   Definir timeouts.
-   Tratar metadados incompletos.
-   Evitar operações de escrita.
-   Não retornar objetos internos da biblioteca Kafka para as camadas
    superiores.

### Cuidado com criação acidental de tópicos

Não usar consultas direcionadas a um nome de tópico inexistente como
mecanismo de validação sem compreender a semântica e a configuração do
broker. Em alguns cenários/configurações, consultas Kafka podem
interagir com auto-creation.

Para `describe_topic`, preferir consultar metadados conhecidos e validar
a existência sem provocar criação. Testar esse comportamento com o
broker local e documentar a garantia.

------------------------------------------------------------------------

# 10. MCP Presentation Layer

## 10.1 SDK

Utilizar o SDK oficial MCP para Python. Antes de implementar:

-   Consultar a documentação oficial atual.
-   Confirmar a versão estável disponível.
-   Confirmar a API de criação do servidor, registro de
    tools/resources/prompts e execução stdio.
-   Fixar uma faixa de versão compatível no `pyproject.toml` e registrar
    a resolução exata no `uv.lock`.
-   Não copiar exemplos de versões antigas sem validação.

O MCP deve ser uma camada fina: receber parâmetros, validar entradas,
chamar use cases e serializar resultados.

## 10.2 Tools do M1

  ----------------------------------------------------------------------------------
  Tool                    Entrada                            Resultado
  ----------------------- ---------------------------------- -----------------------
  `get_cluster_info`      Nenhuma                            ID, brokers e
                                                             controller quando
                                                             disponível

  `list_topics`           `include_internal: bool = false`   Lista de tópicos

  `describe_topic`        `topic_name: str`                  Partições, leader,
                                                             replicas e ISR
  ----------------------------------------------------------------------------------

Todas as ferramentas devem ter descrições claras, parâmetros tipados e
respostas estruturadas.

## 10.3 Tools planejadas para M2

  -----------------------------------------------------------------------------
  Tool                          Entrada                 Resultado
  ----------------------------- ----------------------- -----------------------
  `list_consumer_groups`        Filtros opcionais       Consumer groups

  `describe_consumer_group`     `group_id`              Estado e membros

  `get_consumer_lag`            `group_id`              Lag por tópico/partição

  `analyze_consumer_health`     `group_id`              Classificação baseada
                                                        em evidências

  `detect_stalled_partitions`   `group_id`              Partições sem
                                                        progresso, se houver
                                                        snapshots suficientes
  -----------------------------------------------------------------------------

## 10.4 Resources do M1

-   `kafka://cluster/info`
-   `kafka://topics`

Resources devem fornecer dados consultáveis e não duplicar
desnecessariamente as regras das tools. Reutilizar use cases/serviços de
aplicação.

## 10.5 Prompt do M1

`investigate_kafka_cluster`

O prompt orienta o cliente a:

1.  Consultar metadados do cluster.
2.  Listar tópicos.
3.  Investigar detalhes relevantes.
4.  Distinguir dados observados de hipóteses.
5.  Não afirmar saúde completa do cluster com base apenas em metadados
    limitados.

## 10.6 Transporte

M1: stdio.

Evolução futura: Streamable HTTP.

Não implementar HTTP no M1.

------------------------------------------------------------------------

# 11. Configuração

Utilizar Pydantic Settings e variáveis de ambiente.

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

Modelo inicial:

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

Requisitos:

-   Validar configurações inconsistentes.
-   Não registrar segredos.
-   Suportar PLAINTEXT apenas para desenvolvimento local.
-   Documentar configuração SASL/TLS.
-   Nunca versionar `.env`.
-   Usar `SecretStr` para segredos.
-   Não permitir que o cliente MCP forneça credenciais arbitrárias em
    cada tool call.

------------------------------------------------------------------------

# 12. Segurança

## 12.1 Read-only by design

Permitido:

-   Consultar metadados do cluster.
-   Listar e descrever tópicos.
-   Consultar consumer groups.
-   Consultar offsets e lag.
-   Inspecionar mensagens em escopo limitado em milestone futuro.

Proibido no MVP:

-   Criar ou excluir tópicos.
-   Publicar mensagens.
-   Alterar offsets.
-   Alterar configurações do cluster.
-   Executar operações administrativas de escrita.

A política read-only deve ser aplicada tanto no código quanto nas
ACLs/credenciais do Kafka.

## 12.2 Princípio do menor privilégio

A credencial Kafka deve possuir somente as permissões necessárias às
consultas disponibilizadas. Não confiar apenas na ausência de métodos de
escrita no código.

## 12.3 Proteção de dados

-   Não registrar payloads de mensagens por padrão.
-   Não registrar senhas, tokens ou certificados.
-   Limitar quantidade e tamanho de mensagens inspecionadas.
-   Sanitizar informações sensíveis.
-   Definir timeouts e limites para consultas.
-   Não retornar stack traces internos aos clientes MCP.
-   Não expor configurações secretas em Resources.

## 12.4 MCP

-   Validar todos os argumentos das tools.
-   Não permitir que entradas controlem comandos de shell.
-   Não aceitar caminhos arbitrários de arquivos.
-   Documentar claramente o transporte e as considerações de confiança.
-   Se Streamable HTTP for implementado, adicionar autenticação e
    controles de acesso antes de disponibilizá-lo fora de ambiente
    local.

------------------------------------------------------------------------

# 13. Dependências

Utilizar `uv` para gerenciamento e lockfile.

## 13.1 Produção

-   `mcp[cli]` --- SDK oficial MCP e CLI, conforme a
    distribuição/documentação atual.
-   `confluent-kafka` --- cliente Kafka.
-   `pydantic` --- validação e modelos de fronteira.
-   `pydantic-settings` --- configuração.

## 13.2 Desenvolvimento

-   `pytest`
-   `pytest-cov`
-   `pytest-asyncio` --- somente onde houver código assíncrono que
    precise de testes.
-   `ruff`
-   `mypy`
-   `pre-commit`
-   `testcontainers` --- opcional para testes de integração; avaliar se
    Docker Compose é mais simples para o CI.

## 13.3 Não adicionar no MVP

-   LangChain.
-   LangGraph.
-   SDKs de provedores de LLM.
-   Banco de dados.
-   Framework web adicional.
-   Sistema de filas próprio.

## 13.4 pyproject.toml inicial

As faixas abaixo são propostas iniciais. Validar a disponibilidade e
compatibilidade das versões no momento da implementação. Não assumir que
uma versão futura ou preview é estável.

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

Observação: a faixa do pacote MCP deve ser ajustada após verificar a
versão estável e sua API oficial. Se o pacote oficial não tiver a faixa
proposta, corrigir para a faixa real e registrar a decisão. Não forçar
uma versão inexistente.

------------------------------------------------------------------------

# 14. Ambiente local Kafka

Utilizar Apache Kafka em modo KRaft, sem ZooKeeper.

M1:

-   Um broker.
-   Porta externa `9092`.
-   Endereço interno apropriado para containers.
-   Healthcheck.
-   Volume persistente.
-   Configuração reproduzível via Docker Compose.

M1 não precisa simular alta disponibilidade. Em milestone futuro, criar
ambiente com três brokers para testar replicação, ISR e distribuição de
partições.

O `docker-compose.yml` deve usar uma imagem oficial/confiável do Kafka,
tag explícita e configuração KRaft documentada. Evitar `latest` para
builds reproduzíveis.

Comandos esperados:

``` bash
docker compose up -d
docker compose ps
docker compose logs -f kafka
docker compose down
```

O teste de integração deve aguardar o broker ficar saudável antes de
executar consultas.

------------------------------------------------------------------------

# 15. Composition Root

O ponto de composição instancia Settings, factories, adapters, use cases
e servidor MCP.

Exemplo conceitual:

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

O `main.py` deve iniciar o servidor no transporte stdio no M1.

Também deve ser definido um ciclo de vida claro para clientes/conexões e
encerramento. Não deixar recursos sem gerenciamento.

------------------------------------------------------------------------

# 16. Estratégia de testes

## 16.1 Unit tests

Testar sem Kafka:

-   Entidades e Value Objects.
-   Regras de domínio.
-   Use cases usando mocks/fakes.
-   Conversões e validações puras.
-   Normalização de erros.

## 16.2 Integration tests

Executar contra Kafka real:

-   Conexão e leitura de metadados.
-   Listagem de tópicos.
-   Descrição de tópico.
-   Tratamento de broker indisponível.
-   Comportamento de tópico inexistente.
-   Filtragem de tópicos internos.

## 16.3 Contract tests

Verificar que os adapters implementam os Ports esperados e que os
contratos de entrada/saída permanecem consistentes.

## 16.4 MCP tests

-   Inicialização do servidor.
-   Registro das tools.
-   Validação dos argumentos.
-   Execução de cada tool.
-   Formato das respostas.
-   Tratamento de erros sem vazamento de detalhes internos.

## 16.5 Cobertura

Meta inicial: 80% de cobertura de branches no código de domínio e
aplicação, sem perseguir cobertura artificial em código de integração
gerado ou trivial.

------------------------------------------------------------------------

# 17. CI/CD

GitHub Actions deve executar em Pull Requests e pushes relevantes:

``` bash
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest --cov
```

Adicionar job de integração que inicialize Kafka e execute os testes de
integração.

A CI deve:

-   Utilizar versão de Python suportada.
-   Utilizar lockfile.
-   Não usar credenciais reais.
-   Publicar resultados de testes.
-   Falhar em lint, type checking ou testes.
-   Não publicar pacote automaticamente no M1.

------------------------------------------------------------------------

# 18. Milestones

## M1 --- Foundation & Cluster Explorer

**Status:** Próximo milestone a implementar.

Objetivo: provar o fluxo ponta a ponta entre cliente MCP, aplicação
Python e Kafka real.

### Entregas

1.  Inicializar repositório.
2.  Configurar `pyproject.toml`, `uv` e lockfile.
3.  Criar Settings.
4.  Criar Kafka AdminClient Factory.
5.  Criar Docker Compose com Kafka KRaft.
6.  Criar entidades de Cluster, Broker, Topic e Partition.
7.  Criar Ports ClusterReader e TopicReader.
8.  Criar KafkaClusterAdapter e KafkaTopicAdapter.
9.  Criar use cases.
10. Criar MCP server.
11. Registrar três tools.
12. Registrar resources e prompt inicial.
13. Criar testes unitários e de integração.
14. Criar workflow de CI.
15. Documentar execução local.

### Tools do M1

-   `get_cluster_info`
-   `list_topics`
-   `describe_topic`

### Resources do M1

-   `kafka://cluster/info`
-   `kafka://topics`

### Prompt do M1

-   `investigate_kafka_cluster`

### Critérios de aceite

-   Servidor inicia sem erros.
-   Conexão com Kafka local funciona.
-   `get_cluster_info` retorna metadados disponíveis.
-   `list_topics` retorna tópicos.
-   Tópicos internos são filtrados por padrão.
-   `describe_topic` retorna partições, leader, replicas e ISR.
-   Tópico inexistente gera erro controlado.
-   Broker indisponível gera erro controlado e limitado por timeout.
-   Unit tests não dependem de Kafka.
-   Integration tests funcionam com Kafka real.
-   MCP Inspector consegue executar as três tools.
-   Nenhuma operação de escrita é exposta.
-   Segredos não aparecem em logs ou respostas.

## M2 --- Consumer Intelligence

-   Listar e descrever consumer groups.
-   Consultar committed offsets.
-   Consultar log end offsets.
-   Calcular lag por partição e grupo.
-   Ordenar partições por lag.
-   Tratar ausência de committed offset como dado ausente, não zero.
-   Criar classificação inicial de saúde com limites configuráveis.

## M3 --- Diagnostics Engine

-   Findings estruturados.
-   Severidade configurável.
-   Evidências.
-   Hipóteses e recomendações.
-   Dados insuficientes explicitamente sinalizados.
-   Detecção de partições sem progresso por comparação de snapshots.
-   Não afirmar causa raiz sem evidências suficientes.

**Regra:** lag alto não significa necessariamente consumer travado. Para
detectar ausência de progresso, comparar offsets em mais de um instante.

## M4 --- Message Intelligence

-   Inspeção limitada de mensagens.
-   Metadados, key, headers, timestamp e offset.
-   Busca por chave dentro de limites definidos.
-   DLQ configurável, sem pressupor convenção universal.
-   Limites de quantidade e tamanho.
-   Sanitização de payloads.

## M5 --- Production Readiness

-   Streamable HTTP.
-   Autenticação e autorização para HTTP.
-   TLS/SASL.
-   Logging estruturado.
-   Métricas do servidor.
-   Documentação de segurança.
-   Release v1.0.

------------------------------------------------------------------------

# 19. README e apresentação pública

O repositório público deve ter documentação em inglês.

Estrutura sugerida para README:

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

A demonstração deverá mostrar um cliente MCP executando ferramentas
contra um Kafka local real. Não apresentar dados fictícios como se
fossem resultados reais.

Descrição sugerida para GitHub:

`An open-source MCP server for Kafka observability, diagnostics and troubleshooting using AI agents.`

Repositório:

`FVitor7/streamsage`

Licença proposta: MIT.

------------------------------------------------------------------------

# 20. Regras de engenharia para Claude Code

Ao implementar:

-   Preferir funções e classes pequenas.
-   Utilizar type hints.
-   Não usar `Any` sem justificativa.
-   Não capturar `Exception` genericamente sem repropagar/normalizar
    corretamente.
-   Não retornar objetos da biblioteca Kafka para fora dos adapters.
-   Não importar MCP em Application ou Domain.
-   Não importar Kafka em Domain.
-   Não colocar regras de negócio nas tools.
-   Não colocar configurações globais espalhadas pelo código.
-   Não criar singletons globais de cliente.
-   Não adicionar dependência sem necessidade.
-   Não adicionar funcionalidades de escrita.
-   Não expor stack traces aos clientes.
-   Não registrar credenciais ou payloads sensíveis.
-   Documentar decisões técnicas não óbvias.
-   Testar comportamentos de erro e limites, não apenas o caminho feliz.

## Tratamento de erros

Definir uma hierarquia pequena e útil, por exemplo:

-   `StreamSageError`
-   `KafkaConnectionError`
-   `KafkaOperationTimeoutError`
-   `KafkaResourceNotFoundError`
-   `KafkaMetadataError`
-   `ConfigurationError`

A infraestrutura traduz erros específicos da biblioteca para erros da
aplicação. A camada MCP converte erros conhecidos em respostas seguras e
úteis, sem revelar segredos ou stack traces.

## Observabilidade do próprio servidor

Logs estruturados devem registrar:

-   Nome da operação/tool.
-   Duração.
-   Resultado (sucesso/falha).
-   Tipo de erro normalizado.
-   Identificador de correlação, se disponível.

Não registrar senhas, tokens, certificados ou payloads de mensagens.

------------------------------------------------------------------------

# 21. Primeira tarefa para Claude Code

Comece implementando **somente a fundação do M1**, sem tentar antecipar
M2--M5.

Ordem de trabalho:

1.  Inspecione o repositório e confirme se está vazio ou se já contém
    arquivos.
2.  Verifique as versões estáveis atuais de Python, SDK MCP e
    `confluent-kafka` nas documentações oficiais.
3.  Crie a estrutura mínima `src` e `tests`.
4.  Configure `pyproject.toml`, uv e lockfile com versões compatíveis
    reais.
5.  Configure Ruff, mypy e pytest.
6.  Crie `.gitignore`, `.env.example` e `.python-version`.
7.  Crie Docker Compose com um broker Apache Kafka em modo KRaft e tag
    explícita.
8.  Implemente Settings e Kafka AdminClient Factory.
9.  Implemente entidades de domínio e Ports para Cluster e Topic.
10. Implemente os adapters Kafka e os dois use cases.
11. Implemente o MCP server com `get_cluster_info`, `list_topics` e
    `describe_topic`.
12. Implemente os Resources e o Prompt definidos para M1, se a API
    oficial da versão selecionada permitir.
13. Crie testes unitários e de integração.
14. Execute lint, type checking e testes.
15. Atualize README com instruções reais e verificadas.

Não avance para consumer groups, lag ou diagnóstico neste primeiro
ciclo.

Ao final, apresente:

-   Árvore real de arquivos criada.
-   Decisões técnicas e versões selecionadas.
-   Comandos para iniciar Kafka.
-   Comandos para instalar e executar StreamSage.
-   Como conectar um cliente MCP ou usar MCP Inspector.
-   Resultado dos testes.
-   Pendências conhecidas.
-   Confirmação de que nenhuma operação Kafka de escrita foi
    implementada.

**Definição de sucesso do primeiro ciclo:** um cliente MCP consegue
chamar `get_cluster_info` e `list_topics` contra um broker Kafka local
real, com testes automatizados e sem violar a arquitetura definida neste
documento.
