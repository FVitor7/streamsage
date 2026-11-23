"""Smoke-test a built StreamSage package in a clean virtual environment.

Usage:
    uv run python scripts/smoke_test_package.py dist/streamsage-X.whl
    uv run python scripts/smoke_test_package.py dist/streamsage-X.tar.gz --python 3.15

The package is installed into a fresh virtual environment outside the
checkout, with runtime dependencies only. Then, from an empty directory (so
no source tree or ``.env`` file can leak in), it checks that:

- the installed version matches ``pyproject.toml`` and the package ships
  ``py.typed``;
- ``streamsage --help`` works;
- over stdio, an MCP client can initialize and list every tool, the prompt
  and the resources;
- the native Kafka client loads: a tool call against an unreachable broker
  fails with StreamSage's safe error, not a crash.

No Kafka broker is needed. Requires ``uv`` on PATH. POSIX only.
"""

import argparse
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

import anyio
from mcp import Client, StdioServerParameters
from mcp.types import TextContent

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_TOOLS = frozenset(
    {
        "get_cluster_info",
        "list_topics",
        "describe_topic",
        "list_consumer_groups",
        "describe_consumer_group",
        "get_consumer_lag",
        "analyze_consumer_health",
        "detect_stalled_partitions",
        "inspect_messages",
        "find_messages_by_key",
        "list_dlq_topics",
    }
)
EXPECTED_PROMPTS = frozenset({"investigate_kafka_cluster"})

# Nothing listens on port 1, so tool calls exercise the Kafka client and its
# error handling without a broker.
UNREACHABLE_ENV = {
    "STREAMSAGE_KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
    "STREAMSAGE_KAFKA_REQUEST_TIMEOUT": "2",
    "STREAMSAGE_KAFKA_SOCKET_TIMEOUT": "2",
    "STREAMSAGE_LOG_LEVEL": "CRITICAL",
}


class SmokeTestError(Exception):
    pass


def expected_version() -> str:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    return str(project["version"])


def _run(*args: str | Path, cwd: Path) -> str:
    result = subprocess.run(
        [str(a) for a in args], cwd=cwd, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise SmokeTestError(
            f"`{' '.join(str(a) for a in args)}` exited with {result.returncode}:\n"
            f"{result.stdout}{result.stderr}"
        )
    return result.stdout


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeTestError(message)


def install(dist: Path, python: str, workdir: Path) -> Path:
    venv = workdir / "venv"
    _run("uv", "venv", "--quiet", "--python", python, venv, cwd=workdir)
    interpreter = venv / "bin" / "python"
    _run("uv", "pip", "install", "--quiet", "--python", interpreter, dist, cwd=workdir)
    return venv


def check_metadata(venv: Path, version: str, python: str, workdir: Path) -> None:
    report = _run(
        venv / "bin" / "python",
        "-I",
        "-c",
        "import importlib.metadata as m, pathlib, sys, streamsage\n"
        "print(m.version('streamsage'))\n"
        "print(streamsage.__version__)\n"
        "print((pathlib.Path(streamsage.__file__).parent / 'py.typed').is_file())\n"
        "print('%d.%d' % sys.version_info[:2])\n"
        "print(pathlib.Path(streamsage.__file__).resolve())",
        cwd=workdir,
    ).split("\n")
    metadata_version, module_version, typed, runtime, location = report[:5]
    _check(metadata_version == version, f"metadata version {metadata_version!r}")
    _check(module_version == version, f"streamsage.__version__ {module_version!r}")
    _check(typed == "True", "the package does not ship py.typed")
    _check(runtime == python, f"installed on Python {runtime}, expected {python}")
    _check(
        Path(location).is_relative_to(venv.resolve()),
        f"streamsage imported from {location}, not from the clean environment",
    )
    usage = _run(venv / "bin" / "streamsage", "--help", cwd=workdir)
    _check("generate-token" in usage, "`streamsage --help` lists no commands")


async def check_mcp(venv: Path, workdir: Path) -> None:
    server = StdioServerParameters(
        command=str(venv / "bin" / "streamsage"),
        env=UNREACHABLE_ENV,
        cwd=workdir,
    )
    async with Client(server) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        _check(tools == EXPECTED_TOOLS, f"unexpected tools: {sorted(tools)}")
        prompts = {prompt.name for prompt in (await client.list_prompts()).prompts}
        _check(prompts == EXPECTED_PROMPTS, f"unexpected prompts: {sorted(prompts)}")
        resources = (await client.list_resources()).resources
        _check(bool(resources), "no resources are listed")

        result = await client.call_tool("get_cluster_info", {})
    text = " ".join(c.text for c in result.content if isinstance(c, TextContent))
    _check(result.is_error is True, "a call to an unreachable cluster succeeded")
    _check("Could not connect to the Kafka cluster" in text, f"unexpected: {text}")
    _check("Traceback" not in text, "the error exposes a traceback")


def smoke_test(dist: Path, python: str) -> None:
    version = expected_version()
    _check(dist.is_file(), f"{dist} does not exist")
    _check(f"-{version}" in dist.name, f"{dist.name} is not version {version}")
    with tempfile.TemporaryDirectory(prefix="streamsage-smoke-") as tmp:
        workdir = Path(tmp)
        venv = install(dist.resolve(), python, workdir)
        check_metadata(venv, version, python, workdir)
        anyio.run(check_mcp, venv, workdir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("dist", type=Path, help="wheel or sdist to test")
    parser.add_argument(
        "--python",
        default="3.12",
        help="Python version for the clean environment (default: 3.12)",
    )
    args = parser.parse_args(argv)
    try:
        smoke_test(args.dist, args.python)
    except SmokeTestError as exc:
        print(f"Smoke test FAILED for {args.dist.name}: {exc}", file=sys.stderr)
        return 1
    print(f"Smoke test passed: {args.dist.name} on Python {args.python}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
