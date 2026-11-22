"""StreamSage: an MCP server for Kafka observability and troubleshooting."""

from importlib.metadata import PackageNotFoundError, version

try:
    # Single source of truth: the version in pyproject.toml.
    __version__ = version("streamsage")
except PackageNotFoundError:  # pragma: no cover - running from a bare checkout
    __version__ = "0.0.0"
