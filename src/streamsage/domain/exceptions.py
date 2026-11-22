"""Error hierarchy shared across StreamSage layers.

Messages of these exceptions are written by StreamSage itself and are safe to
show to MCP clients: they never contain credentials, raw library output or
stack traces. Infrastructure code translates library errors into these types.
"""


class StreamSageError(Exception):
    """Base class for all anticipated StreamSage failures."""


class ConfigurationError(StreamSageError):
    """The server configuration is invalid or inconsistent."""


class InvalidInputError(StreamSageError):
    """A caller-supplied argument is invalid."""


class KafkaConnectionError(StreamSageError):
    """The Kafka cluster could not be reached or authentication failed."""


class KafkaOperationTimeoutError(StreamSageError):
    """A Kafka operation did not complete within its configured timeout."""


class KafkaResourceNotFoundError(StreamSageError):
    """A requested Kafka resource (for example a topic) does not exist."""


class KafkaMetadataError(StreamSageError):
    """Kafka returned an error or unusable metadata."""
