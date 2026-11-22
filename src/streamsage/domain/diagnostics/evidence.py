"""Helpers for building bounded evidence lists."""

from collections.abc import Sequence

from streamsage.domain.diagnostics.finding import Evidence
from streamsage.domain.value_objects.lag import PartitionLag

# Findings list at most this many partitions as evidence.
MAX_EVIDENCE_PARTITIONS = 5


def partition_evidence(partitions: Sequence[PartitionLag]) -> tuple[Evidence, ...]:
    """One ``topic[partition].lag`` entry per partition, capped, plus a count."""
    evidence = [
        Evidence(f"{p.topic}[{p.partition}].lag", p.lag)
        for p in partitions[:MAX_EVIDENCE_PARTITIONS]
    ]
    if len(partitions) > MAX_EVIDENCE_PARTITIONS:
        evidence.append(
            Evidence("more_partitions", len(partitions) - MAX_EVIDENCE_PARTITIONS)
        )
    return tuple(evidence)
