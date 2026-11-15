from __future__ import annotations

from typing import Protocol, Sequence

class NodeInfo:

    __slots__ = (
        "node_id", "machine_id", "disk_id", "grpc_target",
        "state", "total_bytes", "used_bytes", "available_bytes",
    )

    def __init__(
        self,
        node_id: str,
        machine_id: str,
        disk_id: str,
        grpc_target: str,
        state: str,
        total_bytes: int,
        used_bytes: int,
        available_bytes: int,
    ) -> None:
        self.node_id = node_id
        self.machine_id = machine_id
        self.disk_id = disk_id
        self.grpc_target = grpc_target
        self.state = state
        self.total_bytes = total_bytes
        self.used_bytes = used_bytes
        self.available_bytes = available_bytes


class BlockInfo:

    __slots__ = ("block_id", "node_id", "healthy", "version", "stored_size_bytes")

    def __init__(
        self,
        block_id: str,
        node_id: str,
        healthy: bool,
        version: int = 0,
        stored_size_bytes: int = 0,
    ) -> None:
        self.block_id = block_id
        self.node_id = node_id
        self.healthy = healthy
        self.version = version
        self.stored_size_bytes = stored_size_bytes


class ReplicaReceipt:

    __slots__ = ("block_id", "node_id", "stored_size_bytes", "verified_sha256")

    def __init__(
        self,
        block_id: str,
        node_id: str,
        stored_size_bytes: int,
        verified_sha256: bytes,
    ) -> None:
        self.block_id = block_id
        self.node_id = node_id
        self.stored_size_bytes = stored_size_bytes
        self.verified_sha256 = verified_sha256

class MetadataPort(Protocol):

    def list_under_replicated_blocks(self) -> list[tuple[str, int, int]]:
        ...

    def list_healthy_sources(self, block_id: str) -> list[BlockInfo]:
        ...

    def select_destination(
        self, block_id: str, excluded_node_ids: set[str]
    ) -> NodeInfo | None:
        ...

    def mark_replica_invalid(self, block_id: str, node_id: str) -> None:
        ...

    def register_recovered_replica(self, block_id: str, receipt: ReplicaReceipt) -> None:
        ...

    def list_blocks_on_node(self, node_id: str) -> list[BlockInfo]:
        ...

    def gc_candidates(self) -> list[tuple[str, str]]:
        ...

class NodeRegistryPort(Protocol):

    def list_eligible_nodes(self) -> list[NodeInfo]:
        ...

    def get_node_state(self, node_id: str) -> str | None:
        ...

    def get_node_capacity(self, node_id: str) -> tuple[int, int, int] | None:
        ...
