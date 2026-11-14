from dataclasses import dataclass
from typing import Protocol, Sequence

@dataclass(frozen=True)
class NodeSnapshot:
    node_id: str
    machine_id: str
    disk_id: str
    grpc_target: str
    state: int  # NodeState do Protobuf
    total_bytes: int
    used_bytes: int
    available_bytes: int  # mínimo entre saldo lógico e espaço físico

class NodeRegistryPort(Protocol):
    def list_eligible_nodes(self) -> Sequence[NodeSnapshot]:
        ...

    def get_node_state(self, node_id: str) -> int:
        ...

    def get_node_capacity(self, node_id: str) -> NodeSnapshot:
        ...

class MetadataPort(Protocol):
    def list_under_replicated_blocks(self): ...

    def list_healthy_sources(self, block_id: str): ...

    def list_blocks_on_node(self, node_id: str): ...

    def select_destination(self, block_id: str, excluded_node_ids: set[str]): ...

    def mark_replica_invalid(self, block_id: str, node_id: str): ...

    def register_recovered_replica(self, block_id: str, receipt): ...
