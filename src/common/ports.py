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
