from collections.abc import Iterable

import dcomp_drive_pb2 as pb
from common.ports import NodeSnapshot

class SimulatedNodeRegistry:
    def __init__(self, nodes: Iterable[NodeSnapshot]):
        self._nodes = {node.node_id: node for node in nodes}

    def list_eligible_nodes(self) -> tuple[NodeSnapshot, ...]:
        return tuple(node for node in self._nodes.values() if node.state == pb.NODE_ALIVE)

    def get_node_state(self, node_id: str) -> int:
        return self._nodes[node_id].state

    def get_node_capacity(self, node_id: str) -> NodeSnapshot:
        return self._nodes[node_id]
