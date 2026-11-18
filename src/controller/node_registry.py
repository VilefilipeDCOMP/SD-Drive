from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Sequence

from common.ports import NodeRegistryPort, NodeSnapshot

try:
    from common.logging import get_logger
    logger = get_logger("central.control")
except ImportError:
    class _CompatLogger:
        def __init__(self, name: str) -> None:
            self._log = logging.getLogger(name)

        def debug(self, event: str, **fields) -> None:
            self._log.debug("%s %s", event, fields)

        def info(self, event: str, **fields) -> None:
            self._log.info("%s %s", event, fields)

        def warning(self, event: str, **fields) -> None:
            self._log.warning("%s %s", event, fields)

        def error(self, event: str, **fields) -> None:
            self._log.error("%s %s", event, fields)

    logger = _CompatLogger("central.control")

try:
    import dcomp_drive_pb2 as pb
    NODE_STATE_UNSPECIFIED = pb.NODE_STATE_UNSPECIFIED
    NODE_ALIVE = pb.NODE_ALIVE
    NODE_SUSPECT = pb.NODE_SUSPECT
    NODE_DEAD = pb.NODE_DEAD
    NODE_DRAINING = pb.NODE_DRAINING
except ImportError:
    NODE_STATE_UNSPECIFIED = 0
    NODE_ALIVE = 1
    NODE_SUSPECT = 2
    NODE_DEAD = 3
    NODE_DRAINING = 4

_STATE_NAMES = {
    NODE_STATE_UNSPECIFIED: "UNSPECIFIED",
    NODE_ALIVE: "ALIVE",
    NODE_SUSPECT: "SUSPECT",
    NODE_DEAD: "DEAD",
    NODE_DRAINING: "DRAINING",
}

_MAX_DISK_USAGE_FRACTION = 0.90


@dataclass
class _NodeEntry:
    node_id: str
    machine_id: str
    disk_id: str
    grpc_target: str
    boot_id: str
    state: int = NODE_ALIVE
    total_bytes: int = 0
    used_bytes: int = 0
    available_bytes: int = 0
    last_heartbeat: float = field(default_factory=time.monotonic)
    registered_at: float = field(default_factory=time.monotonic)

    def to_snapshot(self) -> NodeSnapshot:
        return NodeSnapshot(
            node_id=self.node_id,
            machine_id=self.machine_id,
            disk_id=self.disk_id,
            grpc_target=self.grpc_target,
            state=self.state,
            total_bytes=self.total_bytes,
            used_bytes=self.used_bytes,
            available_bytes=self.available_bytes,
        )


class NodeRegistry(NodeRegistryPort):
    def __init__(
        self,
        heartbeat_interval_seconds: float = 3.0,
        suspect_timeout_seconds: float = 10.0,
        dead_timeout_seconds: float = 30.0,
        max_disk_usage_fraction: float = _MAX_DISK_USAGE_FRACTION,
    ) -> None:
        self._heartbeat_interval = heartbeat_interval_seconds
        self._suspect_timeout = suspect_timeout_seconds
        self._dead_timeout = dead_timeout_seconds
        self._max_usage = max_disk_usage_fraction

        self._nodes: dict[str, _NodeEntry] = {}
        self._lock = threading.RLock()
        self._state_listeners: list = []

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name="node-monitor"
        )
        self._monitor_thread.start()

    def register_node(
        self,
        node_id: str,
        machine_id: str,
        disk_id: str,
        grpc_target: str,
        boot_id: str,
        total_bytes: int,
        used_bytes: int,
    ) -> dict:
        with self._lock:
            existing = self._nodes.get(node_id)
            if existing is not None:
                old_boot_id = existing.boot_id
                if old_boot_id != boot_id:
                    logger.info(
                        "node.reboot_detected",
                        node_id=node_id,
                        old_boot_id=old_boot_id,
                        boot_id=boot_id,
                    )
                old_state = existing.state
                new_state = NODE_ALIVE if old_state != NODE_DRAINING else NODE_DRAINING
                existing.machine_id = machine_id
                existing.disk_id = disk_id
                existing.boot_id = boot_id
                existing.grpc_target = grpc_target
                existing.total_bytes = total_bytes
                existing.used_bytes = used_bytes
                existing.available_bytes = max(0, total_bytes - used_bytes)
                existing.last_heartbeat = time.monotonic()
                existing.state = new_state
                if old_state != new_state:
                    logger.info(
                        "node.state_changed",
                        node_id=node_id,
                        from_state=_STATE_NAMES.get(old_state, str(old_state)),
                        to_state=_STATE_NAMES.get(new_state, str(new_state)),
                        silence_seconds=0.0,
                    )
                    self._notify_state_change(node_id, old_state, new_state)
            else:
                entry = _NodeEntry(
                    node_id=node_id,
                    machine_id=machine_id,
                    disk_id=disk_id,
                    grpc_target=grpc_target,
                    boot_id=boot_id,
                    total_bytes=total_bytes,
                    used_bytes=used_bytes,
                    available_bytes=max(0, total_bytes - used_bytes),
                )
                self._nodes[node_id] = entry

            logger.info(
                "node.registered",
                node_id=node_id,
                boot_id=boot_id,
                machine_id=machine_id,
                disk_id=disk_id,
                grpc_target=grpc_target,
            )

        return {
            "heartbeat_interval_seconds": int(self._heartbeat_interval),
            "suspect_timeout_seconds": int(self._suspect_timeout),
            "dead_timeout_seconds": int(self._dead_timeout),
        }

    def process_heartbeat(
        self,
        node_id: str,
        boot_id: str,
        total_bytes: int,
        used_bytes: int,
        available_bytes: int,
    ) -> bool:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                logger.warning(
                    "heartbeat.rejected",
                    node_id=node_id,
                    boot_id=boot_id,
                    reason="unknown_node",
                )
                return False
            if entry.boot_id != boot_id:
                logger.warning(
                    "heartbeat.rejected",
                    node_id=node_id,
                    boot_id=boot_id,
                    reason="stale_boot_id",
                )
                return False

            now = time.monotonic()
            silence = round(now - entry.last_heartbeat, 3)
            old_state = entry.state
            entry.last_heartbeat = now
            entry.total_bytes = total_bytes
            entry.used_bytes = used_bytes
            entry.available_bytes = available_bytes

            logger.debug("heartbeat.received", node_id=node_id, boot_id=boot_id)

            if old_state in (NODE_SUSPECT, NODE_DEAD):
                entry.state = NODE_ALIVE
                logger.info(
                    "node.state_changed",
                    node_id=node_id,
                    from_state=_STATE_NAMES.get(old_state, str(old_state)),
                    to_state=_STATE_NAMES[NODE_ALIVE],
                    silence_seconds=silence,
                )
                self._notify_state_change(node_id, old_state, NODE_ALIVE)

            return True

    def set_draining(self, node_id: str) -> bool:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                return False
            old_state = entry.state
            entry.state = NODE_DRAINING
            if old_state != NODE_DRAINING:
                logger.info(
                    "node.state_changed",
                    node_id=node_id,
                    from_state=_STATE_NAMES.get(old_state, str(old_state)),
                    to_state=_STATE_NAMES[NODE_DRAINING],
                    silence_seconds=round(time.monotonic() - entry.last_heartbeat, 3),
                )
                self._notify_state_change(node_id, old_state, NODE_DRAINING)
            return True

    def set_alive(self, node_id: str) -> bool:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                return False
            old_state = entry.state
            entry.state = NODE_ALIVE
            if old_state != NODE_ALIVE:
                logger.info(
                    "node.state_changed",
                    node_id=node_id,
                    from_state=_STATE_NAMES.get(old_state, str(old_state)),
                    to_state=_STATE_NAMES[NODE_ALIVE],
                    silence_seconds=0.0,
                )
                self._notify_state_change(node_id, old_state, NODE_ALIVE)
            return True

    def add_state_listener(self, fn) -> None:
        self._state_listeners.append(fn)

    def list_eligible_nodes(self) -> Sequence[NodeSnapshot]:
        with self._lock:
            result = []
            for entry in self._nodes.values():
                if entry.state != NODE_ALIVE:
                    continue
                if entry.total_bytes > 0:
                    usage_fraction = entry.used_bytes / entry.total_bytes
                    if usage_fraction >= self._max_usage:
                        continue
                result.append(entry.to_snapshot())
            return tuple(result)

    def get_node_state(self, node_id: str) -> int:
        with self._lock:
            entry = self._nodes.get(node_id)
            return entry.state if entry else NODE_STATE_UNSPECIFIED

    def get_node_capacity(self, node_id: str) -> NodeSnapshot:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                raise KeyError(node_id)
            return entry.to_snapshot()

    def get_all_nodes(self) -> list[NodeSnapshot]:
        with self._lock:
            return [e.to_snapshot() for e in self._nodes.values()]

    def get_node(self, node_id: str) -> Optional[NodeSnapshot]:
        with self._lock:
            entry = self._nodes.get(node_id)
            return entry.to_snapshot() if entry else None

    def get_node_grpc_target(self, node_id: str) -> Optional[str]:
        with self._lock:
            entry = self._nodes.get(node_id)
            return entry.grpc_target if entry else None

    def _monitor_loop(self) -> None:
        check_interval = max(0.5, self._heartbeat_interval / 2)
        while True:
            time.sleep(check_interval)
            self._check_timeouts()

    def _check_timeouts(self) -> None:
        now = time.monotonic()
        with self._lock:
            for entry in list(self._nodes.values()):
                if entry.state == NODE_DRAINING:
                    continue

                elapsed = now - entry.last_heartbeat
                old_state = entry.state

                if elapsed >= self._dead_timeout and entry.state != NODE_DEAD:
                    entry.state = NODE_DEAD
                    logger.warning(
                        "node.state_changed",
                        node_id=entry.node_id,
                        from_state=_STATE_NAMES.get(old_state, str(old_state)),
                        to_state=_STATE_NAMES[NODE_DEAD],
                        silence_seconds=round(elapsed, 3),
                    )
                    self._notify_state_change(entry.node_id, old_state, NODE_DEAD)

                elif elapsed >= self._suspect_timeout and entry.state == NODE_ALIVE:
                    entry.state = NODE_SUSPECT
                    logger.warning(
                        "node.state_changed",
                        node_id=entry.node_id,
                        from_state=_STATE_NAMES.get(old_state, str(old_state)),
                        to_state=_STATE_NAMES[NODE_SUSPECT],
                        silence_seconds=round(elapsed, 3),
                    )
                    self._notify_state_change(entry.node_id, old_state, NODE_SUSPECT)

    def _notify_state_change(self, node_id: str, old_state: int, new_state: int) -> None:
        for fn in self._state_listeners:
            try:
                fn(node_id, old_state, new_state)
            except Exception as exc:
                logger.error(
                    "log.record",
                    msg=f"erro em listener de estado: {exc}",
                    node_id=node_id,
                )
