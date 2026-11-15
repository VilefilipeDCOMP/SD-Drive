from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from src.common.logging_config import get_logger
from src.common.ports import NodeInfo, NodeRegistryPort

logger = get_logger("controller.node_registry")

STATE_ALIVE = "ALIVE"
STATE_SUSPECT = "SUSPECT"
STATE_DEAD = "DEAD"
STATE_DRAINING = "DRAINING"

_MAX_DISK_USAGE_FRACTION = 0.90


@dataclass
class _NodeEntry:

    node_id: str
    machine_id: str
    disk_id: str
    grpc_target: str
    boot_id: str
    state: str = STATE_ALIVE
    total_bytes: int = 0
    used_bytes: int = 0
    available_bytes: int = 0
    last_heartbeat: float = field(default_factory=time.monotonic)
    registered_at: float = field(default_factory=time.monotonic)

    def to_node_info(self) -> NodeInfo:
        return NodeInfo(
            node_id=self.node_id,
            machine_id=self.machine_id,
            disk_id=self.disk_id,
            grpc_target=self.grpc_target,
            state=self.state,
            total_bytes=self.total_bytes,
            used_bytes=self.used_bytes,
            available_bytes=self.available_bytes,
        )


class NodeRegistry:

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

        logger.info(
            "NodeRegistry iniciado",
            extra={
                "event": "node_registry.started",
                "suspect_timeout_s": suspect_timeout_seconds,
                "dead_timeout_s": dead_timeout_seconds,
            },
        )

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
                if existing.boot_id != boot_id:
                    logger.info(
                        "Nó reiniciou com novo boot_id",
                        extra={
                            "event": "node.restarted",
                            "node_id": node_id,
                            "old_boot_id": existing.boot_id,
                            "new_boot_id": boot_id,
                        },
                    )
                new_state = STATE_ALIVE if existing.state != STATE_DRAINING else STATE_DRAINING
                existing.boot_id = boot_id
                existing.grpc_target = grpc_target
                existing.total_bytes = total_bytes
                existing.used_bytes = used_bytes
                existing.available_bytes = max(0, total_bytes - used_bytes)
                existing.last_heartbeat = time.monotonic()
                old_state = existing.state
                existing.state = new_state
                if old_state != new_state:
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
                    "Nó registrado",
                    extra={
                        "event": "node.registered",
                        "node_id": node_id,
                        "machine_id": machine_id,
                        "disk_id": disk_id,
                        "grpc_target": grpc_target,
                        "total_bytes": total_bytes,
                    },
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
                    "Heartbeat de nó desconhecido",
                    extra={"event": "node.heartbeat_unknown", "node_id": node_id},
                )
                return False
            if entry.boot_id != boot_id:
                logger.warning(
                    "Heartbeat com boot_id stale — nó deve re-registrar",
                    extra={
                        "event": "node.heartbeat_stale_boot",
                        "node_id": node_id,
                        "expected_boot_id": entry.boot_id,
                        "received_boot_id": boot_id,
                    },
                )
                return False

            old_state = entry.state
            entry.last_heartbeat = time.monotonic()
            entry.total_bytes = total_bytes
            entry.used_bytes = used_bytes
            entry.available_bytes = available_bytes

            if old_state in (STATE_SUSPECT, STATE_DEAD):
                entry.state = STATE_ALIVE
                logger.info(
                    "Nó recuperou heartbeat — voltou para ALIVE",
                    extra={
                        "event": "node.recovered",
                        "node_id": node_id,
                        "old_state": old_state,
                    },
                )
                self._notify_state_change(node_id, old_state, STATE_ALIVE)

            return True

    def set_draining(self, node_id: str) -> bool:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                return False
            old_state = entry.state
            entry.state = STATE_DRAINING
            logger.info(
                "Nó marcado como DRAINING",
                extra={
                    "event": "node.draining",
                    "node_id": node_id,
                    "old_state": old_state,
                },
            )
            self._notify_state_change(node_id, old_state, STATE_DRAINING)
            return True

    def set_alive(self, node_id: str) -> bool:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                return False
            old_state = entry.state
            entry.state = STATE_ALIVE
            self._notify_state_change(node_id, old_state, STATE_ALIVE)
            return True

    def add_state_listener(self, fn) -> None:
        self._state_listeners.append(fn)

    def list_eligible_nodes(self) -> list[NodeInfo]:
        with self._lock:
            result = []
            for entry in self._nodes.values():
                if entry.state != STATE_ALIVE:
                    continue
                if entry.total_bytes > 0:
                    usage_fraction = entry.used_bytes / entry.total_bytes
                    if usage_fraction >= self._max_usage:
                        continue
                result.append(entry.to_node_info())
            return result

    def get_node_state(self, node_id: str) -> Optional[str]:
        with self._lock:
            entry = self._nodes.get(node_id)
            return entry.state if entry else None

    def get_node_capacity(self, node_id: str) -> Optional[tuple[int, int, int]]:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                return None
            return (entry.total_bytes, entry.used_bytes, entry.available_bytes)

    def get_all_nodes(self) -> list[NodeInfo]:
        with self._lock:
            return [e.to_node_info() for e in self._nodes.values()]

    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        with self._lock:
            entry = self._nodes.get(node_id)
            return entry.to_node_info() if entry else None

    def get_node_grpc_target(self, node_id: str) -> Optional[str]:
        with self._lock:
            entry = self._nodes.get(node_id)
            return entry.grpc_target if entry else None

    def _monitor_loop(self) -> None:
        check_interval = max(1.0, self._heartbeat_interval / 2)
        while True:
            time.sleep(check_interval)
            self._check_timeouts()

    def _check_timeouts(self) -> None:
        now = time.monotonic()
        with self._lock:
            for entry in list(self._nodes.values()):
                if entry.state == STATE_DRAINING:
                    continue 

                elapsed = now - entry.last_heartbeat
                old_state = entry.state

                if elapsed >= self._dead_timeout and entry.state != STATE_DEAD:
                    entry.state = STATE_DEAD
                    logger.warning(
                        "Nó marcado como DEAD por timeout",
                        extra={
                            "event": "node.dead",
                            "node_id": entry.node_id,
                            "elapsed_s": round(elapsed, 1),
                            "dead_timeout_s": self._dead_timeout,
                        },
                    )
                    self._notify_state_change(entry.node_id, old_state, STATE_DEAD)

                elif (
                    elapsed >= self._suspect_timeout
                    and entry.state == STATE_ALIVE
                ):
                    entry.state = STATE_SUSPECT
                    logger.warning(
                        "Nó marcado como SUSPECT por timeout",
                        extra={
                            "event": "node.suspect",
                            "node_id": entry.node_id,
                            "elapsed_s": round(elapsed, 1),
                            "suspect_timeout_s": self._suspect_timeout,
                        },
                    )
                    self._notify_state_change(entry.node_id, old_state, STATE_SUSPECT)

    def _notify_state_change(self, node_id: str, old_state: str, new_state: str) -> None:
        for fn in self._state_listeners:
            try:
                fn(node_id, old_state, new_state)
            except Exception as exc:
                logger.error(
                    "Erro em listener de estado",
                    extra={
                        "event": "node_registry.listener_error",
                        "node_id": node_id,
                        "error": str(exc),
                    },
                )
