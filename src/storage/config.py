from __future__ import annotations

import argparse
import json
import os
import socket
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

_TRUE = {"1", "true", "yes", "on", "sim"}


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    value = _env(name)
    return int(value) if value is not None else default


def _env_float(name: str, default: float) -> float:
    value = _env(name)
    return float(value) if value is not None else default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    return value.strip().lower() in _TRUE if value is not None else default


@dataclass(frozen=True)
class StorageConfig:
    data_dir: Path = Path("/data")
    block_dir: Optional[Path] = None
    bind: str = "0.0.0.0:50052"
    advertise: Optional[str] = None
    controller_target: str = "controller:50051"
    machine_id: Optional[str] = None
    disk_id: Optional[str] = None
    node_id: Optional[str] = None
    capacity_bytes: int = 10 * 1024 ** 3
    frame_size: int = 256 * 1024
    scrub_interval: float = 60.0
    scrub_max_bytes_per_sec: int = 0
    verify_on_read: bool = True
    fsync: bool = True
    delete_obsolete: bool = True
    discard_corrupt: bool = True
    max_workers: int = 64
    metrics_port: int = 9100
    copy_deadline: float = 300.0
    controller_deadline: float = 10.0
    default_heartbeat_interval: float = 3.0

    @property
    def blocks_path(self) -> Path:
        return self.block_dir if self.block_dir is not None else self.data_dir / "blocks"


def from_env(argv: Optional[Sequence[str]] = None) -> StorageConfig:
    p = argparse.ArgumentParser(prog="python -m storage", description="Nó de armazenamento DComp Drive")
    p.add_argument("--data-dir")
    p.add_argument("--block-dir")
    p.add_argument("--bind")
    p.add_argument("--advertise")
    p.add_argument("--controller")
    p.add_argument("--machine-id")
    p.add_argument("--disk-id")
    p.add_argument("--node-id")
    p.add_argument("--capacity-bytes", type=int)
    p.add_argument("--scrub-interval", type=float)
    p.add_argument("--metrics-port", type=int)
    a = p.parse_args(argv)

    block_dir = a.block_dir or _env("DCOMP_STORAGE_BLOCK_DIR")
    return StorageConfig(
        data_dir=Path(a.data_dir or _env("DCOMP_STORAGE_DATA_DIR", "/data")),
        block_dir=Path(block_dir) if block_dir else None,
        bind=a.bind or _env("DCOMP_STORAGE_BIND", "0.0.0.0:50052"),
        advertise=a.advertise or _env("DCOMP_STORAGE_ADVERTISE"),
        controller_target=a.controller or _env("DCOMP_CONTROLLER_TARGET", "controller:50051"),
        machine_id=a.machine_id or _env("DCOMP_MACHINE_ID"),
        disk_id=a.disk_id or _env("DCOMP_DISK_ID"),
        node_id=a.node_id or _env("DCOMP_NODE_ID"),
        capacity_bytes=a.capacity_bytes if a.capacity_bytes is not None
        else _env_int("DCOMP_STORAGE_CAPACITY_BYTES", 10 * 1024 ** 3),
        frame_size=_env_int("DCOMP_FRAME_SIZE_BYTES", 256 * 1024),
        scrub_interval=a.scrub_interval if a.scrub_interval is not None
        else _env_float("DCOMP_STORAGE_SCRUB_INTERVAL_SECONDS", 60.0),
        scrub_max_bytes_per_sec=_env_int("DCOMP_STORAGE_SCRUB_MAX_BYTES_PER_SEC", 0),
        verify_on_read=_env_bool("DCOMP_STORAGE_VERIFY_ON_READ", True),
        fsync=_env_bool("DCOMP_STORAGE_FSYNC", True),
        delete_obsolete=_env_bool("DCOMP_STORAGE_DELETE_OBSOLETE", True),
        discard_corrupt=_env_bool("DCOMP_STORAGE_DISCARD_CORRUPT", True),
        max_workers=_env_int("DCOMP_STORAGE_MAX_WORKERS", 64),
        metrics_port=a.metrics_port if a.metrics_port is not None else _env_int("DCOMP_METRICS_PORT", 9100),
        copy_deadline=_env_float("DCOMP_STORAGE_COPY_DEADLINE_SECONDS", 300.0),
        controller_deadline=_env_float("DCOMP_CONTROLLER_DEADLINE_SECONDS", 10.0),
        default_heartbeat_interval=_env_float("DCOMP_HEARTBEAT_INTERVAL_SECONDS", 3.0),
    )


@dataclass(frozen=True)
class Identity:
    node_id: str
    machine_id: str
    disk_id: str


def load_identity(cfg: StorageConfig) -> Identity:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.data_dir / "identity.json"
    saved: dict = {}
    if path.exists():
        try:
            saved = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            saved = {}
    host = socket.gethostname()
    node_id = cfg.node_id or saved.get("node_id") or str(uuid.uuid4())
    machine_id = cfg.machine_id or saved.get("machine_id") or f"machine-{host}"
    disk_id = cfg.disk_id or saved.get("disk_id") or f"disk-{host}"
    identity = Identity(node_id=node_id, machine_id=machine_id, disk_id=disk_id)
    new = {"node_id": node_id, "machine_id": machine_id, "disk_id": disk_id}
    if saved != new:
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(new), encoding="utf-8")
        os.replace(tmp, path)
    return identity
