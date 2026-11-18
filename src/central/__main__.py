"""ponto de entrada do processo central.

sobe o MetadataService (pessoa 1) e o ControlService (pessoa 3) no mesmo servidor grpc.
configuração via variáveis de ambiente ou config.yaml.

uso:
    python -m central
    docker compose up central
"""
from __future__ import annotations

import os
import signal
import sys
import threading
from concurrent import futures

try:
    import grpc
    import dcomp_drive_pb2_grpc as pb2_grpc
except ImportError as exc:
    print(f"stubs grpc não encontrados: {exc}", file=sys.stderr)
    print("rode: python scripts/gerar_proto.py", file=sys.stderr)
    sys.exit(1)

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

from common.logging import get_logger, configure_logging
from metadata.bootstrap import register_metadata
from controller.node_registry import NodeRegistry
from controller.recovery import RecoveryManager
from controller.service import ControlServicer

configure_logging()
logger = get_logger("central")


def _load_config() -> dict:
    """lê config.yaml (ou config.example.yaml) e aplica variáveis de ambiente."""
    path = os.getenv("DCOMP_CONFIG", "config.yaml")
    cfg: dict = {}
    if yaml is not None:
        for candidate in (path, "config.example.yaml"):
            if os.path.exists(candidate):
                with open(candidate, encoding="utf-8") as f:
                    cfg = yaml.safe_load(f) or {}
                break

    ctrl = cfg.setdefault("controller", {})
    ctrl.setdefault("grpc_bind", os.getenv("DCOMP_CONTROLLER_BIND", "0.0.0.0:50051"))
    ctrl.setdefault("heartbeat_interval_seconds", 3)
    ctrl.setdefault("suspect_timeout_seconds", 10)
    ctrl.setdefault("dead_timeout_seconds", 30)
    ctrl.setdefault("max_concurrent_recoveries", 2)
    return cfg


def main() -> None:
    cfg = _load_config()
    ctrl = cfg["controller"]

    logger.info(
        "central.starting",
        bind=ctrl["grpc_bind"],
        heartbeat_interval_s=ctrl["heartbeat_interval_seconds"],
    )

    # --- node registry (pessoa 3) ---
    registry = NodeRegistry(
        heartbeat_interval_seconds=ctrl["heartbeat_interval_seconds"],
        suspect_timeout_seconds=ctrl["suspect_timeout_seconds"],
        dead_timeout_seconds=ctrl["dead_timeout_seconds"],
        max_disk_usage_fraction=cfg.get("cluster", {}).get("max_disk_usage_fraction", 0.90),
    )

    # --- servidor grpc ---
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=20),
        options=[
            ("grpc.max_receive_message_length", 4 * 1024 * 1024 + 65536),
            ("grpc.max_send_message_length", 4 * 1024 * 1024 + 65536),
        ],
    )

    # --- metadata service (pessoa 1) ---
    metadata_service = register_metadata(server, registry, cfg)

    # --- recovery manager + control service (pessoa 3) ---
    recovery = RecoveryManager(
        metadata_port=metadata_service,
        node_registry=registry,
        max_concurrent_recoveries=ctrl["max_concurrent_recoveries"],
        poll_interval_s=5.0,
        gc_interval_s=60.0,
    )
    recovery.start()

    control_servicer = ControlServicer(
        node_registry=registry,
        metadata_port=metadata_service,
        recovery_manager=recovery,
    )
    pb2_grpc.add_ControlServiceServicer_to_server(control_servicer, server)

    # --- inicia servidor ---
    bind = ctrl["grpc_bind"]
    server.add_insecure_port(bind)
    server.start()
    logger.info("central.started", bind=bind)

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        logger.info("central.stopping", signal=signum)
        recovery.stop()
        server.stop(grace=5)
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    stop_event.wait()
    logger.info("central.stopped")


if __name__ == "__main__":
    main()
