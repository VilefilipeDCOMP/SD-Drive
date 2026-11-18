from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
from concurrent import futures

from controller.node_registry import NodeRegistry
from controller.recovery import RecoveryManager
from controller.service import ControlServicer


def _load_runtime_config(config_path: str | None = None, argv: list[str] | None = None):
    try:
        from common.config import add_config_arguments, load_config, load_from_args
        if argv is not None:
            parser = argparse.ArgumentParser(prog="central")
            add_config_arguments(parser)
            args = parser.parse_args(argv)
            if config_path and not args.config:
                args.config = config_path
            return load_from_args(args)
        return load_config(config_path)
    except ImportError:
        import yaml
        path = config_path or os.getenv("DCOMP_CONFIG") or "config.example.yaml"
        if not os.path.exists(path):
            return {}
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}


def _cfg_dict(cfg) -> dict:
    if hasattr(cfg, "as_dict"):
        return cfg.as_dict()
    return dict(cfg)


def build_and_run(metadata_port=None, config_path: str | None = None, argv: list[str] | None = None) -> int:
    try:
        from common.logging import configure_from_config, configure_logging, get_logger
        configure_logging()
        log = get_logger("central")
    except ImportError:
        import logging
        configure_from_config = None
        log = logging.getLogger("central")

    try:
        cfg = _load_runtime_config(config_path=config_path, argv=argv)
    except Exception as exc:
        if hasattr(log, "error"):
            try:
                log.error("config.invalid", problems=[str(exc)])
            except TypeError:
                log.error("config.invalid: %s", exc)
        return 2

    cfg_map = _cfg_dict(cfg)
    if configure_from_config is not None and hasattr(cfg, "observability"):
        configure_from_config(cfg.observability)
        log.info("config.loaded", source=getattr(cfg, "source", None), config=cfg_map)

    ctrl_cfg = cfg_map.get("controller", {})
    cluster_cfg = cfg_map.get("cluster", {})
    obs_cfg = cfg_map.get("observability", {})

    registry = NodeRegistry(
        heartbeat_interval_seconds=ctrl_cfg.get("heartbeat_interval_seconds", 3),
        suspect_timeout_seconds=ctrl_cfg.get("suspect_timeout_seconds", 10),
        dead_timeout_seconds=ctrl_cfg.get("dead_timeout_seconds", 30),
        max_disk_usage_fraction=cluster_cfg.get("max_disk_usage_fraction", 0.90),
    )

    import grpc
    import dcomp_drive_pb2_grpc as rpc

    interceptors = []
    metrics = None
    try:
        from common.metrics import Metrics, MetricsInterceptor, StatusCollector, start_metrics_server
        metrics = Metrics()
        interceptors.append(MetricsInterceptor(metrics, "central"))
    except ImportError:
        StatusCollector = None
        start_metrics_server = None

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=interceptors,
        options=[
            ("grpc.max_receive_message_length", 4 * 1024 * 1024 + 65536),
            ("grpc.max_send_message_length", 4 * 1024 * 1024 + 65536),
        ],
    )

    if metadata_port is None:
        try:
            from metadata.bootstrap import register_metadata
            metadata_port = register_metadata(server, registry, cfg_map)
        except ImportError:
            metadata_port = _StubMetadataPort()

    recovery = RecoveryManager(
        metadata_port=metadata_port,
        node_registry=registry,
        max_concurrent_recoveries=ctrl_cfg.get("max_concurrent_recoveries", 2),
        poll_interval_s=5.0,
        gc_interval_s=60.0,
        metrics=metrics,
    )

    servicer = ControlServicer(
        node_registry=registry,
        metadata_port=metadata_port,
        recovery_manager=recovery,
    )
    rpc.add_ControlServiceServicer_to_server(servicer, server)

    bind_address = ctrl_cfg.get("grpc_bind", os.getenv("DCOMP_CONTROLLER_BIND", "0.0.0.0:50051"))
    if server.add_insecure_port(bind_address) == 0:
        return 1

    if metrics is not None and StatusCollector is not None and start_metrics_server is not None:
        try:
            from observability.status_cli import status_to_dict
            from google.protobuf.empty_pb2 import Empty
            metrics.registry.register(
                StatusCollector(lambda: status_to_dict(servicer.GetStatus(Empty(), None)))
            )
        except Exception:
            pass
        start_metrics_server(obs_cfg.get("metrics_port", 9100), metrics)

    server.start()
    recovery.start()

    try:
        log.info("service.started", role="central", grpc_bind=bind_address, advertise_addr=bind_address)
    except TypeError:
        log.info("service.started %s", bind_address)

    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    stop_event.wait()

    try:
        log.info("service.stopping", reason="sinal de parada")
    except TypeError:
        log.info("service.stopping")

    recovery.stop()
    server.stop(grace=5).wait()
    return 0


class _StubMetadataPort:
    def list_under_replicated_blocks(self):
        return []

    def list_healthy_sources(self, block_id: str):
        return []

    def select_destination(self, block_id: str, excluded_node_ids):
        return None

    def mark_replica_invalid(self, block_id: str, node_id: str):
        pass

    def register_recovered_replica(self, block_id: str, receipt):
        pass

    def list_blocks_on_node(self, node_id: str):
        return []

    def gc_candidates(self):
        return []

    def acknowledge_gc(self, block_id: str, node_id: str):
        pass

    def set_recovery_state(self, block_id: str, recovering: bool):
        pass


if __name__ == "__main__":
    raise SystemExit(build_and_run(argv=sys.argv[1:]))
