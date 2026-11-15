"""
src/bootstrap.py

Ponto de inicialização do processo central.

Instancia todos os módulos e injeta as dependências via Protocols definidos
em src/common/ports.py, evitando imports circulares entre Pessoa 1 e Pessoa 3.

Responsabilidades:
  1. Carregar configuração (YAML + env vars)
  2. Instanciar NodeRegistry (Pessoa 3)
  3. Instanciar MetadataService (Pessoa 1) injetando NodeRegistryPort
  4. Instanciar RecoveryManager injetando MetadataPort + NodeRegistryPort
  5. Instanciar ControlServicer com as dependências
  6. Registrar ambos os serviços no servidor gRPC e iniciar

Referência: ARQUITETURA.md §Interfaces Python internas, CONTRATO.md §9.
"""

from __future__ import annotations

import os
import sys
import signal
import threading
from concurrent import futures

import yaml

from src.common.logging_config import get_logger
from src.controller.node_registry import NodeRegistry
from src.controller.recovery import RecoveryManager
from src.controller.service import ControlServicer

logger = get_logger("bootstrap")


def load_config(path: str = "config.yaml") -> dict:
    """Carrega configuração do YAML; env vars sobrescrevem valores específicos."""
    if not os.path.exists(path):
        path = "config.example.yaml"
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Variáveis de ambiente com prioridade
    ctrl = cfg.setdefault("controller", {})
    ctrl.setdefault("grpc_bind", os.getenv("DCOMP_CONTROLLER_BIND", "0.0.0.0:50051"))
    ctrl.setdefault("heartbeat_interval_seconds", 3)
    ctrl.setdefault("suspect_timeout_seconds", 10)
    ctrl.setdefault("dead_timeout_seconds", 30)
    ctrl.setdefault("max_concurrent_recoveries", 2)

    return cfg


def build_and_run(metadata_port=None, config_path: str = "config.yaml") -> None:
    """Monta e inicia o processo central.

    Args:
        metadata_port: instância de MetadataPort (Pessoa 1).
                       Se None, usa um stub para permitir desenvolvimento isolado.
        config_path: caminho para o YAML de configuração.
    """
    cfg = load_config(config_path)
    ctrl_cfg = cfg.get("controller", {})

    # --- 1. NodeRegistry (Pessoa 3) ----------------------------------------
    registry = NodeRegistry(
        heartbeat_interval_seconds=ctrl_cfg.get("heartbeat_interval_seconds", 3),
        suspect_timeout_seconds=ctrl_cfg.get("suspect_timeout_seconds", 10),
        dead_timeout_seconds=ctrl_cfg.get("dead_timeout_seconds", 30),
        max_disk_usage_fraction=cfg.get("cluster", {}).get("max_disk_usage_fraction", 0.90),
    )

    # --- 2. MetadataPort — injetado de fora (Pessoa 1) ----------------------
    if metadata_port is None:
        logger.warning(
            "MetadataPort não fornecido — usando stub de desenvolvimento",
            extra={"event": "bootstrap.stub_metadata"},
        )
        metadata_port = _StubMetadataPort()

    # --- 3. RecoveryManager -------------------------------------------------
    recovery = RecoveryManager(
        metadata_port=metadata_port,
        node_registry=registry,
        max_concurrent_recoveries=ctrl_cfg.get("max_concurrent_recoveries", 2),
        poll_interval_s=5.0,
        gc_interval_s=60.0,
    )
    recovery.start()

    # --- 4. ControlServicer -------------------------------------------------
    servicer = ControlServicer(
        node_registry=registry,
        metadata_port=metadata_port,
        recovery_manager=recovery,
    )

    # --- 5. Servidor gRPC ---------------------------------------------------
    bind_address = ctrl_cfg.get("grpc_bind", "0.0.0.0:50051")

    try:
        import grpc
        from src import dcomp_drive_pb2_grpc as pb2_grpc  # type: ignore

        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                ("grpc.max_receive_message_length", 4 * 1024 * 1024 + 65536),
                ("grpc.max_send_message_length", 4 * 1024 * 1024 + 65536),
            ],
        )
        pb2_grpc.add_ControlServiceServicer_to_server(servicer, server)

        # MetadataService registrado pela Pessoa 1 no mesmo server
        # (a Pessoa 1 recebe `server` e `registry` como NodeRegistryPort)

        server.add_insecure_port(bind_address)
        server.start()
        logger.info(
            "Processo central iniciado",
            extra={"event": "server.started", "bind": bind_address},
        )

        # Aguarda sinal de parada
        stop_event = threading.Event()

        def _handle_signal(signum, frame):
            logger.info("Sinal recebido, encerrando", extra={"event": "server.stopping"})
            recovery.stop()
            server.stop(grace=5)
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

        stop_event.wait()

    except ImportError:
        logger.warning(
            "gRPC não disponível — rodando em modo stub (apenas para testes)",
            extra={"event": "bootstrap.grpc_unavailable"},
        )
        # Mantém processo vivo para testes locais sem gRPC instalado
        threading.Event().wait()

    except Exception as exc:
        logger.error(
            "Falha ao iniciar servidor gRPC",
            extra={"event": "server.start_failed", "error": str(exc)},
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Stub para desenvolvimento isolado (sem Pessoa 1 implementada)
# ---------------------------------------------------------------------------

class _StubMetadataPort:
    """Implementação vazia de MetadataPort para desenvolvimento/testes."""

    def list_under_replicated_blocks(self):
        return []

    def list_healthy_sources(self, block_id: str):
        return []

    def select_destination(self, block_id: str, excluded_node_ids):
        return None

    def mark_replica_invalid(self, block_id: str, node_id: str):
        logger.debug("STUB: mark_replica_invalid", extra={"block_id": block_id, "node_id": node_id})

    def register_recovered_replica(self, block_id: str, receipt):
        logger.debug("STUB: register_recovered_replica", extra={"block_id": block_id})

    def list_blocks_on_node(self, node_id: str):
        return []

    def gc_candidates(self):
        return []


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    build_and_run(config_path=config_file)
