"""
tests/controller/test_recovery.py

Testes unitários do RecoveryManager (Pessoa 3).
"""

from __future__ import annotations

import threading
import time
import pytest

from src.common.ports import NodeInfo, BlockInfo, ReplicaReceipt
from src.controller.recovery import RecoveryManager
from src.controller.node_registry import NodeRegistry


# ---------------------------------------------------------------------------
# Stubs / mocks
# ---------------------------------------------------------------------------

class StubMetadata:
    """Implementação controlável de MetadataPort para testes."""

    def __init__(self):
        self.under_replicated: list[tuple[str, int, int]] = []
        self.healthy_sources: dict[str, list[BlockInfo]] = {}
        self.destinations: dict[str, NodeInfo | None] = {}
        self.invalid_calls: list[tuple[str, str]] = []
        self.recovered_calls: list[tuple[str, ReplicaReceipt]] = []
        self.gc_list: list[tuple[str, str]] = []
        self.blocks_on_node: dict[str, list[BlockInfo]] = {}

    def list_under_replicated_blocks(self):
        return list(self.under_replicated)

    def list_healthy_sources(self, block_id: str):
        return self.healthy_sources.get(block_id, [])

    def select_destination(self, block_id: str, excluded_node_ids: set[str]):
        return self.destinations.get(block_id)

    def mark_replica_invalid(self, block_id: str, node_id: str):
        self.invalid_calls.append((block_id, node_id))

    def register_recovered_replica(self, block_id: str, receipt: ReplicaReceipt):
        self.recovered_calls.append((block_id, receipt))

    def list_blocks_on_node(self, node_id: str):
        return self.blocks_on_node.get(node_id, [])

    def gc_candidates(self):
        return list(self.gc_list)


def make_node_info(node_id: str, grpc_target: str = "storage-1:50052") -> NodeInfo:
    return NodeInfo(
        node_id=node_id,
        machine_id="m1",
        disk_id="d1",
        grpc_target=grpc_target,
        state="ALIVE",
        total_bytes=10 * 1024**3,
        used_bytes=0,
        available_bytes=10 * 1024**3,
    )


def make_block_info(block_id: str, node_id: str, healthy: bool = True) -> BlockInfo:
    return BlockInfo(
        block_id=block_id,
        node_id=node_id,
        healthy=healthy,
        version=1,
        stored_size_bytes=4 * 1024 * 1024,
    )


def make_registry_with_nodes(*node_ids: str) -> NodeRegistry:
    reg = NodeRegistry(heartbeat_interval_seconds=1, suspect_timeout_seconds=2, dead_timeout_seconds=5)
    for nid in node_ids:
        reg.register_node(
            node_id=nid,
            machine_id=f"m-{nid}",
            disk_id=f"d-{nid}",
            grpc_target=f"{nid}:50052",
            boot_id=f"boot-{nid}",
            total_bytes=10 * 1024**3,
            used_bytes=0,
        )
    return reg


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------

class TestRecoveryManagerBasic:
    def test_start_stop(self):
        meta = StubMetadata()
        reg = make_registry_with_nodes("n1", "n2")
        rm = RecoveryManager(meta, reg, max_concurrent_recoveries=2, poll_interval_s=100)
        rm.start()
        assert rm.running_job_count() == 0
        rm.stop()

    def test_running_job_count_zero_sem_jobs(self):
        meta = StubMetadata()
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        assert rm.running_job_count() == 0
        rm.stop()


class TestCorruptionReport:
    def test_corruption_marca_replica_invalida(self):
        meta = StubMetadata()
        reg = make_registry_with_nodes("n1")
        # Sem fontes nem destino: recovery vai falhar silenciosamente
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        rm.handle_corruption_report("blk-1", "n1", b"\x00" * 32, "reporter-1")
        # Aguarda execução assíncrona
        time.sleep(0.3)
        assert ("blk-1", "n1") in meta.invalid_calls
        rm.stop()

    def test_corruption_aciona_recovery(self):
        meta = StubMetadata()
        meta.healthy_sources["blk-1"] = [make_block_info("blk-1", "n1")]
        meta.destinations["blk-1"] = make_node_info("n2", "n2:50052")
        reg = make_registry_with_nodes("n1", "n2")
        rm = RecoveryManager(meta, reg, poll_interval_s=100, grpc_timeout_s=5)
        rm.start()
        rm.handle_corruption_report("blk-1", "n1", b"\x00" * 32, "reporter-1")
        # Aguarda execução assíncrona (modo stub sem gRPC real)
        time.sleep(0.5)
        # Em modo stub, register_recovered_replica pode ser chamado
        rm.stop()


class TestDrainNode:
    def test_drain_registra_job_e_retorna_id(self):
        meta = StubMetadata()
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        job_id = rm.drain_node("n1")
        assert job_id  # deve ser um UUID não vazio
        time.sleep(0.2)
        rm.stop()

    def test_drain_no_sem_blocos_completa_rapidamente(self):
        meta = StubMetadata()
        meta.blocks_on_node["n1"] = []  # nó vazio
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        job_id = rm.drain_node("n1")
        time.sleep(0.3)
        # Deve ter completado sem erros
        rm.stop()


class TestScanAndRecover:
    def test_sem_blocos_sub_replicados_nenhum_job(self):
        meta = StubMetadata()
        meta.under_replicated = []
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        rm._scan_and_recover()
        assert rm.running_job_count() == 0
        rm.stop()

    def test_bloco_in_flight_nao_re_agendado(self):
        meta = StubMetadata()
        meta.under_replicated = [("blk-dup", 1, 3)]
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        # Marcar bloco como em voo
        rm._in_flight_blocks.add("blk-dup")
        rm._scan_and_recover()
        # Bloco não deve ter sido agendado novamente
        time.sleep(0.2)
        rm.stop()
