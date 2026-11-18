from __future__ import annotations

import time
from types import SimpleNamespace

from controller.node_registry import NodeRegistry
from controller.recovery import RecoveryManager


class StubMetadata:
    def __init__(self):
        self.under_replicated: list[dict] = []
        self.healthy_sources: dict[str, list] = {}
        self.destinations: dict[str, object] = {}
        self.invalid_calls: list[tuple[str, str]] = []
        self.recovered_calls: list[tuple[str, object]] = []
        self.gc_list: list[dict] = []
        self.gc_acks: list[tuple[str, str]] = []
        self.recovery_states: list[tuple[str, bool]] = []
        self.blocks_on_node: dict[str, list[dict]] = {}

    def list_under_replicated_blocks(self):
        return list(self.under_replicated)

    def list_healthy_sources(self, block_id: str):
        return self.healthy_sources.get(block_id, [])

    def select_destination(self, block_id: str, excluded_node_ids: set[str]):
        return self.destinations.get(block_id)

    def mark_replica_invalid(self, block_id: str, node_id: str):
        self.invalid_calls.append((block_id, node_id))

    def register_recovered_replica(self, block_id: str, receipt):
        self.recovered_calls.append((block_id, receipt))

    def list_blocks_on_node(self, node_id: str):
        return self.blocks_on_node.get(node_id, [])

    def gc_candidates(self):
        return list(self.gc_list)

    def acknowledge_gc(self, block_id: str, node_id: str):
        self.gc_acks.append((block_id, node_id))

    def set_recovery_state(self, block_id: str, recovering: bool):
        self.recovery_states.append((block_id, recovering))


def make_node_addr(node_id: str, grpc_target: str = "storage-1:50052"):
    return SimpleNamespace(
        node_id=node_id,
        machine_id="m1",
        disk_id="d1",
        grpc_target=grpc_target,
    )


def make_block_record(block_id: str, live_count: int = 1, desired_count: int = 3) -> dict:
    return {
        "block_id": block_id,
        "manifest": SimpleNamespace(
            block_id=block_id,
            stored_size_bytes=4 * 1024 * 1024,
            storage_sha256=b"\xaa" * 32,
        ),
        "version": 1,
        "desired_replica_count": desired_count,
        "live_replica_count": live_count,
        "replicas": [],
    }


def make_registry_with_nodes(*node_ids: str) -> NodeRegistry:
    reg = NodeRegistry(
        heartbeat_interval_seconds=1,
        suspect_timeout_seconds=2,
        dead_timeout_seconds=5,
    )
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
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        rm.handle_corruption_report("blk-1", "n1", b"\x00" * 32, "reporter-1")
        time.sleep(0.3)
        assert ("blk-1", "n1") in meta.invalid_calls
        rm.stop()

    def test_corruption_aciona_recovery(self):
        meta = StubMetadata()
        meta.under_replicated = [make_block_record("blk-1")]
        meta.healthy_sources["blk-1"] = [make_node_addr("n1", "n1:50052")]
        meta.destinations["blk-1"] = make_node_addr("n2", "n2:50052")
        reg = make_registry_with_nodes("n1", "n2")
        rm = RecoveryManager(meta, reg, poll_interval_s=100, grpc_timeout_s=5)
        rm.start()
        rm.handle_corruption_report("blk-1", "n1", b"\x00" * 32, "reporter-1")
        time.sleep(0.5)
        assert ("blk-1", True) in meta.recovery_states
        assert ("blk-1", False) in meta.recovery_states
        rm.stop()


class TestDrainNode:
    def test_drain_registra_job_e_retorna_id(self):
        meta = StubMetadata()
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        job_id = rm.drain_node("n1")
        assert job_id
        time.sleep(0.2)
        rm.stop()

    def test_drain_no_sem_blocos_completa_rapidamente(self):
        meta = StubMetadata()
        meta.blocks_on_node["n1"] = []
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        job_id = rm.drain_node("n1")
        assert job_id
        time.sleep(0.3)
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
        meta.under_replicated = [make_block_record("blk-dup")]
        reg = make_registry_with_nodes("n1")
        rm = RecoveryManager(meta, reg, poll_interval_s=100)
        rm.start()
        rm._in_flight_blocks.add("blk-dup")
        rm._scan_and_recover()
        time.sleep(0.2)
        assert rm.running_job_count() == 0
        rm.stop()
