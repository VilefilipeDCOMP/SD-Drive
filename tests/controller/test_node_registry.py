from __future__ import annotations

import time
import pytest

from common.ports import NodeSnapshot
from controller.node_registry import (
    NODE_ALIVE,
    NODE_DEAD,
    NODE_DRAINING,
    NODE_STATE_UNSPECIFIED,
    NODE_SUSPECT,
    NodeRegistry,
)


def make_registry(**kwargs) -> NodeRegistry:
    defaults = dict(
        heartbeat_interval_seconds=1,
        suspect_timeout_seconds=2,
        dead_timeout_seconds=5,
        max_disk_usage_fraction=0.90,
    )
    defaults.update(kwargs)
    return NodeRegistry(**defaults)


def _register(registry: NodeRegistry, node_id: str = "node-1", **kwargs) -> dict:
    defaults = dict(
        machine_id="machine-1",
        disk_id="disk-1",
        grpc_target="storage-1:50052",
        boot_id="boot-aaa",
        total_bytes=10 * 1024 * 1024 * 1024,
        used_bytes=0,
    )
    defaults.update(kwargs)
    return registry.register_node(node_id=node_id, **defaults)


class TestRegisterNode:
    def test_register_novo_no(self):
        reg = make_registry()
        config = _register(reg)
        assert config["heartbeat_interval_seconds"] == 1
        assert config["suspect_timeout_seconds"] == 2

    def test_no_registrado_aparece_em_eligible(self):
        reg = make_registry()
        _register(reg)
        eligible = reg.list_eligible_nodes()
        assert len(eligible) == 1
        assert isinstance(eligible[0], NodeSnapshot)
        assert eligible[0].node_id == "node-1"
        assert eligible[0].state == NODE_ALIVE

    def test_re_registro_com_mesmo_boot_id_mantem_alive(self):
        reg = make_registry()
        _register(reg)
        _register(reg)
        assert reg.get_node_state("node-1") == NODE_ALIVE

    def test_re_registro_com_novo_boot_id_reseta_para_alive(self):
        reg = make_registry()
        _register(reg, boot_id="boot-aaa")
        reg._nodes["node-1"].state = NODE_SUSPECT
        _register(reg, boot_id="boot-bbb")
        assert reg.get_node_state("node-1") == NODE_ALIVE

    def test_multiplos_nos_registrados(self):
        reg = make_registry()
        _register(reg, node_id="node-1", machine_id="m1")
        _register(reg, node_id="node-2", machine_id="m2")
        eligible = reg.list_eligible_nodes()
        assert len(eligible) == 2

    def test_no_desconhecido_retorna_unspecified_e_keyerror_em_capacidade(self):
        reg = make_registry()
        assert reg.get_node_state("desconhecido") == NODE_STATE_UNSPECIFIED
        with pytest.raises(KeyError):
            reg.get_node_capacity("desconhecido")


class TestHeartbeat:
    def test_heartbeat_reconhecido(self):
        reg = make_registry()
        _register(reg)
        ok = reg.process_heartbeat("node-1", "boot-aaa", 100, 10, 90)
        assert ok is True

    def test_heartbeat_boot_id_errado_retorna_false(self):
        reg = make_registry()
        _register(reg)
        ok = reg.process_heartbeat("node-1", "boot-errado", 100, 10, 90)
        assert ok is False

    def test_heartbeat_no_desconhecido_retorna_false(self):
        reg = make_registry()
        ok = reg.process_heartbeat("inexistente", "boot-x", 100, 10, 90)
        assert ok is False

    def test_heartbeat_atualiza_capacidade(self):
        reg = make_registry()
        _register(reg)
        reg.process_heartbeat("node-1", "boot-aaa", 200, 50, 150)
        cap = reg.get_node_capacity("node-1")
        assert isinstance(cap, NodeSnapshot)
        assert (cap.total_bytes, cap.used_bytes, cap.available_bytes) == (200, 50, 150)

    def test_heartbeat_recupera_suspect_para_alive(self):
        reg = make_registry()
        _register(reg)
        reg._nodes["node-1"].state = NODE_SUSPECT
        reg.process_heartbeat("node-1", "boot-aaa", 100, 10, 90)
        assert reg.get_node_state("node-1") == NODE_ALIVE

    def test_heartbeat_nao_altera_draining(self):
        reg = make_registry()
        _register(reg)
        reg._nodes["node-1"].state = NODE_DRAINING
        reg.process_heartbeat("node-1", "boot-aaa", 100, 10, 90)
        assert reg.get_node_state("node-1") == NODE_DRAINING


class TestTimeout:
    def test_suspect_apos_timeout(self):
        reg = make_registry(suspect_timeout_seconds=0.1, dead_timeout_seconds=1.0)
        _register(reg)
        time.sleep(0.3)
        reg._check_timeouts()
        assert reg.get_node_state("node-1") == NODE_SUSPECT

    def test_dead_apos_timeout(self):
        reg = make_registry(suspect_timeout_seconds=0.1, dead_timeout_seconds=0.3)
        _register(reg)
        time.sleep(0.5)
        reg._check_timeouts()
        assert reg.get_node_state("node-1") == NODE_DEAD

    def test_draining_nao_afetado_por_timeout(self):
        reg = make_registry(suspect_timeout_seconds=0.1, dead_timeout_seconds=0.3)
        _register(reg)
        reg._nodes["node-1"].state = NODE_DRAINING
        time.sleep(0.5)
        reg._check_timeouts()
        assert reg.get_node_state("node-1") == NODE_DRAINING

    def test_listener_notificado_em_suspect(self):
        reg = make_registry(suspect_timeout_seconds=0.1, dead_timeout_seconds=1.0)
        _register(reg)
        events = []
        reg.add_state_listener(lambda nid, old, new: events.append((nid, old, new)))
        time.sleep(0.3)
        reg._check_timeouts()
        assert any(e[2] == NODE_SUSPECT for e in events)


class TestEligibility:
    def test_no_cheio_nao_elegivel(self):
        reg = make_registry()
        total = 10 * 1024 * 1024 * 1024
        _register(reg, total_bytes=total, used_bytes=int(total * 0.95))
        assert reg.list_eligible_nodes() == ()

    def test_no_suspect_nao_elegivel(self):
        reg = make_registry()
        _register(reg)
        reg._nodes["node-1"].state = NODE_SUSPECT
        assert reg.list_eligible_nodes() == ()

    def test_no_dead_nao_elegivel(self):
        reg = make_registry()
        _register(reg)
        reg._nodes["node-1"].state = NODE_DEAD
        assert reg.list_eligible_nodes() == ()

    def test_no_draining_nao_elegivel(self):
        reg = make_registry()
        _register(reg)
        reg.set_draining("node-1")
        assert reg.list_eligible_nodes() == ()


class TestDrain:
    def test_set_draining(self):
        reg = make_registry()
        _register(reg)
        result = reg.set_draining("node-1")
        assert result is True
        assert reg.get_node_state("node-1") == NODE_DRAINING

    def test_set_draining_no_inexistente(self):
        reg = make_registry()
        result = reg.set_draining("inexistente")
        assert result is False

    def test_drain_notifica_listener(self):
        reg = make_registry()
        _register(reg)
        events = []
        reg.add_state_listener(lambda nid, old, new: events.append((nid, old, new)))
        reg.set_draining("node-1")
        assert any(e[2] == NODE_DRAINING for e in events)
