from pathlib import Path

import pytest

from common.config import ConfigError, load_config

EXAMPLE = Path(__file__).resolve().parent.parent / "config.example.yaml"


def test_exemplo_carrega_com_defaults_do_contrato():
    cfg = load_config(EXAMPLE, env={})
    assert cfg.cluster.replication_factor == 3
    assert cfg.cluster.resolve_write_quorum() == 3
    assert cfg.controller.placement_eligible_node_states == ("ALIVE",)


def test_sem_arquivo_usa_defaults():
    assert load_config(env={}).controller.heartbeat_interval_seconds == 3


def test_ambiente_vence_arquivo_e_cli_vence_ambiente():
    env = {"DCOMP_CLUSTER__REPLICATION_FACTOR": "2", "DCOMP_CONFIG": str(EXAMPLE)}
    assert load_config(env=env).cluster.replication_factor == 2
    cfg = load_config(env=env, overrides={"cluster.replication_factor": "4"})
    assert cfg.cluster.replication_factor == 4
    assert cfg.cluster.resolve_write_quorum() == 4


@pytest.mark.parametrize("w,n,ok", [("all", 1, True), ("2", 3, True), ("5", 3, False), ("0", 3, False), ("tudo", 3, False)])
def test_quorum_nunca_e_reduzido_em_silencio(w, n, ok):
    env = {"DCOMP_CLUSTER__REPLICATION_FACTOR": str(n), "DCOMP_CLUSTER__WRITE_QUORUM": w}
    if ok:
        load_config(env=env)
    else:
        with pytest.raises(ConfigError):
            load_config(env=env)


def test_resolve_quorum_com_n_diferente_rejeita_w_fixo_maior():
    cfg = load_config(env={"DCOMP_CLUSTER__WRITE_QUORUM": "3"})
    assert cfg.cluster.resolve_write_quorum(3) == 3
    with pytest.raises(ValueError):
        cfg.cluster.resolve_write_quorum(2)


def test_erros_voltam_juntos_com_sugestao_de_chave():
    env = {
        "DCOMP_CLUSTER__REPLICATION_FACTR": "3",
        "DCOMP_CONTROLLER__SUSPECT_TIMEOUT_SECONDS": "40",
        "DCOMP_CLIENT__ENCRYPTION_ENABLED": "talvez",
        "DCOMP_NADA__X": "1",
    }
    with pytest.raises(ConfigError) as err:
        load_config(env=env)
    text = str(err.value)
    assert "replication_factor" in text
    assert "encryption_enabled" in text
    assert "secao desconhecida" in text
    assert len(err.value.problems) >= 3


def test_timeouts_de_heartbeat_precisam_ser_crescentes():
    with pytest.raises(ConfigError, match="dead_timeout"):
        load_config(env={"DCOMP_CONTROLLER__SUSPECT_TIMEOUT_SECONDS": "40"})


def test_so_alive_e_elegivel():
    with pytest.raises(ConfigError, match="ALIVE"):
        load_config(env={"DCOMP_CONTROLLER__PLACEMENT_ELIGIBLE_NODE_STATES": "ALIVE,SUSPECT"})


def test_frame_grande_demais_e_rejeitado():
    with pytest.raises(ConfigError, match="4 MiB"):
        load_config(env={"DCOMP_CLIENT__GRPC_FRAME_SIZE_BYTES": str(4 * 1024 * 1024)})


def test_helpers_de_identidade_do_storage():
    st = load_config(env={}).storage
    assert st.resolve_advertise_addr("storage-1") == "storage-1:50052"
    assert st.resolve_disk_id("storage-1") == "storage-1-disk0"


def test_arquivo_inexistente():
    with pytest.raises(ConfigError, match="nao encontrado"):
        load_config("nao_existe.yaml", env={})

