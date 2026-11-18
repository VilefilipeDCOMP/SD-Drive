import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

from observability.faults import (
    FaultError,
    FaultInjector,
    load_fault_log,
    main as fault_main,
)


class FakeDocker:
    def __init__(self, *, container_id: str = "abc123def4567890", node_id: str = "node-uuid-3", fail_on: str | None = None):
        self.container_id = container_id
        self.node_id = node_id
        self.fail_on = fail_on
        self.calls: list[list[str]] = []

    def __call__(self, cmd, timeout=None):
        argv = list(cmd)
        self.calls.append(argv)
        if self.fail_on and self.fail_on in argv:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="simulacao de erro no docker")
        if argv[:4] == ["docker", "compose", "ps", "-a"]:
            return subprocess.CompletedProcess(argv, 0, stdout=self.container_id + "\n", stderr="")
        if argv[:3] == ["docker", "exec", self.container_id] and argv[3:] == ["cat", "/data/node_id"]:
            return subprocess.CompletedProcess(argv, 0, stdout=self.node_id + "\n", stderr="")
        if "python" in argv and "-c" in argv:
            # roda o snippet real de corrupcao no python local para validar de ponta a ponta
            idx = argv.index("-c")
            real = subprocess.run([sys.executable, *argv[idx:]], capture_output=True, text=True, check=False)
            return subprocess.CompletedProcess(argv, real.returncode, stdout=real.stdout, stderr=real.stderr)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")


def test_kill_detecta_node_id_e_grava_marcador(tmp_path: Path):
    log_file = tmp_path / "faults.jsonl"
    docker = FakeDocker()
    inj = FaultInjector(log_path=log_file, runner=docker, service_instance="host-teste")

    rec = inj.kill("storage-3", note="queda no teste v3")

    assert rec["event"] == "fault.injected"
    assert rec["kind"] == "kill"
    assert rec["target"] == "storage-3"
    assert rec["node_id"] == "node-uuid-3"
    assert rec["note"] == "queda no teste v3"
    assert ["docker", "compose", "kill", "-s", "SIGKILL", "storage-3"] in docker.calls

    saved = load_fault_log(log_file)
    assert len(saved) == 1 and saved[0]["fault_id"] == rec["fault_id"]


def test_pause_le_node_id_antes_de_congelar_e_unpause_reverte(tmp_path: Path):
    log_file = tmp_path / "faults.jsonl"
    docker = FakeDocker()
    inj = FaultInjector(log_path=log_file, runner=docker)

    paused = inj.pause("storage-2", fault_id="f-1")
    unpaused = inj.unpause("storage-2", fault_id="f-1")

    # garante que o cat /data/node_id rodou antes do comando pause
    cat_idx = next(i for i, c in enumerate(docker.calls) if "cat" in c)
    pause_idx = next(i for i, c in enumerate(docker.calls) if "pause" in c)
    assert cat_idx < pause_idx
    assert paused["event"] == "fault.injected" and unpaused["event"] == "fault.reverted"
    assert len(load_fault_log(log_file)) == 2


def test_disconnect_e_reconnect_mantem_alias_dns(tmp_path: Path):
    log_file = tmp_path / "faults.jsonl"
    docker = FakeDocker(container_id="1122334455667788")
    inj = FaultInjector(log_path=log_file, runner=docker)

    inj.disconnect("storage-4")
    inj.reconnect("storage-4")

    assert ["docker", "network", "disconnect", "-f", "dcomp_net", "1122334455667788"] in docker.calls
    assert ["docker", "network", "connect", "--alias", "storage-4", "dcomp_net", "1122334455667788"] in docker.calls


def test_netem_e_netem_clear_usam_tc_como_root(tmp_path: Path):
    log_file = tmp_path / "faults.jsonl"
    docker = FakeDocker(container_id="cid99")
    inj = FaultInjector(log_path=log_file, runner=docker)

    rec = inj.netem("storage-1", delay_ms=150, jitter_ms=20, loss_percent=2.5)
    inj.netem_clear("storage-1")

    assert rec["delay_ms"] == 150 and rec["loss_percent"] == 2.5
    assert ["docker", "exec", "-u", "root", "cid99", "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", "delay", "150ms", "20ms", "loss", "2.5%"] in docker.calls
    assert ["docker", "exec", "-u", "root", "cid99", "tc", "qdisc", "del", "dev", "eth0", "root"] in docker.calls


def test_netem_rejeita_parametros_invalidos(tmp_path: Path):
    inj = FaultInjector(log_path=tmp_path / "faults.jsonl", runner=FakeDocker())
    with pytest.raises(FaultError):
        inj.netem("storage-1", delay_ms=0, loss_percent=0)
    with pytest.raises(FaultError):
        inj.netem("storage-1", delay_ms=100, loss_percent=120)


def test_corrupt_inverte_um_byte_sem_alterar_tamanho(tmp_path: Path):
    blocks_dir = tmp_path / "blocks" / "ab"
    blocks_dir.mkdir(parents=True)
    # arquivo temporario deve ser ignorado na busca pelo block_id
    (blocks_dir / "bloco-abc.tmp").write_bytes(b"temporario")
    block_file = blocks_dir / "bloco-abc"
    original = b"conteudo integro do bloco de teste"
    block_file.write_bytes(original)

    docker = FakeDocker()
    inj = FaultInjector(log_path=tmp_path / "faults.jsonl", runner=docker)
    rec = inj.corrupt("storage-2", "bloco-abc", byte_offset=3, blocks_dir=str(tmp_path / "blocks"))

    corrupted = block_file.read_bytes()
    assert len(corrupted) == len(original)
    assert corrupted != original
    assert corrupted[3] == (original[3] ^ 0xFF)
    assert rec["sha256_before"] == hashlib.sha256(original).hexdigest()
    assert rec["sha256_after"] == hashlib.sha256(corrupted).hexdigest()


def test_erro_no_docker_nao_grava_marcador_falso(tmp_path: Path):
    log_file = tmp_path / "faults.jsonl"
    inj = FaultInjector(log_path=log_file, runner=FakeDocker(fail_on="kill"))
    with pytest.raises(FaultError):
        inj.kill("storage-inexistente")
    assert not log_file.exists()


def test_dry_run_e_cli_mark_e_list(tmp_path: Path, capsys):
    log_file = tmp_path / "faults.jsonl"
    docker = FakeDocker()

    assert fault_main(["--log", str(log_file), "--dry-run", "kill", "storage-1"], runner=docker) == 0
    assert not log_file.exists() and docker.calls == []

    assert fault_main(["--log", str(log_file), "--note", "manual", "mark", "stop", "storage-5"], runner=docker) == 0
    assert fault_main(["--log", str(log_file), "list"], runner=docker) == 0
    out = capsys.readouterr().out
    assert "fault.injected" in out and "storage-5" in out

