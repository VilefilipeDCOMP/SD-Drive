import io
import json
from concurrent import futures

import grpc
import pytest

import dcomp_drive_pb2_grpc as rpc
from fake_control import FakeControlServicer, build_response
from observability.status_cli import human_bytes, main, render_table, run_status, status_to_dict


def test_human_bytes():
    assert human_bytes(0) == "0 B"
    assert human_bytes(1536) == "1.5 KiB"
    assert human_bytes(2 * 1024**3) == "2.0 GiB"


def test_status_to_dict_traduz_estados_e_contagens():
    status = status_to_dict(build_response(2))
    states = [n["state"] for n in status["nodes"]]
    assert states.count("DEAD") == 1 and states.count("ALIVE") == 4
    assert status["files"] == {"healthy": 10, "under_replicated": 6, "recovering": 4, "unavailable": 0}
    assert status["running_recovery_jobs"] == 2


def test_tabela_mostra_no_morto_primeiro_e_os_totais():
    text = render_table(status_to_dict(build_response(2)), "central:50051", now=0)
    lines = text.splitlines()
    first_row = next(line for line in lines if "storage-" in line)
    assert "DEAD" in first_row and "storage-3" in first_row
    assert "Nos: ALIVE 4 | SUSPECT 0 | DEAD 1 | DRAINING 0" in text
    assert "UNDER_REPLICATED 6" in text and "Re-replicacoes em andamento: 2" in text


def test_tabela_sem_nos():
    status = {"nodes": [], "files": {"healthy": 0, "under_replicated": 0, "recovering": 0, "unavailable": 0}, "running_recovery_jobs": 0}
    assert "nenhum no registrado" in render_table(status)


@pytest.fixture
def central():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    rpc.add_ControlServiceServicer_to_server(FakeControlServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"127.0.0.1:{port}"
    server.stop(0).wait()


def test_run_status_json_e_codigo_de_saida(central):
    out = io.StringIO()
    assert run_status(central, as_json=True, out=out) == 0
    assert json.loads(out.getvalue())["files"]["healthy"] == 20


def test_run_status_tabela(central):
    out = io.StringIO()
    assert run_status(central, out=out) == 0
    assert "DComp Drive" in out.getvalue() and "machine-1" in out.getvalue()


def test_central_fora_do_ar_da_codigo_2(capsys):
    assert run_status("127.0.0.1:1", timeout=0.5) == 2
    assert "nao foi possivel consultar" in capsys.readouterr().err


def test_main_usa_target_da_linha_de_comando(central, capsys):
    assert main(["--target", central, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["running_recovery_jobs"] == 0

