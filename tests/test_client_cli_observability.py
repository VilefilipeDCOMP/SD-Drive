"""
Testes da Interface de Linha de Comando (CLI Typer) e Observabilidade JSONL do Cliente.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
from typer.testing import CliRunner

import dcomp_drive_pb2 as pb
from client.cli import app as cli_app
from client.controller_client import ControllerClient
from client.service import ClientService

runner = CliRunner()


def test_cli_help_lists_all_ten_commands():
    """Verifica que o menu --help apresenta todos os 10 comandos da CLI."""
    res = runner.invoke(cli_app, ["--help"])
    assert res.exit_code == 0
    for cmd in ("put", "get", "delete", "rename", "ls", "mkdir", "rmdir", "stat", "drain", "status"):
        assert cmd in res.stdout


def test_cli_all_commands_and_jsonl_observability_events(tmp_path: Path):
    """Executa os 10 comandos da CLI e valida a gravação estruturada no arquivo JSONL."""
    events_file = tmp_path / "logs" / "client.jsonl"
    in_file = tmp_path / "local.txt"
    in_file.write_bytes(b"conteudo de teste cli")
    out_file = tmp_path / "baixado.txt"

    fake_ctrl = MagicMock(spec=ControllerClient)
    fake_ctrl.list_dir.return_value = pb.ListDirResponse(
        entries=[
            pb.DirectoryEntry(path="/docs", is_directory=True, size_bytes=0),
            pb.DirectoryEntry(path="/local.txt", is_directory=False, size_bytes=21),
        ]
    )
    fake_ctrl.stat.return_value = pb.StatResponse(
        is_directory=False,
        file=pb.FileMetadata(
            file_id="f-1",
            path="/local.txt",
            version=1,
            plaintext_size_bytes=21,
            plaintext_sha256=b"a" * 32,
            encryption=pb.ENCRYPTION_NONE,
            state=pb.FILE_COMMITTED,
            replication_state=pb.REPLICATION_HEALTHY,
            desired_replica_count=3,
        ),
        chunks=[],
    )
    fake_ctrl.drain_node.return_value = pb.DrainNodeResponse(job_id="drain-job-01")
    fake_ctrl.get_status.return_value = pb.GetStatusResponse(
        healthy_files=10,
        under_replicated_files=1,
        recovering_files=1,
        unavailable_files=0,
        running_recovery_jobs=1,
        nodes=[
            pb.NodeStatus(
                node=pb.NodeAddress(node_id="n1", grpc_target="storage-1:50052"),
                state=pb.NODE_ALIVE,
                total_bytes=10 * 1024 * 1024,
                used_bytes=2 * 1024 * 1024,
                available_bytes=8 * 1024 * 1024,
            )
        ],
    )

    fake_svc = MagicMock(spec=ClientService)
    meta = pb.FileMetadata(
        file_id="f-1",
        path="/local.txt",
        version=1,
        plaintext_size_bytes=21,
        plaintext_sha256=b"a" * 32,
        encryption=pb.ENCRYPTION_NONE,
        state=pb.FILE_COMMITTED,
    )
    fake_svc.upload_file.return_value = meta
    fake_svc.download_file.return_value = meta

    opts = ["--events-path", str(events_file)]
    with patch("client.cli._get_controller", return_value=fake_ctrl), patch(
        "client.cli._get_service", return_value=fake_svc
    ):
        assert runner.invoke(cli_app, opts + ["put", str(in_file), "/local.txt"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["get", "/local.txt", str(out_file)]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["ls", "/"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["stat", "/local.txt"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["mkdir", "/docs"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["rmdir", "--recursive", "/docs"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["rename", "/local.txt", "/novo.txt"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["delete", "/novo.txt"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["drain", "n1"]).exit_code == 0
        assert runner.invoke(cli_app, opts + ["status"]).exit_code == 0

    lines = events_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 10
    ops = [json.loads(line)["operation"] for line in lines]
    assert ops == ["put", "get", "ls", "stat", "mkdir", "rmdir", "rename", "delete", "drain", "status"]


def test_cli_error_exit_code_and_failure_event_logging(tmp_path: Path):
    """Verifica que falhas gRPC retornam código de saída 1 e gravam error_code no JSONL."""
    events_file = tmp_path / "err_events.jsonl"
    fake_ctrl = MagicMock(spec=ControllerClient)

    rpc_err = grpc.RpcError()
    rpc_err.code = lambda: grpc.StatusCode.NOT_FOUND
    rpc_err.details = lambda: "Arquivo ausente"
    fake_ctrl.stat.side_effect = rpc_err

    with patch("client.cli._get_controller", return_value=fake_ctrl):
        res = runner.invoke(cli_app, ["--events-path", str(events_file), "stat", "/inexistente.txt"])

    assert res.exit_code == 1
    assert "Recurso não encontrado" in res.output

    ev = json.loads(events_file.read_text(encoding="utf-8").strip())
    assert ev["operation"] == "stat"
    assert ev["success"] is False
    assert ev["error_code"] == "NOT_FOUND"

