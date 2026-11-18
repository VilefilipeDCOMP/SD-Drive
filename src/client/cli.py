from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

_SRC_DIR = str(Path(__file__).resolve().parent.parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import grpc
import typer

import dcomp_drive_pb2 as pb
from client.config import ClientConfig, load_client_config
from client.controller_client import ControllerClient, format_rpc_error
from client.crypto import CryptoError
from client.observability import OperationTracker
from client.service import (
    ClientService,
    ClientServiceError,
    DownloadError,
    InsufficientNodesError,
    IntegrityError,
    QuorumError,
    UploadError,
)
from client.storage_client import StorageClient

app = typer.Typer(
    name="sd-drive",
    help="SD-Drive: Cliente CLI para Sistema Distribuído de Armazenamento de Arquivos",
    add_completion=False,
)

cli_state = {
    "controller_target": None,
    "config_path": None,
    "events_path": None,
}


def _get_config() -> ClientConfig:
    cfg = load_client_config(cli_state["config_path"])
    if cli_state["controller_target"]:
        cfg.central_target = cli_state["controller_target"]
    if cli_state["events_path"]:
        cfg.client_events_path = cli_state["events_path"]
    return cfg


def _get_service(cfg: Optional[ClientConfig] = None) -> ClientService:
    config = cfg or _get_config()
    controller = ControllerClient(
        target=config.central_target,
        default_timeout=config.upload_rpc_deadline_seconds,
    )
    storage = StorageClient(
        default_timeout=config.upload_rpc_deadline_seconds
    )
    return ClientService(config=config, controller=controller, storage=storage)


def _get_controller(cfg: Optional[ClientConfig] = None) -> ControllerClient:
    config = cfg or _get_config()
    return ControllerClient(
        target=config.central_target,
        default_timeout=config.upload_rpc_deadline_seconds,
    )


def _rpc_error_code(exc: grpc.RpcError) -> str:
    if hasattr(exc, "code") and callable(exc.code):
        c = exc.code()
        return c.name if hasattr(c, "name") else str(c)
    return "RpcError"


@app.callback()
def main(
    controller: Optional[str] = typer.Option(
        None,
        "--controller",
        "-c",
        help="Endereço gRPC do processo central (padrão: controller:50051 ou definido no config).",
    ),
    config: Optional[str] = typer.Option(
        None,
        "--config",
        help="Caminho para arquivo de configuração YAML (ex: config.yaml).",
    ),
    events_path: Optional[str] = typer.Option(
        None,
        "--events-path",
        help="Caminho do arquivo JSONL para eventos de observabilidade (ex: /logs/client.jsonl).",
    ),
) -> None:
    cli_state["controller_target"] = controller
    cli_state["config_path"] = config
    cli_state["events_path"] = events_path




# PUT
@app.command("put", help="Envia um arquivo local para o sistema .")
def put(
    local_path: str = typer.Argument(..., help="Caminho do arquivo no sistema de arquivos local"),
    remote_path: str = typer.Argument(..., help="Caminho de destino no sistema (ex: /docs/arquivo.pdf)"),
    key: Optional[str] = typer.Option(
        None,
        "--key",
        "-k",
        help="Chave de criptografia AES-256 (32 bytes ou hex de 64 caracteres). Se omitida, usa DCOMP_ENCRYPTION_KEY.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Sobrescreve o arquivo no destino se já existir uma versão COMMITTED.",
    ),
    expected_version: Optional[int] = typer.Option(
        None,
        "--expected-version",
        help="Versão esperada do arquivo para atualização condicional (Compare-and-Swap).",
    ),
) -> None:
    cfg = _get_config()
    with OperationTracker("put", events_path=cfg.client_events_path) as tracker:
        service = _get_service(cfg)
        try:
            local = Path(local_path)
            if not local.is_file():
                tracker.record_error("FileNotFoundError")
                typer.secho(f"Erro: Arquivo local não encontrado: {local_path}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=1)

            typer.echo(f"Iniciando upload de '{local_path}' para '{remote_path}'...")
            meta = service.upload_file(
                local_path=local,
                remote_path=remote_path,
                overwrite=overwrite,
                expected_version=expected_version,
                key=key,
            )

            tracker.set_bytes(meta.plaintext_size_bytes)
            typer.secho(
                f"Upload concluído com sucesso!",
                fg=typer.colors.GREEN,
                bold=True,
            )
            typer.echo(f"  Caminho:    {meta.path}")
            typer.echo(f"  Versão:     {meta.version}")
            typer.echo(f"  Tamanho:    {meta.plaintext_size_bytes} bytes")
            typer.echo(f"  SHA-256:    {meta.plaintext_sha256.hex()}")
            enc_str = "AES-256-GCM" if meta.encryption == pb.ENCRYPTION_AES_256_GCM else "Nenhuma"
            typer.echo(f"  Criptografia: {enc_str}")
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro de comunicação gRPC: {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        except (ClientServiceError, CryptoError, ValueError) as err:
            tracker.record_error(type(err).__name__)
            typer.secho(f"Erro no upload: {err}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            service.close()




# GET
@app.command("get", help="Baixa um arquivo para o disco local.")
def get(
    remote_path: str = typer.Argument(..., help="Caminho do arquivo no sistema "),
    local_path: str = typer.Argument(..., help="Caminho local onde o arquivo será salvo"),
    key: Optional[str] = typer.Option(
        None,
        "--key",
        "-k",
        help="Chave de criptografia AES-256 (se o arquivo tiver sido cifrado).",
    ),
) -> None:
    cfg = _get_config()
    with OperationTracker("get", events_path=cfg.client_events_path) as tracker:
        service = _get_service(cfg)
        try:
            typer.echo(f"Iniciando download de '{remote_path}' para '{local_path}'...")
            meta = service.download_file(
                remote_path=remote_path,
                destination=local_path,
                key=key,
            )

            tracker.set_bytes(meta.plaintext_size_bytes)
            typer.secho(
                f"Download concluído com sucesso!",
                fg=typer.colors.GREEN,
                bold=True,
            )
            typer.echo(f"  Origem:     {remote_path}")
            typer.echo(f"  Destino:    {local_path}")
            typer.echo(f"  Tamanho:    {meta.plaintext_size_bytes} bytes")
            typer.echo(f"  Versão:     {meta.version}")
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro de comunicação gRPC: {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        except (ClientServiceError, CryptoError, ValueError) as err:
            tracker.record_error(type(err).__name__)
            typer.secho(f"Erro no download: {err}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            service.close()

#DELEYE
@app.command("delete", help="Remove logicamente um arquivo no namespace (marca como DELETED).")
def delete(
    path: str = typer.Argument(..., help="Caminho do arquivo no sistema"),
    expected_version: Optional[int] = typer.Option(
        None,
        "--expected-version",
        help="Versão esperada do arquivo (CAS) para exclusão segura.",
    ),
) -> None:
    cfg = _get_config()
    with OperationTracker("delete", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            controller.delete(path=path, expected_version=expected_version)
            typer.secho(f"Arquivo '{path}' excluído com sucesso.", fg=typer.colors.GREEN)
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao excluir '{path}': {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#RENAME
@app.command("rename", help="Renomeia ou move um arquivo ou diretório.")
def rename(
    source_path: str = typer.Argument(..., help="Caminho de origem"),
    target_path: str = typer.Argument(..., help="Caminho de destino"),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Sobrescreve o destino se já existir.",
    ),
) -> None:
    cfg = _get_config()
    with OperationTracker("rename", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            controller.rename(
                source_path=source_path,
                target_path=target_path,
                overwrite=overwrite,
            )
            typer.secho(
                f"'{source_path}' renomeado com sucesso para '{target_path}'.",
                fg=typer.colors.GREEN,
            )
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao renomear: {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#LS
@app.command("ls", help="Lista entradas de arquivos e diretórios visíveis.")
def ls(
    path: str = typer.Argument("/", help="Caminho do diretório (padrão: '/')"),
    page_size: int = typer.Option(100, "--page-size", help="Número máximo de itens por página"),
    page_token: str = typer.Option("", "--page-token", help="Token de paginação para consultar próxima página"),
) -> None:
    cfg = _get_config()
    with OperationTracker("ls", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            resp = controller.list_dir(
                path=path,
                page_size=page_size,
                page_token=page_token,
            )

            entries = list(resp.entries)
            if not entries:
                typer.echo(f"Nenhum item encontrado em '{path}'.")
                return

            typer.secho(f"Conteúdo de '{path}' ({len(entries)} itens):", bold=True)
            for entry in entries:
                if entry.is_directory:
                    typer.secho(f"  [DIR]  {entry.path}", fg=typer.colors.BLUE, bold=True)
                else:
                    typer.echo(f"  [FILE] {entry.path:<40} ({entry.size_bytes} bytes)")

            if resp.next_page_token:
                typer.secho(
                    f"\nPróxima página disponível: --page-token '{resp.next_page_token}'",
                    fg=typer.colors.YELLOW,
                )
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao listar '{path}': {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#MKDIR
@app.command("mkdir", help="Cria um novo diretório no sistema.")
def mkdir(
    path: str = typer.Argument(..., help="Caminho do diretório a ser criado (ex: /Maior_do_Brasil/BOTAFOGO)"),
) -> None:
    cfg = _get_config()
    with OperationTracker("mkdir", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            controller.mkdir(path=path)
            typer.secho(f"Diretório '{path}' criado com sucesso.", fg=typer.colors.GREEN)
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao criar diretório '{path}': {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#RMDIR
@app.command("rmdir", help="Remove um diretório no sistema.")
def rmdir(
    path: str = typer.Argument(..., help="Caminho do diretório a remover"),
    recursive: bool = typer.Option(
        False,
        "--recursive",
        "-r",
        help="Remove recursivamente o diretório e todo o seu conteúdo.",
    ),
) -> None:
    cfg = _get_config()
    with OperationTracker("rmdir", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            controller.rmdir(path=path, recursive=recursive)
            typer.secho(f"Diretório '{path}' removido com sucesso.", fg=typer.colors.GREEN)
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao remover diretório '{path}': {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#STAT
@app.command("stat", help="Exibe detalhes, metadados e localização de blocos de um arquivo ou diretório.")
def stat(
    path: str = typer.Argument(..., help="Caminho do arquivo ou diretório a consultar"),
) -> None:
    cfg = _get_config()
    with OperationTracker("stat", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            resp = controller.stat(path=path)
            if resp.is_directory:
                typer.secho(f"[DIRETÓRIO] {path}", fg=typer.colors.BLUE, bold=True)
                return

            f = resp.file
            typer.secho(f"[ARQUIVO] {f.path}", fg=typer.colors.GREEN, bold=True)
            typer.echo(f"   File ID:           {f.file_id}")
            typer.echo(f"   Versão:            {f.version}")
            typer.echo(f"   Tamanho original:  {f.plaintext_size_bytes} bytes")
            typer.echo(f"   SHA-256 original:  {f.plaintext_sha256.hex()}")
            enc_name = pb.EncryptionMode.Name(f.encryption) if hasattr(pb, "EncryptionMode") else str(f.encryption)
            typer.echo(f"   Criptografia:      {enc_name}")
            state_name = pb.FileState.Name(f.state) if hasattr(pb, "FileState") else str(f.state)
            typer.echo(f"   Estado:            {state_name}")
            rep_name = pb.ReplicationState.Name(f.replication_state) if hasattr(pb, "ReplicationState") else str(f.replication_state)
            typer.echo(f"   Replicação:        {rep_name} (Desejado: {f.desired_replica_count})")

            chunks = list(resp.chunks)
            typer.secho(f"\n  Blocos Lógicos ({len(chunks)} blocos):", bold=True)
            for cl in chunks:
                m = cl.manifest
                typer.echo(
                    f"    - Bloco #{m.index} (id: {m.block_id[:8]}..., armazenado: {m.stored_size_bytes} bytes, hash: {m.storage_sha256.hex()[:12]}...)"
                )
                for r in cl.replicas:
                    health_str = "Saudável" if r.healthy else "Degradada/Suspeita"
                    typer.echo(
                        f"         Réplica no nó {r.node.node_id} ({r.node.grpc_target}) — [{health_str}]"
                    )
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao consultar stat de '{path}': {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#DRAIN
@app.command("drain", help="Solicita o esvaziamento de um nó de armazenamento.")
def drain(
    node_id: str = typer.Argument(..., help="Identificador UUID do nó a ser esvaziado"),
) -> None:
    cfg = _get_config()
    with OperationTracker("drain", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            typer.echo(f"Enviando solicitação de drain para o nó '{node_id}'...")
            resp = controller.drain_node(node_id=node_id)
            typer.secho(
                f"Drenagem do nó iniciada com sucesso!",
                fg=typer.colors.GREEN,
                bold=True,
            )
            typer.echo(f"  Nó alvo:  {node_id}")
            typer.echo(f"  Job ID:   {resp.job_id}")
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao solicitar drain do nó '{node_id}': {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()

#STATUS
@app.command("status", help="Consulta o status geral do cluster de armazenamento e saúde dos arquivos.")
def status() -> None:
    cfg = _get_config()
    with OperationTracker("status", events_path=cfg.client_events_path) as tracker:
        controller = _get_controller(cfg)
        try:
            resp = controller.get_status()

            typer.secho("==================================================", fg=typer.colors.CYAN)
            typer.secho("              STATUS DO CLUSTER SD-DRIVE          ", fg=typer.colors.CYAN, bold=True)
            typer.secho("==================================================", fg=typer.colors.CYAN)

            typer.secho("\n Arquivos:", bold=True)
            typer.echo(f"   Íntegros (Healthy):        {resp.healthy_files}")
            typer.echo(f"   Sub-replicados (Under):    {resp.under_replicated_files}")
            typer.echo(f"   Em recuperação:            {resp.recovering_files}")
            typer.echo(f"   Indisponíveis:             {resp.unavailable_files}")
            typer.echo(f"   Jobs de recuperação ativos: {resp.running_recovery_jobs}")

            nodes = list(resp.nodes)
            typer.secho(f"\n  Nós de Armazenamento ({len(nodes)} registrados):", bold=True)
            for ns in nodes:
                st_name = pb.NodeState.Name(ns.state) if hasattr(pb, "NodeState") else str(ns.state)
                color = typer.colors.GREEN if st_name == "NODE_ALIVE" else typer.colors.YELLOW
                total_mb = ns.total_bytes / (1024 * 1024)
                used_mb = ns.used_bytes / (1024 * 1024)
                avail_mb = ns.available_bytes / (1024 * 1024)
                typer.echo(f"   Nó: {ns.node.node_id} ({ns.node.grpc_target})")
                typer.secho(f"    Estado: {st_name}", fg=color)
                typer.echo(f"    Capacidade: {used_mb:.1f} MiB usados / {avail_mb:.1f} MiB livres / {total_mb:.1f} MiB total")
            typer.echo()
        except grpc.RpcError as rpc_err:
            tracker.record_error(_rpc_error_code(rpc_err))
            typer.secho(f"Erro ao consultar status do cluster: {format_rpc_error(rpc_err)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)
        finally:
            controller.close()


if __name__ == "__main__":
    app()
