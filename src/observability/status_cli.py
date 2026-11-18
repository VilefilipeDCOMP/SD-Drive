"""comando status (rf11): mostra nos, espaco, arquivos por estado e recuperacoes em andamento.

a unica fonte de dados e o rpc ControlService.GetStatus; o comando nunca le tabelas do central.

uso:
    python -m observability.status_cli                  # uma vez, usando client.central_target da config
    python -m observability.status_cli --json           # saida para scripts e evidencias
    python -m observability.status_cli --watch 2        # atualiza a cada 2 s (bom para a demo)
    python -m observability.status_cli --target central:50051

codigos de saida: 0 ok, 1 se existe arquivo UNAVAILABLE (util em scripts de verificacao),
2 se nao deu para falar com o central ou a configuracao e invalida.

a cli da pessoa 4 pode chamar run_status(...) direto no comando `status` dela.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Mapping, TextIO

import grpc
from google.protobuf.empty_pb2 import Empty

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc
from common.config import ConfigError, add_config_arguments, load_from_args

NODE_STATE_NAMES = {
    pb.NODE_ALIVE: "ALIVE",
    pb.NODE_SUSPECT: "SUSPECT",
    pb.NODE_DEAD: "DEAD",
    pb.NODE_DRAINING: "DRAINING",
}
DEFAULT_TIMEOUT_SECONDS = 5.0


def status_to_dict(response: pb.GetStatusResponse) -> dict[str, Any]:
    """GetStatusResponse em dict simples; e o mesmo formato que o StatusCollector das metricas espera."""
    nodes = []
    for item in response.nodes:
        nodes.append(
            {
                "node_id": item.node.node_id,
                "machine_id": item.node.machine_id,
                "disk_id": item.node.disk_id,
                "grpc_target": item.node.grpc_target,
                "state": NODE_STATE_NAMES.get(item.state, "UNSPECIFIED"),
                "total_bytes": item.total_bytes,
                "used_bytes": item.used_bytes,
                "available_bytes": item.available_bytes,
            }
        )
    return {
        "nodes": nodes,
        "files": {
            "healthy": response.healthy_files,
            "under_replicated": response.under_replicated_files,
            "recovering": response.recovering_files,
            "unavailable": response.unavailable_files,
        },
        "running_recovery_jobs": response.running_recovery_jobs,
    }


def fetch_status(target: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    with grpc.insecure_channel(target) as channel:
        stub = rpc.ControlServiceStub(channel)
        return status_to_dict(stub.GetStatus(Empty(), timeout=timeout))


def human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


def render_table(status: Mapping[str, Any], target: str = "", now: float | None = None) -> str:
    when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
    lines = [f"DComp Drive | {target or 'central'} | {when}", ""]

    header = ("NO", "MAQUINA", "DISCO", "ESTADO", "USADO / TOTAL", "USO", "ENDERECO")
    rows = []
    # dead e suspect primeiro, porque e o que interessa quando algo deu errado
    order = {"DEAD": 0, "SUSPECT": 1, "DRAINING": 2, "ALIVE": 3}
    for node in sorted(status["nodes"], key=lambda n: (order.get(n["state"], 9), n["grpc_target"])):
        total = node["total_bytes"]
        usage = f"{100 * node['used_bytes'] / total:.0f}%" if total else "-"
        rows.append(
            (
                node["grpc_target"].split(":")[0] or node["node_id"][:8],
                node["machine_id"],
                node["disk_id"],
                node["state"],
                f"{human_bytes(node['used_bytes'])} / {human_bytes(total)}",
                usage,
                node["grpc_target"],
            )
        )
    if not rows:
        lines.append("nenhum no registrado")
    else:
        widths = [max(len(str(r[i])) for r in [header, *rows]) for i in range(len(header))]
        for row in [header, *rows]:
            lines.append("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)).rstrip())

    counts = {state: 0 for state in ("ALIVE", "SUSPECT", "DEAD", "DRAINING")}
    for node in status["nodes"]:
        counts[node["state"]] = counts.get(node["state"], 0) + 1
    files = status["files"]
    lines += [
        "",
        "Nos: " + " | ".join(f"{name} {count}" for name, count in counts.items()),
        "Arquivos: "
        f"HEALTHY {files['healthy']} | UNDER_REPLICATED {files['under_replicated']} | "
        f"RECOVERING {files['recovering']} | UNAVAILABLE {files['unavailable']}",
        f"Re-replicacoes em andamento: {status['running_recovery_jobs']}",
    ]
    return "\n".join(lines)


def run_status(
    target: str,
    *,
    as_json: bool = False,
    watch: float = 0.0,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    out: TextIO | None = None,
) -> int:
    out = out or sys.stdout
    interactive = out.isatty()
    while True:
        try:
            status = fetch_status(target, timeout)
        except grpc.RpcError as exc:
            code = exc.code().name if hasattr(exc, "code") else "ERRO"
            print(f"nao foi possivel consultar o central em {target}: {code}", file=sys.stderr)
            return 2
        if as_json:
            out.write(json.dumps(status, ensure_ascii=False) + "\n")
        else:
            if watch and interactive:
                out.write("\033[2J\033[H")
            out.write(render_table(status, target) + "\n")
            if watch and not interactive:
                out.write("\n")
        out.flush()
        if not watch:
            return 1 if status["files"]["unavailable"] else 0
        try:
            time.sleep(watch)
        except KeyboardInterrupt:
            return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="status", description="estado do cluster DComp Drive (GetStatus)")
    add_config_arguments(parser)
    parser.add_argument("--target", help="host:porta do central (padrao: client.central_target)")
    parser.add_argument("--json", dest="as_json", action="store_true", help="uma linha json por consulta")
    parser.add_argument("--watch", type=float, default=0.0, metavar="SEGUNDOS", help="repete a consulta")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="deadline do rpc")
    args = parser.parse_args(argv)
    try:
        target = args.target or load_from_args(args).client.central_target
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    try:
        return run_status(target, as_json=args.as_json, watch=args.watch, timeout=args.timeout)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())

