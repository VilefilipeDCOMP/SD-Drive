"""processos provisorios para o compose subir antes do codigo real existir.

nao implementam nenhum servico do proto: so abrem a porta grpc, geram um node_id estavel
(no caso do storage), logam no formato oficial e ficam vivos. quando o servico real de uma
pessoa ficar pronto, basta trocar o command no docker-compose.yml, por exemplo:

    command: ["python", "-m", "storage"]

uso:
    python -m placeholder central
    python -m placeholder storage
    python -m placeholder healthcheck host:porta
"""

from __future__ import annotations

import argparse
import signal
import socket
import sys
import threading
import uuid
from concurrent import futures
from pathlib import Path

import grpc

from common.config import ConfigError, add_config_arguments, load_from_args
from common.logging import configure_logging, configure_from_config, get_logger


def _healthcheck(target: str) -> int:
    # o grpc sem servicos nao responde a rpc de saude, entao basta a porta aceitar conexao
    host, _, port = target.rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port)), timeout=2):
            return 0
    except (OSError, ValueError):
        return 1


def _load_or_create_node_id(path: str, log) -> str:
    file = Path(path)
    if file.exists():
        text = file.read_text(encoding="utf-8").strip()
        if text:
            return text
    node_id = str(uuid.uuid4())
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(node_id + "\n", encoding="utf-8")
    log.info("node.identity_created", node_id=node_id, node_id_file=path)
    return node_id


def _serve(role: str, args: argparse.Namespace) -> int:
    # logging basico antes de ler a config, para um erro de config tambem sair em json
    configure_logging()
    boot = get_logger(role)
    try:
        cfg = load_from_args(args)
    except ConfigError as exc:
        boot.error("config.invalid", problems=exc.problems)
        return 2
    configure_from_config(cfg.observability)
    log = get_logger(role)
    log.info("config.loaded", source=cfg.source, config=cfg.as_dict())

    hostname = socket.gethostname()
    bind = cfg.controller.grpc_bind if role == "central" else cfg.storage.grpc_bind
    context: dict = {"role": role, "provisional": True, "grpc_bind": bind}
    if role == "storage":
        node_id = _load_or_create_node_id(cfg.storage.node_id_file, log)
        context.update(
            node_id=node_id,
            boot_id=str(uuid.uuid4()),
            machine_id=cfg.storage.resolve_machine_id(hostname),
            disk_id=cfg.storage.resolve_disk_id(hostname),
            advertise_addr=cfg.storage.resolve_advertise_addr(hostname),
        )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    if server.add_insecure_port(bind) == 0:
        log.error("service.started", msg=f"nao foi possivel abrir {bind}", **context)
        return 1
    server.start()
    log.info("service.started", msg="processo provisorio, sem servicos grpc reais", **context)

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    stop.wait()
    log.info("service.stopping", reason="sinal de parada")
    server.stop(grace=2).wait()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="placeholder")
    parser.add_argument("mode", choices=["central", "storage", "healthcheck"])
    parser.add_argument("target", nargs="?", help="host:porta, so para healthcheck")
    add_config_arguments(parser)
    args = parser.parse_args(argv)
    if args.mode == "healthcheck":
        if not args.target:
            parser.error("healthcheck precisa de host:porta")
        return _healthcheck(args.target)
    return _serve(args.mode, args)


if __name__ == "__main__":
    sys.exit(main())

