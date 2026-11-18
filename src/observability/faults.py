"""injecao controlada de falhas e registro do instante exato em jsonl.

cada comando executa a acao real nos conteineres do cluster (kill, stop, pause, particao de
rede, atraso/perda com tc netem ou corrupcao de bytes em um bloco) e grava uma linha em
logs/faults.jsonl no mesmo esquema de logs do restante do sistema.

isso garante a hora zero exata (fault.injected) e o fim da falha (fault.reverted) para
calcular os tempos de deteccao e de recuperacao nos experimentos e na demonstracao.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Sequence

from common.logging import default_service_instance, utc_timestamp

DEFAULT_FAULT_LOG = "logs/faults.jsonl"
DEFAULT_NETWORK = "dcomp_net"
DEFAULT_BLOCKS_DIR = "/data/blocks"
DEFAULT_NODE_ID_FILE = "/data/node_id"
FAULT_LOG_ENV = "DCOMP_FAULT_LOG"

Runner = Callable[[Sequence[str], float | None], subprocess.CompletedProcess[str]]

# script pequeno rodado dentro do conteiner para virar 1 byte de um bloco sem mudar o tamanho
_CORRUPT_SNIPPET = """
import hashlib, json, os, pathlib, sys
blocks_dir, target, offset = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
direct = pathlib.Path(target)
if direct.is_file():
    chosen = direct
elif (blocks_dir / target).is_file():
    chosen = blocks_dir / target
else:
    if not blocks_dir.exists():
        sys.exit(f"diretorio de blocos nao existe: {blocks_dir}")
    files = [
        p for p in blocks_dir.rglob("*")
        if p.is_file() and not p.name.endswith((".tmp", ".part"))
    ]
    exact = [p for p in files if p.name == target or p.stem == target]
    matches = exact or [p for p in files if p.name.startswith(target)]
    if not matches:
        sys.exit(f"bloco nao encontrado em {blocks_dir}: {target}")
    if len(matches) > 1:
        names = ", ".join(str(p) for p in matches[:5])
        sys.exit(f"mais de um bloco casa com {target}: {names}")
    chosen = matches[0]
raw = chosen.read_bytes()
if not raw:
    sys.exit(f"bloco vazio, nada para corromper: {chosen}")
idx = offset if offset >= 0 else len(raw) + offset
if not 0 <= idx < len(raw):
    sys.exit(f"offset {offset} fora do tamanho do bloco ({len(raw)} bytes)")
before = hashlib.sha256(raw).hexdigest()
flipped = bytes([raw[idx] ^ 0xFF])
with chosen.open("r+b") as fh:
    fh.seek(idx)
    fh.write(flipped)
    fh.flush()
    os.fsync(fh.fileno())
after = hashlib.sha256(chosen.read_bytes()).hexdigest()
print(json.dumps({
    "block_path": str(chosen),
    "size_bytes": len(raw),
    "byte_offset": idx,
    "sha256_before": before,
    "sha256_after": after,
}))
""".strip()


class FaultError(RuntimeError):
    """falha ao aplicar ou reverter uma injecao no ambiente docker."""


def default_runner(cmd: Sequence[str], timeout: float | None = 30.0) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(cmd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise FaultError(f"comando nao encontrado no sistema: {cmd[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise FaultError(f"comando excedeu o tempo limite ({timeout}s): {' '.join(cmd)}") from exc


class FaultInjector:
    """executa acoes de falha via docker/compose e registra os marcadores em jsonl."""

    def __init__(
        self,
        *,
        log_path: str | os.PathLike[str] = DEFAULT_FAULT_LOG,
        compose_file: str | None = None,
        runner: Runner = default_runner,
        dry_run: bool = False,
        service_instance: str | None = None,
    ):
        self.log_path = Path(log_path)
        self.compose_file = compose_file
        self.runner = runner
        self.dry_run = dry_run
        self.service_instance = service_instance or default_service_instance()

    def _compose_cmd(self, *args: str) -> list[str]:
        base = ["docker", "compose"]
        if self.compose_file:
            base.extend(["-f", self.compose_file])
        base.extend(args)
        return base

    def _run_checked(self, cmd: Sequence[str], *, timeout: float | None = 30.0) -> tuple[str, float]:
        if self.dry_run:
            return "", 0.0
        start = time.perf_counter()
        proc = self.runner(cmd, timeout)
        elapsed_ms = round((time.perf_counter() - start) * 1000.0, 3)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise FaultError(f"falha ao executar {' '.join(cmd)} (codigo {proc.returncode}): {detail}")
        return (proc.stdout or "").strip(), elapsed_ms

    def resolve_container(self, target: str) -> str:
        """descobre o id do conteiner pelo nome do servico no compose ou pelo nome direto no docker."""
        if self.dry_run:
            return target
        proc = self.runner(self._compose_cmd("ps", "-a", "-q", target), 10.0)
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip().splitlines()[0].strip()
        direct = self.runner(["docker", "inspect", "--format", "{{.Id}}", target], 10.0)
        if direct.returncode == 0 and direct.stdout.strip():
            return direct.stdout.strip().splitlines()[0].strip()
        raise FaultError(f"servico ou conteiner nao encontrado: {target}")

    def detect_node_id(self, container: str, node_id_file: str = DEFAULT_NODE_ID_FILE) -> str | None:
        """tenta ler o node_id persistido no volume do conteiner sem falhar se ele estiver inacessivel."""
        if self.dry_run or not container:
            return None
        try:
            proc = self.runner(["docker", "exec", container, "cat", node_id_file], 3.0)
        except FaultError:
            return None
        if proc.returncode != 0:
            return None
        value = (proc.stdout or "").strip()
        return value or None

    def record_event(
        self,
        event: str,
        *,
        kind: str,
        target: str,
        node_id: str | None = None,
        fault_id: str | None = None,
        note: str = "",
        **extra: Any,
    ) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "ts": utc_timestamp(),
            "level": "INFO",
            "component": "fault",
            "event": event,
            "service_instance": self.service_instance,
            "fault_id": fault_id or str(uuid.uuid4()),
            "kind": kind,
            "target": target,
        }
        if node_id:
            doc["node_id"] = node_id
        for key, value in extra.items():
            if value is not None:
                doc[key] = value
        if note:
            doc["note"] = note
        if self.dry_run:
            doc["dry_run"] = True
            return doc
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(doc, ensure_ascii=False, separators=(",", ":")) + "\n")
        return doc

    def _pre_node_id(self, target: str, explicit_node_id: str | None) -> tuple[str | None, str | None]:
        if explicit_node_id:
            return None, explicit_node_id
        try:
            cid = self.resolve_container(target)
        except FaultError:
            return None, None
        return cid, self.detect_node_id(cid)

    def kill(self, target: str, *, signal_name: str = "SIGKILL", node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        _, detected = self._pre_node_id(target, node_id)
        cmd = self._compose_cmd("kill", "-s", signal_name, target)
        _, elapsed_ms = self._run_checked(cmd)
        return self.record_event(
            "fault.injected", kind="kill", target=target, node_id=node_id or detected,
            fault_id=fault_id, note=note, signal=signal_name, command_duration_ms=elapsed_ms,
        )

    def stop(self, target: str, *, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        _, detected = self._pre_node_id(target, node_id)
        _, elapsed_ms = self._run_checked(self._compose_cmd("stop", target))
        return self.record_event(
            "fault.injected", kind="stop", target=target, node_id=node_id or detected,
            fault_id=fault_id, note=note, command_duration_ms=elapsed_ms,
        )

    def start(self, target: str, *, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        _, elapsed_ms = self._run_checked(self._compose_cmd("start", target))
        _, detected = self._pre_node_id(target, node_id)
        return self.record_event(
            "fault.reverted", kind="start", target=target, node_id=node_id or detected,
            fault_id=fault_id, note=note, command_duration_ms=elapsed_ms,
        )

    def restart(self, target: str, *, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        _, detected = self._pre_node_id(target, node_id)
        _, elapsed_ms = self._run_checked(self._compose_cmd("restart", target), timeout=60.0)
        return self.record_event(
            "fault.injected", kind="restart", target=target, node_id=node_id or detected,
            fault_id=fault_id, note=note, command_duration_ms=elapsed_ms,
        )

    def pause(self, target: str, *, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        # le o node_id antes do pause porque docker exec trava em conteiner congelado
        _, detected = self._pre_node_id(target, node_id)
        _, elapsed_ms = self._run_checked(self._compose_cmd("pause", target))
        return self.record_event(
            "fault.injected", kind="pause", target=target, node_id=node_id or detected,
            fault_id=fault_id, note=note, command_duration_ms=elapsed_ms,
        )

    def unpause(self, target: str, *, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        _, elapsed_ms = self._run_checked(self._compose_cmd("unpause", target))
        _, detected = self._pre_node_id(target, node_id)
        return self.record_event(
            "fault.reverted", kind="pause", target=target, node_id=node_id or detected,
            fault_id=fault_id, note=note, command_duration_ms=elapsed_ms,
        )

    def disconnect(self, target: str, *, network: str = DEFAULT_NETWORK, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        container = self.resolve_container(target)
        detected = node_id or self.detect_node_id(container)
        _, elapsed_ms = self._run_checked(["docker", "network", "disconnect", "-f", network, container])
        return self.record_event(
            "fault.injected", kind="disconnect", target=target, node_id=detected,
            fault_id=fault_id, note=note, network=network, container=container[:12], command_duration_ms=elapsed_ms,
        )

    def reconnect(self, target: str, *, network: str = DEFAULT_NETWORK, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        container = self.resolve_container(target)
        # --alias devolve o nome dns do servico na rede do compose (ex: storage-3)
        _, elapsed_ms = self._run_checked(["docker", "network", "connect", "--alias", target, network, container])
        detected = node_id or self.detect_node_id(container)
        return self.record_event(
            "fault.reverted", kind="disconnect", target=target, node_id=detected,
            fault_id=fault_id, note=note, network=network, container=container[:12], command_duration_ms=elapsed_ms,
        )

    def netem(
        self,
        target: str,
        *,
        delay_ms: int = 200,
        jitter_ms: int = 0,
        loss_percent: float = 0.0,
        interface: str = "eth0",
        node_id: str | None = None,
        fault_id: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        if delay_ms <= 0 and loss_percent <= 0:
            raise FaultError("informe delay_ms > 0 e/ou loss_percent > 0 para o netem")
        if jitter_ms < 0 or (jitter_ms > 0 and delay_ms <= 0):
            raise FaultError("jitter_ms exige delay_ms > 0 e nao pode ser negativo")
        if not 0.0 <= loss_percent <= 100.0:
            raise FaultError("loss_percent precisa ficar entre 0 e 100")

        container = self.resolve_container(target)
        detected = node_id or self.detect_node_id(container)
        # replace atualiza a regra se ja houver uma ativa sem dar erro de arquivo existente
        cmd = ["docker", "exec", "-u", "root", container, "tc", "qdisc", "replace", "dev", interface, "root", "netem"]
        if delay_ms > 0:
            cmd.extend(["delay", f"{delay_ms}ms"])
            if jitter_ms > 0:
                cmd.append(f"{jitter_ms}ms")
        if loss_percent > 0:
            cmd.extend(["loss", f"{loss_percent}%"])
        _, elapsed_ms = self._run_checked(cmd)
        return self.record_event(
            "fault.injected", kind="netem", target=target, node_id=detected,
            fault_id=fault_id, note=note, interface=interface, delay_ms=delay_ms,
            jitter_ms=jitter_ms, loss_percent=loss_percent, command_duration_ms=elapsed_ms,
        )

    def netem_clear(self, target: str, *, interface: str = "eth0", node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        container = self.resolve_container(target)
        detected = node_id or self.detect_node_id(container)
        _, elapsed_ms = self._run_checked(["docker", "exec", "-u", "root", container, "tc", "qdisc", "del", "dev", interface, "root"])
        return self.record_event(
            "fault.reverted", kind="netem", target=target, node_id=detected,
            fault_id=fault_id, note=note, interface=interface, command_duration_ms=elapsed_ms,
        )

    def corrupt(
        self,
        target: str,
        block: str,
        *,
        byte_offset: int = 0,
        blocks_dir: str = DEFAULT_BLOCKS_DIR,
        node_id: str | None = None,
        fault_id: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        container = self.resolve_container(target)
        detected = node_id or self.detect_node_id(container)
        cmd = ["docker", "exec", container, "python", "-c", _CORRUPT_SNIPPET, blocks_dir, block, str(byte_offset)]
        out, elapsed_ms = self._run_checked(cmd)
        details: dict[str, Any] = {"block_id": block, "byte_offset": byte_offset}
        if out:
            try:
                details.update(json.loads(out.splitlines()[-1]))
            except json.JSONDecodeError:
                details["raw_output"] = out
        return self.record_event(
            "fault.injected", kind="corrupt", target=target, node_id=detected,
            fault_id=fault_id, note=note, command_duration_ms=elapsed_ms, **details,
        )

    def mark(self, kind: str, target: str, *, reverted: bool = False, node_id: str | None = None, fault_id: str | None = None, note: str = "") -> dict[str, Any]:
        event = "fault.reverted" if reverted else "fault.injected"
        return self.record_event(event, kind=kind, target=target, node_id=node_id, fault_id=fault_id, note=note, manual=True)


def load_fault_log(path: str | os.PathLike[str] = DEFAULT_FAULT_LOG) -> list[dict[str, Any]]:
    file = Path(path)
    if not file.exists():
        return []
    items: list[dict[str, Any]] = []
    for raw in file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(doc, dict):
            items.append(doc)
    return items


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fault", description="injeta ou reverte falhas no cluster docker e grava logs/faults.jsonl")
    parser.add_argument("--log", default=os.environ.get(FAULT_LOG_ENV, DEFAULT_FAULT_LOG), help="arquivo jsonl de falhas (padrao: logs/faults.jsonl)")
    parser.add_argument("--compose-file", help="arquivo compose especifico (-f)")
    parser.add_argument("--node-id", help="uuid do no (se omitido, tenta ler /data/node_id do conteiner)")
    parser.add_argument("--fault-id", help="identificador para amarrar injecao e reversao")
    parser.add_argument("--note", default="", help="observacao livre gravada junto ao evento")
    parser.add_argument("--dry-run", action="store_true", help="mostra o evento sem alterar conteineres nem gravar no arquivo")
    parser.add_argument("--json", dest="as_json", action="store_true", help="imprime o evento completo em json")

    sub = parser.add_subparsers(dest="action", required=True)

    p_kill = sub.add_parser("kill", help="queda abrupta do conteiner (SIGKILL por padrao)")
    p_kill.add_argument("target")
    p_kill.add_argument("--signal", default="SIGKILL", dest="signal_name")

    for name, help_text in (
        ("stop", "parada limpa do conteiner"),
        ("start", "inicia novamente um conteiner parado"),
        ("restart", "reinicia um servico (ex: central para testar persistencia)"),
        ("pause", "congela os processos do conteiner mantendo conexoes abertas"),
        ("unpause", "descongela um conteiner pausado"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("target")

    for name, help_text in (
        ("disconnect", "isola o conteiner da rede docker"),
        ("reconnect", "reconecta o conteiner na rede docker mantendo o alias dns"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("target")
        p.add_argument("--network", default=DEFAULT_NETWORK)

    p_netem = sub.add_parser("netem", help="adiciona atraso e/ou perda de pacotes via tc netem")
    p_netem.add_argument("target")
    p_netem.add_argument("--delay-ms", type=int, default=200)
    p_netem.add_argument("--jitter-ms", type=int, default=0)
    p_netem.add_argument("--loss-percent", type=float, default=0.0)
    p_netem.add_argument("--interface", default="eth0")

    p_nclear = sub.add_parser("netem-clear", help="remove a regra tc netem da interface do conteiner")
    p_nclear.add_argument("target")
    p_nclear.add_argument("--interface", default="eth0")

    p_corrupt = sub.add_parser("corrupt", help="inverte 1 byte de um bloco no disco do conteiner sem mudar o tamanho")
    p_corrupt.add_argument("target")
    p_corrupt.add_argument("block", help="block_id ou caminho do arquivo de bloco")
    p_corrupt.add_argument("--byte-offset", type=int, default=0)
    p_corrupt.add_argument("--blocks-dir", default=DEFAULT_BLOCKS_DIR)

    p_mark = sub.add_parser("mark", help="apenas grava o marcador em faults.jsonl sem chamar o docker")
    p_mark.add_argument("kind")
    p_mark.add_argument("target")
    p_mark.add_argument("--reverted", action="store_true")

    sub.add_parser("list", help="lista os marcadores ja gravados em faults.jsonl")
    return parser


def main(argv: Sequence[str] | None = None, *, runner: Runner = default_runner) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.action == "list":
        records = load_fault_log(args.log)
        if args.as_json:
            for item in records:
                print(json.dumps(item, ensure_ascii=False))
            return 0
        if not records:
            print(f"nenhum marcador encontrado em {args.log}")
            return 0
        for item in records:
            nid = f" node_id={item['node_id'][:8]}" if item.get("node_id") else ""
            note = f" ({item['note']})" if item.get("note") else ""
            print(f"{item.get('ts', '?')}  {item.get('event', '?'):<15}  kind={item.get('kind', '?'):<10}  target={item.get('target', '?')}{nid}{note}")
        return 0

    injector = FaultInjector(
        log_path=args.log,
        compose_file=args.compose_file,
        runner=runner,
        dry_run=args.dry_run,
    )
    common = {"node_id": args.node_id, "fault_id": args.fault_id, "note": args.note}
    try:
        if args.action == "kill":
            record = injector.kill(args.target, signal_name=args.signal_name, **common)
        elif args.action == "stop":
            record = injector.stop(args.target, **common)
        elif args.action == "start":
            record = injector.start(args.target, **common)
        elif args.action == "restart":
            record = injector.restart(args.target, **common)
        elif args.action == "pause":
            record = injector.pause(args.target, **common)
        elif args.action == "unpause":
            record = injector.unpause(args.target, **common)
        elif args.action == "disconnect":
            record = injector.disconnect(args.target, network=args.network, **common)
        elif args.action == "reconnect":
            record = injector.reconnect(args.target, network=args.network, **common)
        elif args.action == "netem":
            record = injector.netem(
                args.target, delay_ms=args.delay_ms, jitter_ms=args.jitter_ms,
                loss_percent=args.loss_percent, interface=args.interface, **common,
            )
        elif args.action == "netem-clear":
            record = injector.netem_clear(args.target, interface=args.interface, **common)
        elif args.action == "corrupt":
            record = injector.corrupt(args.target, args.block, byte_offset=args.byte_offset, blocks_dir=args.blocks_dir, **common)
        elif args.action == "mark":
            record = injector.mark(args.kind, args.target, reverted=args.reverted, **common)
        else:
            parser.error(f"acao desconhecida: {args.action}")
            return 2
    except FaultError as exc:
        print(f"erro na injecao de falha: {exc}", file=sys.stderr)
        return 1

    if args.as_json:
        print(json.dumps(record, ensure_ascii=False))
    else:
        where = "dry-run" if args.dry_run else str(injector.log_path)
        nid = f" node_id={record['node_id']}" if record.get("node_id") else ""
        print(f"[{record['ts']}] {record['event']} kind={record['kind']} target={record['target']}{nid} -> {where}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

