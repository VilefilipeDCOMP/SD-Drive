"""coleta, ordenacao cronologica, filtragem e resumo de logs jsonl dos servicos.

junta duas fontes complementares:
1. saida padrao dos conteineres via `docker compose logs --no-color --no-log-prefix`
2. arquivos locais em `logs/*.jsonl` (como `logs/faults.jsonl` e `logs/client.jsonl`)

ordena tudo pelo timestamp real em utc (mesmo quando formatos diferentes de iso-8601 aparecem
misturados), remove duplicatas exatas e calcula janelas de deteccao de falha, recuperacao e
desempenho de operacoes da cli.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import fnmatch
import json
import math
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from common.logging import RESERVED_FIELDS, utc_timestamp

LEVEL_ORDER = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "WARN": 30, "ERROR": 40, "CRITICAL": 50}

MILESTONE_PATTERNS = (
    "fault.*",
    "node.state_changed",
    "node.reboot_detected",
    "block.corruption_detected",
    "corruption.reported",
    "recovery.job_started",
    "recovery.job_completed",
    "recovery.job_failed",
    "recovery.cluster_healthy",
    "reconcile.completed",
    "drain.started",
    "drain.completed",
    "client.retry",
    "client.replica_fallback",
)

NODE_FAILURE_KINDS = {"kill", "stop", "pause", "disconnect"}
_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$", re.IGNORECASE)

ComposeRunner = Callable[[Sequence[str], float | None], subprocess.CompletedProcess[str]]


class LogCollectorError(RuntimeError):
    """erro ao coletar logs do docker ou ler arquivos de entrada."""


def default_compose_runner(cmd: Sequence[str], timeout: float | None = 60.0) -> subprocess.CompletedProcess[str]:
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
        raise LogCollectorError(f"comando nao encontrado: {cmd[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise LogCollectorError(f"tempo esgotado ({timeout}s) ao executar {' '.join(cmd)}") from exc


def parse_timestamp(value: Any) -> float | None:
    """converte iso-8601 (com Z, +00:00 ou outro fuso) em segundos epoch utc."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = _dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed.timestamp()


def parse_time_spec(spec: str, *, now: float | None = None) -> float:
    """interpreta duracao relativa (ex: 5m, 30s, 2h) ou timestamp iso-8601."""
    match = _DURATION_RE.match(spec)
    if match:
        amount = float(match.group(1))
        unit = match.group(2).lower()
        factor = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
        base = time.time() if now is None else now
        return base - (amount * factor)
    epoch = parse_timestamp(spec)
    if epoch is None:
        raise ValueError(f"formato de tempo invalido: {spec!r} (use ex: 5m, 30s ou 2026-11-17T22:00:00Z)")
    return epoch


def _strip_compose_prefix(line: str) -> tuple[str, str]:
    # trata saidas salvas sem --no-log-prefix, ex: "storage-1-1  | {"ts":...}"
    if line.lstrip().startswith("{"):
        return "", line.strip()
    prefix, sep, rest = line.partition("|")
    if sep and "{" in rest:
        clean_prefix = prefix.strip()
        # nomes gerados pelo compose costumam terminar com -1; mantemos legivel
        return clean_prefix, rest.strip()
    return "", line.strip()


def _unwrap_nested_json(doc: dict[str, Any]) -> dict[str, Any]:
    # alguns modulos serializam json dentro de logging.info(json.dumps(...)), virando log.record
    if doc.get("event") != "log.record":
        return doc
    msg = doc.get("msg")
    if not isinstance(msg, str) or not msg.lstrip().startswith("{"):
        return doc
    try:
        inner = json.loads(msg)
    except json.JSONDecodeError:
        return doc
    if not isinstance(inner, dict) or "event" not in inner:
        return doc
    merged: dict[str, Any] = {}
    merged["ts"] = inner.get("ts") or doc.get("ts")
    merged["level"] = str(inner.get("level") or doc.get("level") or "INFO").upper()
    merged["component"] = inner.get("component") or doc.get("component") or "unknown"
    merged["event"] = inner["event"]
    if doc.get("service_instance"):
        merged["service_instance"] = doc["service_instance"]
    for key, value in inner.items():
        if key not in merged:
            merged[key] = value
    return merged


def parse_log_line(
    raw_line: str,
    *,
    default_instance: str = "",
    include_raw: bool = False,
    unwrap_nested: bool = True,
) -> dict[str, Any] | None:
    """transforma uma linha de texto em dict estruturado; ignora lixo nao-json por padrao."""
    stripped = raw_line.strip()
    if not stripped:
        return None
    prefix, candidate = _strip_compose_prefix(stripped)
    if candidate.startswith("{"):
        try:
            doc = json.loads(candidate)
        except json.JSONDecodeError:
            doc = None
        if isinstance(doc, dict):
            if unwrap_nested:
                doc = _unwrap_nested_json(doc)
            if "level" in doc and isinstance(doc["level"], str):
                doc["level"] = doc["level"].upper()
            if not doc.get("service_instance") and (prefix or default_instance):
                doc["service_instance"] = prefix or default_instance
            return doc
    if not include_raw:
        return None
    return {
        "ts": utc_timestamp(),
        "level": "INFO",
        "component": "raw",
        "event": "log.raw",
        "service_instance": prefix or default_instance or "unknown",
        "msg": stripped,
    }


def collect_from_compose(
    *,
    services: Sequence[str] = (),
    since: str | None = None,
    compose_file: str | None = None,
    runner: ComposeRunner = default_compose_runner,
    include_raw: bool = False,
) -> list[dict[str, Any]]:
    cmd = ["docker", "compose"]
    if compose_file:
        cmd.extend(["-f", compose_file])
    cmd.extend(["logs", "--no-color", "--no-log-prefix"])
    if since:
        cmd.extend(["--since", since])
    cmd.extend(services)
    proc = runner(cmd, 60.0)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise LogCollectorError(f"falha em {' '.join(cmd)} (codigo {proc.returncode}): {detail}")
    records: list[dict[str, Any]] = []
    # junta stdout e stderr caso algum servico escreva logs no stderr
    combined = "\n".join(part for part in (proc.stdout, proc.stderr) if part)
    for line in combined.splitlines():
        parsed = parse_log_line(line, include_raw=include_raw)
        if parsed is not None:
            records.append(parsed)
    return records


def collect_from_paths(
    paths: Iterable[str | os.PathLike[str]],
    *,
    include_raw: bool = False,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    files: list[Path] = []
    for item in paths:
        p = Path(item)
        if not p.exists():
            continue
        if p.is_dir():
            files.extend(sorted(f for f in p.glob("*.jsonl") if f.is_file()))
        elif p.is_file():
            files.append(p)
    for file in files:
        default_inst = file.stem
        for line in file.read_text(encoding="utf-8", errors="replace").splitlines():
            parsed = parse_log_line(line, default_instance=default_inst, include_raw=include_raw)
            if parsed is not None:
                records.append(parsed)
    return records


def merge_and_sort(batches: Iterable[Iterable[dict[str, Any]]]) -> list[dict[str, Any]]:
    """remove linhas identicas e ordena por timestamp utc preservando a ordem relativa."""
    seen: set[str] = set()
    indexed: list[tuple[int, float, int, dict[str, Any]]] = []
    seq = 0
    for batch in batches:
        for rec in batch:
            fingerprint = json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            epoch = parse_timestamp(rec.get("ts"))
            # registros sem timestamp valido ficam no final, na ordem em que chegaram
            missing_ts = 1 if epoch is None else 0
            indexed.append((missing_ts, 0.0 if epoch is None else epoch, seq, rec))
            seq += 1
    indexed.sort(key=lambda item: (item[0], item[1], item[2]))
    return [item[3] for item in indexed]


@dataclass(frozen=True)
class LogFilter:
    components: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    min_level: str | None = None
    node_id: str | None = None
    block_id: str | None = None
    file_id: str | None = None
    upload_id: str | None = None
    operation_id: str | None = None
    path: str | None = None
    target: str | None = None
    grep: str | None = None
    since_epoch: float | None = None
    until_epoch: float | None = None
    limit: int = 0

    def matches(self, rec: Mapping[str, Any]) -> bool:
        if self.min_level:
            rec_lvl = LEVEL_ORDER.get(str(rec.get("level", "INFO")).upper(), 20)
            req_lvl = LEVEL_ORDER.get(self.min_level.upper(), 20)
            if rec_lvl < req_lvl:
                return False

        if self.components:
            comp = str(rec.get("component", ""))
            if not any(comp == c or comp.startswith(f"{c}.") or fnmatch.fnmatchcase(comp, c) for c in self.components):
                return False

        if self.events:
            ev = str(rec.get("event", ""))
            if not any(ev == pattern or fnmatch.fnmatchcase(ev, pattern) for pattern in self.events):
                return False

        if self.since_epoch is not None or self.until_epoch is not None:
            epoch = parse_timestamp(rec.get("ts"))
            if epoch is None:
                return False
            if self.since_epoch is not None and epoch < self.since_epoch:
                return False
            if self.until_epoch is not None and epoch > self.until_epoch:
                return False

        if self.node_id and not _matches_node(rec, self.node_id):
            return False

        for field_name, expected in (
            ("block_id", self.block_id),
            ("file_id", self.file_id),
            ("upload_id", self.upload_id),
            ("operation_id", self.operation_id),
            ("path", self.path),
            ("target", self.target),
        ):
            if expected and str(rec.get(field_name, "")) != expected:
                return False

        if self.grep:
            haystack = json.dumps(rec, ensure_ascii=False, default=str).lower()
            if self.grep.lower() not in haystack:
                return False

        return True


def _matches_node(rec: Mapping[str, Any], node_id: str) -> bool:
    for key in ("node_id", "suspected_node_id", "target_node_id", "source_node_id", "failed_node_id", "next_node_id"):
        val = rec.get(key)
        if isinstance(val, str) and (val == node_id or val.startswith(node_id)):
            return True
    excluded = rec.get("excluded_node_ids")
    if isinstance(excluded, list) and any(isinstance(x, str) and (x == node_id or x.startswith(node_id)) for x in excluded):
        return True
    return False


def filter_records(records: Iterable[dict[str, Any]], flt: LogFilter) -> list[dict[str, Any]]:
    matched = [rec for rec in records if flt.matches(rec)]
    if flt.limit > 0:
        return matched[-flt.limit:]
    return matched


def add_filter_arguments(parser: argparse.ArgumentParser) -> None:
    """adiciona flags comuns de filtragem em scripts de coleta e de consulta."""
    parser.add_argument("--component", dest="components", action="append", default=[], help="filtra componente (ex: central, storage, fault)")
    parser.add_argument("--event", dest="events", action="append", default=[], help="filtra evento; aceita coringa (ex: recovery.*, node.*)")
    parser.add_argument("--level", dest="min_level", help="nivel minimo (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--node-id", help="filtra pelo uuid ou prefixo do no")
    parser.add_argument("--block-id", help="filtra por block_id")
    parser.add_argument("--file-id", help="filtra por file_id")
    parser.add_argument("--upload-id", help="filtra por upload_id")
    parser.add_argument("--operation-id", help="filtra por operation_id")
    parser.add_argument("--path", help="filtra pelo caminho do arquivo no namespace")
    parser.add_argument("--target", help="filtra pelo alvo de falha (ex: storage-3)")
    parser.add_argument("--grep", help="busca texto livre no registro")
    parser.add_argument("--since", help="janela inicial (ex: 10m, 30s ou timestamp iso-8601)")
    parser.add_argument("--until", help="janela final (timestamp iso-8601 ou duracao relativa)")
    parser.add_argument("--limit", type=int, default=0, help="mostra apenas os N registros mais recentes")


def build_filter_from_args(args: argparse.Namespace) -> LogFilter:
    since_ep = parse_time_spec(args.since) if args.since else None
    until_ep = parse_time_spec(args.until) if args.until else None
    return LogFilter(
        components=tuple(args.components),
        events=tuple(args.events),
        min_level=args.min_level,
        node_id=args.node_id,
        block_id=args.block_id,
        file_id=args.file_id,
        upload_id=args.upload_id,
        operation_id=args.operation_id,
        path=args.path,
        target=args.target,
        grep=args.grep,
        since_epoch=since_ep,
        until_epoch=until_ep,
        limit=args.limit,
    )


def format_table_line(rec: Mapping[str, Any]) -> str:
    """uma linha curta para leitura no terminal e na demonstracao."""
    ts = str(rec.get("ts", ""))
    short_ts = ts[11:23] if len(ts) >= 23 and "T" in ts else ts[:19]
    lvl = str(rec.get("level", "INFO"))[:7].ljust(7)
    comp = str(rec.get("component", "?"))[:18].ljust(18)
    ev = str(rec.get("event", "?"))[:28].ljust(28)
    skip = set(RESERVED_FIELDS) | {"uncatalogued"}
    extras = []
    for k, v in rec.items():
        if k in skip:
            continue
        rendered = json.dumps(v, ensure_ascii=False, default=str) if not isinstance(v, str) else v
        extras.append(f"{k}={rendered}")
    if "msg" in rec:
        extras.append(f"msg={rec['msg']!r}")
    tail = " ".join(extras)
    return f"{short_ts}  {lvl}  {comp}  {ev}  {tail}".rstrip()


def _percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def analyze_timeline(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """extrai marcos, tempos de deteccao/recuperacao e estatisticas de operacoes do cliente."""
    by_comp: dict[str, int] = {}
    by_level: dict[str, int] = {}
    epochs: list[float] = []
    milestones: list[dict[str, Any]] = []

    for rec in records:
        comp = str(rec.get("component", "unknown"))
        lvl = str(rec.get("level", "INFO")).upper()
        by_comp[comp] = by_comp.get(comp, 0) + 1
        by_level[lvl] = by_level.get(lvl, 0) + 1
        ep = parse_timestamp(rec.get("ts"))
        if ep is not None:
            epochs.append(ep)
        ev = str(rec.get("event", ""))
        if any(fnmatch.fnmatchcase(ev, pat) for pat in MILESTONE_PATTERNS):
            milestones.append(dict(rec))

    fault_windows = _compute_fault_windows(records)
    client_ops = _compute_client_stats(records)

    span = round(max(epochs) - min(epochs), 3) if len(epochs) >= 2 else 0.0
    return {
        "total_events": len(records),
        "time_span_seconds": span,
        "counts_by_component": by_comp,
        "counts_by_level": by_level,
        "fault_windows": fault_windows,
        "client_operations": client_ops,
        "milestones": milestones,
    }


def _state_name(raw: Any) -> str:
    text = str(raw or "").upper()
    return text[5:] if text.startswith("NODE_") else text


def _compute_fault_windows(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for idx, rec in enumerate(records):
        if rec.get("event") != "fault.injected":
            continue
        kind = str(rec.get("kind") or rec.get("fault_type") or "")
        if kind not in NODE_FAILURE_KINDS:
            continue
        t_fault = parse_timestamp(rec.get("ts"))
        if t_fault is None:
            continue
        fault_node_id = rec.get("node_id")
        target = str(rec.get("target", ""))

        t_suspect: float | None = None
        t_dead: float | None = None
        t_healthy: float | None = None
        matched_node_id = fault_node_id

        for later in records[idx + 1:]:
            t_ev = parse_timestamp(later.get("ts"))
            if t_ev is None or t_ev < t_fault:
                continue
            ev = later.get("event")
            if ev == "node.state_changed":
                later_node = later.get("node_id")
                if fault_node_id and later_node and later_node != fault_node_id:
                    continue
                if not fault_node_id and matched_node_id and later_node and later_node != matched_node_id:
                    continue
                to_state = _state_name(later.get("to_state") or later.get("new_state"))
                if to_state == "SUSPECT" and t_suspect is None:
                    t_suspect = t_ev
                    matched_node_id = matched_node_id or later_node
                elif to_state == "DEAD" and t_dead is None:
                    t_dead = t_ev
                    matched_node_id = matched_node_id or later_node
            elif ev == "recovery.cluster_healthy" and t_healthy is None:
                # considera a recuperacao concluida quando ocorre apos a falha (e apos DEAD se houve)
                if t_dead is None or t_ev >= t_dead:
                    t_healthy = t_ev
                    break

        entry: dict[str, Any] = {
            "fault_id": rec.get("fault_id"),
            "kind": kind,
            "target": target,
            "node_id": matched_node_id,
            "injected_at": rec.get("ts"),
            "suspect_seconds": round(t_suspect - t_fault, 3) if t_suspect is not None else None,
            "detection_seconds": round(t_dead - t_fault, 3) if t_dead is not None else None,
            "recovery_seconds": round(t_healthy - t_fault, 3) if t_healthy is not None else None,
        }
        windows.append(entry)
    return windows


def _compute_client_stats(records: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for rec in records:
        if rec.get("event") not in ("client.operation_completed", "client.op_completed"):
            continue
        op = str(rec.get("operation") or rec.get("op") or "unknown")
        grouped.setdefault(op, []).append(rec)

    stats: dict[str, dict[str, Any]] = {}
    for op, items in sorted(grouped.items()):
        durations: list[float] = []
        ok_count = 0
        fail_count = 0
        bytes_ok = 0
        duration_ok_sec = 0.0
        for item in items:
            success = item.get("success")
            if success is None:
                success = str(item.get("status", "ok")).lower() == "ok"
            dur = item.get("duration_ms")
            dur_f = float(dur) if isinstance(dur, (int, float)) else None
            if dur_f is not None:
                durations.append(dur_f)
            if success:
                ok_count += 1
                b = item.get("bytes")
                if isinstance(b, (int, float)) and b > 0:
                    bytes_ok += int(b)
                    if dur_f and dur_f > 0:
                        duration_ok_sec += dur_f / 1000.0
            else:
                fail_count += 1
        durations.sort()
        avg_ms = round(sum(durations) / len(durations), 3) if durations else 0.0
        p95_ms = round(_percentile(durations, 0.95), 3) if durations else 0.0
        mib_s = round((bytes_ok / (1024 * 1024)) / duration_ok_sec, 3) if duration_ok_sec > 0 else 0.0
        stats[op] = {
            "total": len(items),
            "succeeded": ok_count,
            "failed": fail_count,
            "bytes_total": bytes_ok,
            "avg_duration_ms": avg_ms,
            "p95_duration_ms": p95_ms,
            "avg_throughput_mib_s": mib_s,
        }
    return stats


def render_timeline(summary: Mapping[str, Any]) -> str:
    lines = [
        f"Resumo dos logs: {summary['total_events']} eventos em {summary['time_span_seconds']}s",
        "Por nivel: " + (", ".join(f"{k}={v}" for k, v in sorted(summary["counts_by_level"].items())) or "nenhum"),
        "Por componente: " + (", ".join(f"{k}={v}" for k, v in sorted(summary["counts_by_component"].items())) or "nenhum"),
    ]

    windows = summary.get("fault_windows") or []
    if windows:
        lines.append("")
        lines.append("Janelas de falha e recuperacao:")
        for w in windows:
            susp = f"{w['suspect_seconds']}s" if w["suspect_seconds"] is not None else "n/d"
            det = f"{w['detection_seconds']}s" if w["detection_seconds"] is not None else "n/d"
            rec = f"{w['recovery_seconds']}s" if w["recovery_seconds"] is not None else "n/d"
            nid = f" ({str(w['node_id'])[:8]})" if w.get("node_id") else ""
            lines.append(
                f"  - [{w['injected_at']}] {w['kind']} em {w['target']}{nid}: "
                f"suspect={susp} | deteccao(DEAD)={det} | recuperacao={rec}"
            )

    ops = summary.get("client_operations") or {}
    if ops:
        lines.append("")
        lines.append("Operacoes da CLI:")
        for op, st in ops.items():
            lines.append(
                f"  - {op}: total={st['total']} (ok={st['succeeded']}, falhas={st['failed']}) | "
                f"media={st['avg_duration_ms']}ms | p95={st['p95_duration_ms']}ms | vazao={st['avg_throughput_mib_s']} MiB/s"
            )

    milestones = summary.get("milestones") or []
    if milestones:
        lines.append("")
        lines.append(f"Linha do tempo ({len(milestones)} marcos principais):")
        for item in milestones:
            lines.append("  " + format_table_line(item))
    return "\n".join(lines)
