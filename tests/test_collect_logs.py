import json
import subprocess
from pathlib import Path

from observability.log_collector import (
    LogFilter,
    analyze_timeline,
    collect_from_compose,
    collect_from_paths,
    filter_records,
    format_table_line,
    merge_and_sort,
    parse_log_line,
    render_timeline,
)


def test_parse_aceita_prefixo_do_compose_e_desempacota_json_aninhado():
    prefixed = 'storage-1-1  | {"ts":"2026-11-17T22:00:01.000Z","level":"INFO","component":"storage","event":"block.stored","block_id":"b1"}'
    doc = parse_log_line(prefixed)
    assert doc["event"] == "block.stored"
    assert doc["service_instance"] == "storage-1-1"

    # formato emitido quando um modulo chama logging.info(json.dumps(...)) sob o JsonFormatter
    inner = json.dumps({
        "ts": "2026-11-17T22:00:02.500000+00:00",
        "component": "metadata",
        "event": "metadata.operation_completed",
        "level": "INFO",
        "operation": "commit_upload",
    })
    wrapped = json.dumps({
        "ts": "2026-11-17T22:00:02.501Z",
        "level": "INFO",
        "component": "dcomp_drive",
        "event": "log.record",
        "service_instance": "central",
        "msg": inner,
    })
    unwrapped = parse_log_line(wrapped)
    assert unwrapped["component"] == "metadata"
    assert unwrapped["event"] == "metadata.operation_completed"
    assert unwrapped["operation"] == "commit_upload"
    assert unwrapped["service_instance"] == "central"

    assert parse_log_line("linha solta de boot") is None
    raw = parse_log_line("linha solta de boot", include_raw=True, default_instance="central")
    assert raw["event"] == "log.raw" and raw["service_instance"] == "central"


def test_merge_and_sort_ordena_fusos_diferentes_e_remove_duplicatas():
    # 22:00:02Z e anterior a 19:00:03-03:00 (= 22:00:03Z)
    r_late = {"ts": "2026-11-17T19:00:03.000-03:00", "level": "INFO", "component": "storage", "event": "block.served"}
    r_early = {"ts": "2026-11-17T22:00:02.100Z", "level": "INFO", "component": "fault", "event": "fault.injected"}
    merged = merge_and_sort([[r_late, r_early], [dict(r_early)]])
    assert [r["event"] for r in merged] == ["fault.injected", "block.served"]


def test_filtros_por_coringa_componente_nivel_e_node_id():
    records = [
        {"ts": "2026-11-17T22:00:00Z", "level": "DEBUG", "component": "storage", "event": "heartbeat.sent", "node_id": "n-1"},
        {"ts": "2026-11-17T22:00:01Z", "level": "INFO", "component": "central.control", "event": "recovery.job_started", "target_node_id": "n-222"},
        {"ts": "2026-11-17T22:00:02Z", "level": "WARNING", "component": "client", "event": "client.retry", "excluded_node_ids": ["n-333"]},
    ]
    assert len(filter_records(records, LogFilter(min_level="INFO"))) == 2
    assert len(filter_records(records, LogFilter(components=("central",), events=("recovery.*",)))) == 1
    assert len(filter_records(records, LogFilter(node_id="n-2"))) == 1
    assert len(filter_records(records, LogFilter(node_id="n-333"))) == 1


def test_timeline_calcula_deteccao_recuperacao_e_estatisticas_da_cli():
    records = [
        {"ts": "2026-11-17T22:10:00.000Z", "level": "INFO", "component": "fault", "event": "fault.injected", "fault_id": "f1", "kind": "kill", "target": "storage-3", "node_id": "node-3"},
        {"ts": "2026-11-17T22:10:10.200Z", "level": "WARNING", "component": "central.control", "event": "node.state_changed", "node_id": "node-3", "from_state": "ALIVE", "to_state": "SUSPECT"},
        {"ts": "2026-11-17T22:10:30.500Z", "level": "ERROR", "component": "central.control", "event": "node.state_changed", "node_id": "node-3", "from_state": "SUSPECT", "to_state": "DEAD"},
        {"ts": "2026-11-17T22:10:31.000Z", "level": "INFO", "component": "central.control", "event": "recovery.job_started", "block_id": "b1", "target_node_id": "node-5"},
        {"ts": "2026-11-17T22:10:34.250Z", "level": "INFO", "component": "central.control", "event": "recovery.cluster_healthy", "recovered_blocks": 4},
        {"ts": "2026-11-17T22:10:40.000Z", "level": "INFO", "component": "client", "event": "client.operation_completed", "operation": "put", "success": True, "bytes": 4 * 1024 * 1024, "duration_ms": 500.0},
        {"ts": "2026-11-17T22:10:41.000Z", "level": "ERROR", "component": "client", "event": "client.operation_completed", "operation": "put", "success": False, "bytes": 0, "duration_ms": 1500.0, "error_code": "UNAVAILABLE"},
    ]
    summary = analyze_timeline(records)
    assert len(summary["fault_windows"]) == 1
    win = summary["fault_windows"][0]
    assert win["suspect_seconds"] == 10.2
    assert win["detection_seconds"] == 30.5
    assert win["recovery_seconds"] == 34.25

    put_stats = summary["client_operations"]["put"]
    assert put_stats["total"] == 2 and put_stats["succeeded"] == 1 and put_stats["failed"] == 1
    assert put_stats["avg_throughput_mib_s"] == 8.0

    text = render_timeline(summary)
    assert "deteccao(DEAD)=30.5s" in text and "recuperacao=34.25s" in text
    assert "vazao=8.0 MiB/s" in text
    assert "fault.injected" in format_table_line(records[0])


def test_scripts_collect_logs_e_logq_ponta_a_ponta(tmp_path: Path, capsys):
    # testa o fluxo completo juntando saida simulada do docker compose com logs/faults.jsonl
    import importlib.util

    root = Path(__file__).resolve().parents[1]

    def load_script(rel: str):
        spec = importlib.util.spec_from_file_location(Path(rel).stem, root / rel)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    collect_mod = load_script("scripts/collect_logs.py")
    logq_mod = load_script("scripts/logq.py")

    faults_file = tmp_path / "faults.jsonl"
    faults_file.write_text(
        json.dumps({"ts": "2026-11-17T22:00:00.000Z", "level": "INFO", "component": "fault", "event": "fault.injected", "kind": "kill", "target": "storage-1", "node_id": "n1"}) + "\n",
        encoding="utf-8",
    )
    compose_stdout = "\n".join([
        json.dumps({"ts": "2026-11-17T22:00:25.000Z", "level": "ERROR", "component": "central.control", "event": "node.state_changed", "node_id": "n1", "to_state": "DEAD"}),
        json.dumps({"ts": "2026-11-17T22:00:29.000Z", "level": "INFO", "component": "central.control", "event": "recovery.cluster_healthy", "recovered_blocks": 1}),
    ])

    def fake_compose(cmd, timeout=None):
        return subprocess.CompletedProcess(list(cmd), 0, stdout=compose_stdout, stderr="")

    out_jsonl = tmp_path / "merged.jsonl"
    rc = collect_mod.main(
        ["--no-default-logs-dir", "--input", str(faults_file), "--out", str(out_jsonl)],
        runner=fake_compose,
    )
    assert rc == 0
    saved = collect_from_paths([out_jsonl])
    assert [r["event"] for r in saved] == ["fault.injected", "node.state_changed", "recovery.cluster_healthy"]
    capsys.readouterr()

    assert logq_mod.main([str(out_jsonl), "--timeline"]) == 0
    timeline_out = capsys.readouterr().out
    assert "deteccao(DEAD)=25.0s" in timeline_out and "recuperacao=29.0s" in timeline_out

