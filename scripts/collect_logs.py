"""coleta e ordena os logs json dos conteineres e dos arquivos locais em ordem cronologica.

exemplos:
    python scripts/collect_logs.py --out logs/all.jsonl
    python scripts/collect_logs.py --since 10m --out evidencias/run1/all.jsonl --timeline
    python scripts/collect_logs.py --service central --service storage-1 --table
    python scripts/collect_logs.py --no-docker --input logs --timeline
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from observability.log_collector import (
    ComposeRunner,
    LogCollectorError,
    add_filter_arguments,
    analyze_timeline,
    build_filter_from_args,
    collect_from_compose,
    collect_from_paths,
    default_compose_runner,
    filter_records,
    format_table_line,
    merge_and_sort,
    render_timeline,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="collect_logs", description="mescla logs do docker compose com logs/*.jsonl em ordem cronologica")
    parser.add_argument("--compose-file", help="arquivo docker-compose especifico (-f)")
    parser.add_argument("--service", dest="services", action="append", default=[], help="restringe servicos consultados no compose (pode repetir)")
    parser.add_argument("--no-docker", action="store_true", help="nao chama docker compose; le apenas os arquivos de --input e logs/")
    parser.add_argument("--input", dest="inputs", action="append", default=[], help="arquivo .jsonl ou pasta adicional para incluir na mesclagem")
    parser.add_argument("--no-default-logs-dir", action="store_true", help="nao inclui automaticamente a pasta logs/ local")
    parser.add_argument("--include-raw", action="store_true", help="inclui linhas de texto puro que nao estavam em json")
    parser.add_argument("--out", help="grava os registros ordenados neste arquivo .jsonl")
    parser.add_argument("--table", action="store_true", help="exibe tabela compacta em vez de jsonl na saida padrao")
    parser.add_argument("--timeline", action="store_true", help="imprime o resumo de marcos, deteccao, recuperacao e operacoes da cli")
    parser.add_argument("--timeline-json", help="salva o resumo analitico da linha do tempo em um arquivo .json")
    add_filter_arguments(parser)
    return parser


def main(argv: Sequence[str] | None = None, *, runner: ComposeRunner = default_compose_runner) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        flt = build_filter_from_args(args)
    except ValueError as exc:
        print(f"filtro invalido: {exc}", file=sys.stderr)
        return 2

    batches = []
    if not args.no_docker:
        try:
            batches.append(
                collect_from_compose(
                    services=args.services,
                    since=args.since,
                    compose_file=args.compose_file,
                    runner=runner,
                    include_raw=args.include_raw,
                )
            )
        except LogCollectorError as exc:
            print(f"erro ao coletar logs do docker: {exc}", file=sys.stderr)
            return 1

    paths: list[str | Path] = []
    if not args.no_default_logs_dir:
        default_dir = Path("logs")
        if default_dir.exists():
            # evita reler o proprio arquivo de saida caso ele fique dentro de logs/
            out_resolved = Path(args.out).resolve() if args.out else None
            for candidate in sorted(default_dir.glob("*.jsonl")):
                if out_resolved and candidate.resolve() == out_resolved:
                    continue
                paths.append(candidate)
    paths.extend(args.inputs)
    if paths:
        batches.append(collect_from_paths(paths, include_raw=args.include_raw))

    records = filter_records(merge_and_sort(batches), flt)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="\n") as fh:
            for item in records:
                fh.write(json.dumps(item, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")

    summary = analyze_timeline(records) if (args.timeline or args.timeline_json) else None
    if args.timeline_json and summary is not None:
        tj_path = Path(args.timeline_json)
        tj_path.parent.mkdir(parents=True, exist_ok=True)
        tj_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    # quando --out e usado sem --table nem --timeline, mostra apenas uma confirmacao curta no stderr
    if args.timeline and summary is not None:
        print(render_timeline(summary))
    elif args.table:
        for item in records:
            print(format_table_line(item))
    elif not args.out:
        for item in records:
            print(json.dumps(item, ensure_ascii=False, separators=(",", ":"), default=str))
    else:
        print(f"{len(records)} registros salvos em {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
