"""consulta rapida e linha do tempo sobre arquivos .jsonl ja coletados (ou stdin).

exemplos:
    python scripts/logq.py logs/all.jsonl --event "recovery.*" --table
    python scripts/logq.py logs/all.jsonl --node-id 8f12 --table
    python scripts/logq.py logs/all.jsonl --timeline
    python scripts/logq.py logs/faults.jsonl --table
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
    add_filter_arguments,
    analyze_timeline,
    build_filter_from_args,
    collect_from_paths,
    filter_records,
    format_table_line,
    merge_and_sort,
    parse_log_line,
    render_timeline,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="logq", description="filtra e resume arquivos .jsonl de logs do dcomp drive")
    parser.add_argument("files", nargs="*", help="arquivos .jsonl ou pastas (vazio = le da entrada padrao)")
    parser.add_argument("--table", action="store_true", help="mostra em formato tabular legivel")
    parser.add_argument("--timeline", action="store_true", help="calcula linha do tempo, tempos de deteccao/recuperacao e metricas da cli")
    parser.add_argument("--timeline-json", action="store_true", help="imprime o resumo da linha do tempo em json")
    parser.add_argument("--include-raw", action="store_true", help="inclui linhas que nao sejam json")
    add_filter_arguments(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        flt = build_filter_from_args(args)
    except ValueError as exc:
        print(f"filtro invalido: {exc}", file=sys.stderr)
        return 2

    if args.files:
        missing = [f for f in args.files if not Path(f).exists()]
        if missing:
            print(f"caminho nao encontrado: {', '.join(missing)}", file=sys.stderr)
            return 1
        raw_records = collect_from_paths(args.files, include_raw=args.include_raw)
    else:
        raw_records = []
        for line in sys.stdin:
            parsed = parse_log_line(line, include_raw=args.include_raw)
            if parsed is not None:
                raw_records.append(parsed)

    records = filter_records(merge_and_sort([raw_records]), flt)

    if args.timeline or args.timeline_json:
        summary = analyze_timeline(records)
        if args.timeline_json:
            print(json.dumps(summary, indent=2, ensure_ascii=False))
        else:
            print(render_timeline(summary))
        return 0

    for item in records:
        if args.table:
            print(format_table_line(item))
        else:
            print(json.dumps(item, ensure_ascii=False, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
