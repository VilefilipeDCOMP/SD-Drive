#!/usr/bin/env python3
"""Record a fault-injection marker; this script does NOT inject a fault."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Registra marcador JSONL de falha (não executa a falha).")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--fault-type", required=True, choices=("stop", "restart", "corrupt", "disconnect", "other"))
    parser.add_argument("--log", type=Path, default=Path("logs/faults.jsonl"))
    parser.add_argument("--note", default="")
    args = parser.parse_args()
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "component": "fault-injector",
        "event": "fault.injected",
        "level": "info",
        "node_id": args.node_id,
        "fault_type": args.fault_type,
    }
    if args.note:
        record["note"] = args.note
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with args.log.open("a", encoding="utf-8", newline="\n") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"Marcador gravado em {args.log}: {record['ts']} (não injeta a falha)")


if __name__ == "__main__":
    main()
