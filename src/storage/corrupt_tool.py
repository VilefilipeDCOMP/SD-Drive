from __future__ import annotations

import argparse
import json
import os
import random
import sys
from pathlib import Path

from . import jsonlog


def _blocks(block_dir: Path) -> list[Path]:
    return sorted(p for p in block_dir.glob("*/*.blk") if not p.parent.name.startswith("."))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m storage.corrupt_tool", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default=os.environ.get("DCOMP_STORAGE_DATA_DIR", "/data"))
    ap.add_argument("--block-dir", default=os.environ.get("DCOMP_STORAGE_BLOCK_DIR"))
    ap.add_argument("--block-id")
    ap.add_argument("--random", action="store_true", help="escolhe um bloco aleatório")
    ap.add_argument("--mode", choices=("flip", "truncate", "append", "delete"), default="flip")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--events-file", help="também grava o evento fault.injected neste arquivo JSONL")
    a = ap.parse_args(argv)

    data_dir = Path(a.data_dir)
    block_dir = Path(a.block_dir) if a.block_dir else data_dir / "blocks"
    blocks = _blocks(block_dir)
    if a.list:
        for p in blocks:
            print(f"{p.stem}\t{p.stat().st_size}")
        return 0
    if not blocks:
        print(f"nenhum bloco em {block_dir}", file=sys.stderr)
        return 1
    if a.block_id:
        target = next((p for p in blocks if p.stem == a.block_id), None)
        if target is None:
            print(f"bloco {a.block_id} não encontrado em {block_dir}", file=sys.stderr)
            return 1
    elif a.random:
        target = random.choice(blocks)
    else:
        print("informe --block-id ou --random (ou --list)", file=sys.stderr)
        return 2

    size = target.stat().st_size
    detail = ""
    if a.mode == "flip":
        if size == 0:
            print("bloco vazio: use --mode append ou delete", file=sys.stderr)
            return 1
        offset = random.randrange(size)
        with open(target, "r+b") as f:
            f.seek(offset)
            byte = f.read(1)
            f.seek(offset)
            f.write(bytes([byte[0] ^ 0xFF]))
            f.flush()
            os.fsync(f.fileno())
        detail = f"byte {offset} invertido"
    elif a.mode == "truncate":
        with open(target, "r+b") as f:
            f.truncate(size // 2)
        detail = f"truncado de {size} para {size // 2} B"
    elif a.mode == "append":
        with open(target, "ab") as f:
            f.write(b"\x00CORRUPTED\x00")
        detail = "bytes extras acrescentados"
    else:
        target.unlink()
        detail = "arquivo .blk removido"

    node_id = None
    identity = data_dir / "identity.json"
    if identity.exists():
        try:
            node_id = json.loads(identity.read_text(encoding="utf-8")).get("node_id")
        except (OSError, ValueError):
            pass
    jsonlog.COMPONENT = "fault-injector"
    jsonlog.set_node_id(node_id)
    jsonlog.emit("fault.injected", fault_type="corrupt", block_id=target.stem, mode=a.mode, detail=detail)
    if a.events_file:
        rec = {"ts": jsonlog.now_iso(), "component": "fault-injector", "event": "fault.injected", "level": "info",
               "node_id": node_id, "fault_type": "corrupt", "block_id": target.stem, "mode": a.mode}
        with open(a.events_file, "a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps({k: v for k, v in rec.items() if v is not None}) + "\n")
    print(f"bloco {target.stem}: {detail}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
