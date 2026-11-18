#!/usr/bin/env python3
"""gera os stubs python do grpc a partir do arquivo proto.

uso:
    python scripts/gerar_proto.py

requer requirements-dev.txt instalado.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def main() -> int:
    try:
        import grpc_tools  # noqa: F401
    except ImportError:
        print("instale as dependências: python -m pip install -r requirements-dev.txt", file=sys.stderr)
        return 1

    root = Path(__file__).resolve().parents[1]
    out = root / "src"
    out.mkdir(exist_ok=True)

    includes = Path(grpc_tools.__file__).resolve().parent / "_proto"
    proto = root / "proto" / "dcomp_drive.proto"

    if not proto.exists():
        print(f"arquivo proto não encontrado: {proto}", file=sys.stderr)
        return 1

    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{root / 'proto'}",
        f"-I{includes}",
        f"--python_out={out}",
        f"--grpc_python_out={out}",
        f"--pyi_out={out}",
        str(proto),
    ]
    completed = subprocess.run(cmd, check=False)
    if completed.returncode == 0:
        print(f"stubs gerados em {out}/")
    else:
        print("erro ao gerar stubs — veja saída acima", file=sys.stderr)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
