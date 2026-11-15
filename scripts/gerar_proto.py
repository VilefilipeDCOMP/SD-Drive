#!/usr/bin/env python3
"""Generate Python gRPC stubs on Windows/macOS/Linux. Requires requirements-dev.txt."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def main() -> int:
    try:
        import grpc_tools
    except ImportError:
        print("Instale as dependências: python -m pip install -r requirements-dev.txt", file=sys.stderr)
        return 1
    root = Path(__file__).resolve().parents[1]
    out = root / "src"
    out.mkdir(exist_ok=True)
    includes = Path(grpc_tools.__file__).resolve().parent / "_proto"
    proto = root / "proto" / "dcomp_drive.proto"
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{root / 'proto'}", f"-I{includes}",
        f"--python_out={out}", f"--grpc_python_out={out}", str(proto),
    ]
    completed = subprocess.run(cmd, check=False)
    if completed.returncode == 0:
        print(f"Stubs gerados em {out}")
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
