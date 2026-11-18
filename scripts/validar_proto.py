#!/usr/bin/env python3
"""compila o .proto e verifica serviços, rpcs e assinaturas de streaming.

uso:
    python scripts/validar_proto.py

saída esperada:
    PROTO VALIDADO: serviços, streaming e tag 9 OK
    MetadataService : AllocateUpload, CommitUpload, ...
    StorageService  : StoreBlock, GetBlock, ...
    ControlService  : RegisterNode, Heartbeat, ...
"""
from __future__ import annotations

import sys
from pathlib import Path

# garante que gerar_proto.py seja encontrado mesmo rodando da raiz
_scripts_dir = Path(__file__).resolve().parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from gerar_proto import main as gerar  # noqa: E402


def main() -> int:
    result = gerar()
    if result:
        return result

    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "src"))

    import dcomp_drive_pb2 as pb  # type: ignore
    import dcomp_drive_pb2_grpc as stubs  # noqa: F401  # type: ignore

    services = pb.DESCRIPTOR.services_by_name

    # verifica quantidade de rpcs por servico
    expected = {"MetadataService": 11, "StorageService": 4, "ControlService": 6}
    actual = {name: len(s.methods) for name, s in services.items()}
    missing = set(expected) - set(actual)
    if missing:
        print(f"serviços ausentes no proto: {missing}", file=sys.stderr)
        return 1
    divergent = {k for k in expected if actual.get(k) != expected[k]}
    if divergent:
        print(f"RPCs divergentes: {actual} != {expected}", file=sys.stderr)
        return 1

    # verifica campo tag 9
    excluded = pb.AllocateUploadRequest.DESCRIPTOR.fields_by_name.get("excluded_node_ids")
    if excluded is None or excluded.number != 9:
        print("campo excluded_node_ids (tag 9) ausente ou com número errado", file=sys.stderr)
        return 1

    # verifica streaming
    storage = services["StorageService"].methods_by_name
    assert storage["StoreBlock"].client_streaming and not storage["StoreBlock"].server_streaming, \
        "StoreBlock deve ser client-streaming"
    assert storage["GetBlock"].server_streaming and not storage["GetBlock"].client_streaming, \
        "GetBlock deve ser server-streaming"

    print("PROTO VALIDADO: serviços, streaming e tag 9 OK")
    for name, service in services.items():
        print(f"  {name}: {', '.join(m.name for m in service.methods)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
