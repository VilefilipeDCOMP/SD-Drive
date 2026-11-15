#!/usr/bin/env python3
"""Compile .proto and verify expected services and streaming signatures."""
from __future__ import annotations

import sys
from pathlib import Path
from gerar_proto import main as gerar


def main() -> int:
    result = gerar()
    if result:
        return result
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "src"))
    import dcomp_drive_pb2 as pb
    import dcomp_drive_pb2_grpc as stubs  # noqa: F401

    services = pb.DESCRIPTOR.services_by_name
    expected = {"MetadataService": 11, "StorageService": 4, "ControlService": 6}
    actual = {name: len(s.methods) for name, s in services.items()}
    assert actual == expected, f"RPCs divergentes: {actual} != {expected}"
    excluded = pb.AllocateUploadRequest.DESCRIPTOR.fields_by_name["excluded_node_ids"]
    assert excluded.number == 9, "Campo excluded_node_ids deve ser tag 9"
    storage = services["StorageService"].methods_by_name
    assert storage["StoreBlock"].client_streaming and not storage["StoreBlock"].server_streaming
    assert storage["GetBlock"].server_streaming and not storage["GetBlock"].client_streaming
    print("PROTO VALIDADO: serviços, streaming e tag 9 OK")
    for name, service in services.items():
        print(name, ':', ', '.join(m.name for m in service.methods))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
