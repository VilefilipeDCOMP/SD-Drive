"""central falso so com GetStatus, para desenvolver e demonstrar o comando status sem o controlador real.

    python -m observability.fake_control --port 50051
    python -m observability.status_cli --target localhost:50051 --watch 1

o cenario muda sozinho: a cada ciclo um no cai, as copias de recuperacao aparecem e depois tudo
volta ao normal. serve para testar o --watch e as metricas; nao implementa nenhum outro rpc.
"""

from __future__ import annotations

import argparse
import threading
import time
from concurrent import futures

import grpc

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc

GIB = 1024**3


def build_response(step: int = 0, nodes: int = 5, files: int = 20) -> pb.GetStatusResponse:
    """foto do cluster no passo `step`: 0 saudavel, 1 no caiu, 2 recuperando, 3 quase la."""
    response = pb.GetStatusResponse()
    for i in range(1, nodes + 1):
        state = pb.NODE_ALIVE
        if i == 3 and step in (1, 2, 3):
            state = pb.NODE_SUSPECT if step == 1 else pb.NODE_DEAD
        response.nodes.add(
            node=pb.NodeAddress(node_id=f"00000000-0000-0000-0000-{i:012d}", machine_id=f"machine-{i}", disk_id=f"disk-{i}", grpc_target=f"storage-{i}:50052"),
            state=state,
            total_bytes=2 * GIB,
            used_bytes=int(0.2 * GIB) * i,
            available_bytes=2 * GIB - int(0.2 * GIB) * i,
        )
    under = {0: 0, 1: 0, 2: 6, 3: 2}[step]
    recovering = {0: 0, 1: 0, 2: 4, 3: 1}[step]
    response.healthy_files = files - under - recovering
    response.under_replicated_files = under
    response.recovering_files = recovering
    response.running_recovery_jobs = 2 if step == 2 else (1 if step == 3 else 0)
    return response


class FakeControlServicer(rpc.ControlServiceServicer):
    def __init__(self, cycle_seconds: float = 0.0):
        self._cycle = cycle_seconds
        self._start = time.monotonic()

    def GetStatus(self, request, context):
        step = 0
        if self._cycle > 0:
            step = int(((time.monotonic() - self._start) / self._cycle) % 4)
        return build_response(step)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="central falso com GetStatus")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--cycle", type=float, default=5.0, help="segundos por etapa do cenario (0 = sempre saudavel)")
    args = parser.parse_args(argv)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    rpc.add_ControlServiceServicer_to_server(FakeControlServicer(args.cycle), server)
    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()
    print(f"central falso em 0.0.0.0:{args.port} (ctrl+c para sair)", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.stop(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

