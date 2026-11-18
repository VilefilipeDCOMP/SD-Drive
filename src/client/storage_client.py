from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Generator, Optional, Tuple

_SRC_DIR = str(Path(__file__).resolve().parent.parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import grpc

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc

logger = logging.getLogger(__name__)


class StorageStreamError(Exception):
    """Erro durante streaming com o nó de armazenamento."""
    pass


class StorageClient:
    """Cliente gRPC de baixo nível para interação com o StorageService (Pessoa 2)."""

    def __init__(self, default_timeout: float = 45.0):
        self.default_timeout = default_timeout
        self._channels: dict[str, grpc.Channel] = {}

    def get_channel(self, target: str) -> grpc.Channel:
        """Obtém ou cria canal gRPC para o endereço informado."""
        if target not in self._channels:
            options = [
                ("grpc.max_receive_message_length", 32 * 1024 * 1024),
                ("grpc.max_send_message_length", 32 * 1024 * 1024),
            ]
            self._channels[target] = grpc.insecure_channel(target, options=options)
        return self._channels[target]

    def close(self) -> None:
        """Fecha todos os canais gRPC abertos."""
        for ch in self._channels.values():
            try:
                ch.close()
            except Exception:
                pass
        self._channels.clear()

    def store_block(
        self,
        grpc_target: str,
        header: pb.StoreBlockHeader,
        data: bytes,
        frame_size_bytes: int = 256 * 1024,
        timeout: Optional[float] = None,
        channel: Optional[grpc.Channel] = None,
        stub: Optional[rpc.StorageServiceStub] = None,
    ) -> pb.BlockReceipt:
        """
        Transmite um bloco lógico para o nó via streaming gRPC de StoreBlockFrame.
        O primeiro frame é obrigatoriamente o header.
        Os frames seguintes contêm fatias de dados de até frame_size_bytes (256 KiB).
        Retorna o BlockReceipt emitido pelo nó após validação atômica.
        """
        active_stub = stub or rpc.StorageServiceStub(channel or self.get_channel(grpc_target))
        rpc_timeout = timeout if timeout is not None else self.default_timeout

        def generate_frames() -> Generator[pb.StoreBlockFrame, None, None]:
            # 1. Envia o header obrigatório primeiro
            yield pb.StoreBlockFrame(header=header)

            # 2. Envia os dados em pedaços de 256 KiB
            if not data:
                yield pb.StoreBlockFrame(data=b"")
            else:
                for offset in range(0, len(data), frame_size_bytes):
                    yield pb.StoreBlockFrame(data=data[offset : offset + frame_size_bytes])

        return active_stub.StoreBlock(generate_frames(), timeout=rpc_timeout)

    def get_block(
        self,
        grpc_target: str,
        block_id: str,
        timeout: Optional[float] = None,
        channel: Optional[grpc.Channel] = None,
        stub: Optional[rpc.StorageServiceStub] = None,
    ) -> Tuple[pb.GetBlockHeader, bytes]:
        """
        Baixa um bloco do nó via streaming de GetBlockFrame.
        O primeiro frame contém o GetBlockHeader com tamanho e hash esperados.
        Os frames subsequentes contêm pedaços dos dados armazenados.
        Retorna (header, bytes_completos).
        """
        active_stub = stub or rpc.StorageServiceStub(channel or self.get_channel(grpc_target))
        rpc_timeout = timeout if timeout is not None else self.default_timeout

        request = pb.GetBlockRequest(block_id=block_id)
        stream = active_stub.GetBlock(request, timeout=rpc_timeout)

        header: Optional[pb.GetBlockHeader] = None
        data_buffer = bytearray()

        for frame in stream:
            payload_type = frame.WhichOneof("payload")
            if payload_type == "header":
                header = frame.header
            elif payload_type == "data":
                data_buffer.extend(frame.data)
            else:
                pass

        if header is None:
            raise StorageStreamError(
                f"Nenhum cabeçalho recebido no stream GetBlock para block_id {block_id} de {grpc_target}."
            )

        return header, bytes(data_buffer)

    def delete_block(
        self,
        grpc_target: str,
        block_id: str,
        timeout: Optional[float] = None,
        channel: Optional[grpc.Channel] = None,
        stub: Optional[rpc.StorageServiceStub] = None,
    ) -> None:
        """Solicita a remoção física de um bloco no nó de armazenamento."""
        active_stub = stub or rpc.StorageServiceStub(channel or self.get_channel(grpc_target))
        rpc_timeout = timeout if timeout is not None else self.default_timeout
        active_stub.DeleteBlock(pb.DeleteBlockRequest(block_id=block_id), timeout=rpc_timeout)
