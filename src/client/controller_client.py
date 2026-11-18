from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import List, Optional

_SRC_DIR = str(Path(__file__).resolve().parent.parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import grpc
from google.protobuf import empty_pb2

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc

logger = logging.getLogger(__name__)


def format_rpc_error(exc: grpc.RpcError) -> str:
    #Traduz códigos de erro gRPC para mensagens limpas
    if not isinstance(exc, grpc.RpcError):
        return str(exc)

    code = exc.code() if hasattr(exc, "code") else grpc.StatusCode.UNKNOWN
    details = exc.details() if hasattr(exc, "details") else str(exc)

    detail_str = f" ({details})" if details else ""

    if code == grpc.StatusCode.NOT_FOUND:
        return f"Recurso não encontrado{detail_str}."
    if code == grpc.StatusCode.ALREADY_EXISTS:
        return f"Arquivo ou diretório já existe{detail_str}."
    if code == grpc.StatusCode.FAILED_PRECONDITION:
        return f"Condição prévia não atendida: {details or 'quórum insuficiente ou destinos insuficientes'}."
    if code == grpc.StatusCode.ABORTED:
        return f"Operação abortada{detail_str} (ex: lease expirado ou conflito de versão)."
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return f"Argumento inválido{detail_str}."
    if code == grpc.StatusCode.UNAVAILABLE:
        return f"Serviço ou nó temporariamente inacessível{detail_str}. Verifique se o controlador e os nós estão rodando."
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return f"Tempo limite da operação excedido (timeout){detail_str}."
    if code == grpc.StatusCode.DATA_LOSS:
        return f"Perda ou corrupção irrecuperável de dados detectada{detail_str}."
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        return f"Capacidade de armazenamento esgotada ou limite de cota atingido{detail_str}."
    if code == grpc.StatusCode.PERMISSION_DENIED:
        return f"Permissão negada{detail_str}."

    return f"Erro gRPC [{code.name}]: {details}"


class ControllerClient:
    #Cliente gRPC que interage com o processo central

    def __init__(
        self,
        target: str = "controller:50051",
        default_timeout: float = 45.0,
        channel: Optional[grpc.Channel] = None,
        metadata_stub: Optional[rpc.MetadataServiceStub] = None,
        control_stub: Optional[rpc.ControlServiceStub] = None,
    ):
        self.target = target
        self.default_timeout = default_timeout
        self._external_channel = channel is not None
        self.channel = None

        if metadata_stub and control_stub:
            self.metadata_stub = metadata_stub
            self.control_stub = control_stub
        else:
            if channel:
                self.channel = channel
            else:
                options = [
                    ("grpc.max_receive_message_length", 32 * 1024 * 1024),
                    ("grpc.max_send_message_length", 32 * 1024 * 1024),
                ]
                self.channel = grpc.insecure_channel(target, options=options)

            self.metadata_stub = metadata_stub or rpc.MetadataServiceStub(self.channel)
            self.control_stub = control_stub or rpc.ControlServiceStub(self.channel)

    def close(self) -> None:
        if not self._external_channel and self.channel:
            try:
                self.channel.close()
            except Exception:
                pass

    #Solicita a alocação de upload e a reserva de destinos *PENDING*
    def allocate_upload(
        self,
        path: str,
        operation_id: str,
        plaintext_size_bytes: int,
        plaintext_sha256: bytes,
        encryption: int,
        chunks: List[pb.ChunkManifest],
        overwrite: bool = False,
        expected_version: Optional[int] = None,
        excluded_node_ids: Optional[List[str]] = None,
        timeout: Optional[float] = None,
    ) -> pb.AllocateUploadResponse:
        req = pb.AllocateUploadRequest(
            path=path,
            operation_id=operation_id,
            plaintext_size_bytes=plaintext_size_bytes,
            plaintext_sha256=plaintext_sha256,
            encryption=encryption,
            chunks=chunks,
            overwrite=overwrite,
            expected_version=expected_version,
            excluded_node_ids=excluded_node_ids or [],
        )
        return self.metadata_stub.AllocateUpload(
            req, timeout=timeout or self.default_timeout
        )

    #Renova o lease de um upload PENDING antes que expire
    def renew_lease(
        self, upload_id: str, timeout: Optional[float] = None
    ) -> pb.RenewLeaseResponse:
        req = pb.RenewLeaseRequest(upload_id=upload_id)
        return self.metadata_stub.RenewLease(
            req, timeout=timeout or self.default_timeout
        )

    #Publica o arquivo como COMMITTED após satisfazer o quórum W
    def commit_upload(
        self,
        upload_id: str,
        receipts: List[pb.BlockReceipt],
        timeout: Optional[float] = None,
    ) -> pb.FileMetadata:
        req = pb.CommitUploadRequest(upload_id=upload_id, receipts=receipts)
        return self.metadata_stub.CommitUpload(
            req, timeout=timeout or self.default_timeout
        )

    #Invalida a tentativa de upload e libera locks/recursos de lease
    def abort_upload(
        self, upload_id: str, timeout: Optional[float] = None
    ) -> None:
        req = pb.AbortUploadRequest(upload_id=upload_id)
        self.metadata_stub.AbortUpload(req, timeout=timeout or self.default_timeout)

    #Obtém a lista de chunks e nós réplicas para leitura de versão
    def resolve_download(
        self, path: str, timeout: Optional[float] = None
    ) -> pb.ResolveDownloadResponse:
        req = pb.ResolveDownloadRequest(path=path)
        return self.metadata_stub.ResolveDownload(
            req, timeout=timeout or self.default_timeout
        )

    #Marca o caminho como DELETED logicamente
    def delete(
        self,
        path: str,
        expected_version: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> None:
        req = pb.DeleteRequest(path=path, expected_version=expected_version)
        self.metadata_stub.Delete(req, timeout=timeout or self.default_timeout)

    #Renomeia ou move um arquivo/diretório de modo atômico
    def rename(
        self,
        source_path: str,
        target_path: str,
        overwrite: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        req = pb.RenameRequest(
            source_path=source_path, target_path=target_path, overwrite=overwrite
        )
        self.metadata_stub.Rename(req, timeout=timeout or self.default_timeout)

    #Lista entradas visíveis de arquivos e diretórios
    def list_dir(
        self,
        path: str,
        page_size: int = 100,
        page_token: str = "",
        timeout: Optional[float] = None,
    ) -> pb.ListDirResponse:
        req = pb.ListDirRequest(
            path=path, page_size=page_size, page_token=page_token
        )
        return self.metadata_stub.ListDir(req, timeout=timeout or self.default_timeout)

    #Consulta metadados de arquivo ou diretório
    def stat(self, path: str, timeout: Optional[float] = None) -> pb.StatResponse:
        req = pb.StatRequest(path=path)
        return self.metadata_stub.Stat(req, timeout=timeout or self.default_timeout)

    #Cria um novo diretório no namespace
    def mkdir(self, path: str, timeout: Optional[float] = None) -> None:
        req = pb.MkdirRequest(path=path)
        self.metadata_stub.Mkdir(req, timeout=timeout or self.default_timeout)
    
    #Remove um diretório (opcionalmente recursivo)
    def rmdir(
        self, path: str, recursive: bool = False, timeout: Optional[float] = None
    ) -> None:    
        req = pb.RmdirRequest(path=path, recursive=recursive)
        self.metadata_stub.Rmdir(req, timeout=timeout or self.default_timeout)

    # Métodos do ControlService
    #Solicita o esvaziamento (drain) de um nó de armazenamento
    def drain_node(
        self, node_id: str, timeout: Optional[float] = None
    ) -> pb.DrainNodeResponse:
        req = pb.DrainNodeRequest(node_id=node_id)
        return self.control_stub.DrainNode(
            req, timeout=timeout or self.default_timeout
        )

    #Consulta o status geral do cluster (nós, capacidade e saúde dos arquivos)
    def get_status(
        self, timeout: Optional[float] = None
    ) -> pb.GetStatusResponse:
        return self.control_stub.GetStatus(
            empty_pb2.Empty(), timeout=timeout or self.default_timeout
        )
    
    #Notifica o controlador sobre bloco com corrupção detectada para agendar recuperação
    def report_corruption(
        self,
        block_id: str,
        suspected_node_id: str,
        observed_sha256: bytes,
        reporter_id: str = "client",
        timeout: Optional[float] = None,
    ) -> None:
        req = pb.ReportCorruptionRequest(
            block_id=block_id,
            suspected_node_id=suspected_node_id,
            observed_sha256=observed_sha256,
            reporter_id=reporter_id,
        )
        self.control_stub.ReportCorruption(
            req, timeout=timeout or self.default_timeout
        )