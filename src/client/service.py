from __future__ import annotations

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set

import grpc

import dcomp_drive_pb2 as pb
from client.chunker import PreparedChunk, PreparedUpload, prepare_file_upload
from client.config import ClientConfig, load_client_config
from client.controller_client import ControllerClient, format_rpc_error
from client.crypto import derive_or_validate_key
from client.storage_client import StorageClient

logger = logging.getLogger(__name__)


class ClientServiceError(Exception):
    #Exceção base do serviço do cliente
    pass


class UploadError(ClientServiceError):
    #Erro durante o processo de upload
    pass


class QuorumError(UploadError):
    #Quórum de escrita não alcançado
    pass


class InsufficientNodesError(UploadError):
    #Destinos elegíveis insuficientes para atender o fator de replicação N
    pass


class _LeaseRenewer(threading.Thread):
    #Worker em segundo plano para renovação periódica do lease de upload

    def __init__(
        self,
        controller: ControllerClient,
        upload_id: str,
        interval_seconds: float,
    ):
        super().__init__(daemon=True)
        self.controller = controller
        self.upload_id = upload_id
        self.interval = interval_seconds
        self.stop_event = threading.Event()
        self.last_renewal_ms: Optional[int] = None
        self.error: Optional[Exception] = None

    def run(self) -> None:
        while not self.stop_event.wait(self.interval):
            try:
                resp = self.controller.renew_lease(self.upload_id)
                self.last_renewal_ms = resp.lease_expires_at_unix_ms
                logger.debug(
                    f"Lease do upload {self.upload_id} renovado com sucesso. "
                    f"Nova expiração: {self.last_renewal_ms}"
                )
            except Exception as e:
                self.error = e
                logger.warning(
                    f"Falha ao renovar lease do upload {self.upload_id}: {e}"
                )
                break

    def stop(self) -> None:
        self.stop_event.set()


class ClientService:
    """
    Implementa as regras de negócio distribuídas: chunking, quórum W,
    renovação de lease, retries com exclusão de nós e download com fallback.
    """

    def __init__(
        self,
        config: Optional[ClientConfig] = None,
        controller: Optional[ControllerClient] = None,
        storage: Optional[StorageClient] = None,
    ):
        self.config = config or load_client_config()
        self.controller = controller or ControllerClient(
            self.config.central_target,
            self.config.upload_rpc_deadline_seconds,
        )
        self.storage = storage or StorageClient(
            self.config.upload_rpc_deadline_seconds
        )

    def close(self) -> None:
        if self.storage:
            self.storage.close()
        if self.controller:
            self.controller.close()

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        overwrite: bool = False,
        expected_version: Optional[int] = None,
        key: Optional[bytes | str] = None,
    ) -> pb.FileMetadata:
        """
        Executa o fluxo completo de upload com particionamento em blocos de 4 MiB,
        transmissão paralela gRPC aos nós, verificação de quórum W, renovação de lease
        e política resiliente de retries com exclusão de nós falhos.
        """
        local = Path(local_path).resolve()
        if not local.is_file():
            raise FileNotFoundError(f"Arquivo local não encontrado: {local}")

        # Validação ou obtenção de chave de criptografia se configurada
        validated_key: Optional[bytes] = None
        if key is not None:
            validated_key = derive_or_validate_key(key)
        elif self.config.encryption_enabled:
            validated_key = derive_or_validate_key()

        #Fatiamento e preparação
        prepared = prepare_file_upload(
            local,
            remote_path,
            self.config.logical_chunk_size_bytes,
            validated_key,
        )

        excluded_node_ids: Set[str] = set()
        max_attempts = max(1, self.config.upload_retry_max_attempts)
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            operation_id = str(prepared.chunks[0].block_id if prepared.chunks else "empty-op")
            if attempt > 1:
                # Gera novos block_ids e novos nonces para evitar colisão com tentativa cancelada
                prepared.regenerate_attempt()
                operation_id = str(prepared.chunks[0].block_id if prepared.chunks else "empty-op")

                backoff = min(
                    self.config.upload_retry_backoff_initial_seconds * (2 ** (attempt - 2)),
                    self.config.upload_retry_backoff_max_seconds,
                )
                jitter_range = backoff * self.config.upload_retry_backoff_jitter_fraction
                sleep_time = max(0.0, backoff + random.uniform(-jitter_range, jitter_range))
                logger.info(
                    f"Tentativa de upload {attempt}/{max_attempts} para {remote_path} "
                    f"aguardando {sleep_time:.2f}s (nós excluídos: {list(excluded_node_ids)})"
                )
                time.sleep(sleep_time)

            # Alocação de Upload no Controlador
            try:
                allocation = self.controller.allocate_upload(
                    remote_path,
                    operation_id,
                    prepared.plaintext_size_bytes,
                    prepared.plaintext_sha256,
                    prepared.encryption_mode,
                    prepared.manifests,
                    overwrite,
                    expected_version,
                    sorted(list(excluded_node_ids)),
                )
            except grpc.RpcError as rpc_err:
                code = rpc_err.code() if hasattr(rpc_err, "code") else None
                if code == grpc.StatusCode.FAILED_PRECONDITION:
                    raise InsufficientNodesError(
                        f"Não há nós suficientes elegíveis para atender o fator de replicação "
                        f"(exclusões acumuladas: {list(excluded_node_ids)}). {format_rpc_error(rpc_err)}"
                    ) from rpc_err
                raise UploadError(
                    f"Falha na alocação de upload: {format_rpc_error(rpc_err)}"
                ) from rpc_err

            upload_id = allocation.upload_id
            write_quorum = allocation.write_quorum

            # Arquivo vazio
            if prepared.plaintext_size_bytes == 0 or len(prepared.chunks) == 0:
                try:
                    committed_meta = self.controller.commit_upload(
                        upload_id, []
                    )
                    return committed_meta
                except Exception as e:
                    self.controller.abort_upload(upload_id)
                    raise UploadError(f"Falha ao realizar commit de arquivo vazio: {e}") from e

            # Inicia o renovador assíncrono de lease
            renewer = _LeaseRenewer(
                self.controller,
                upload_id,
                self.config.lease_renew_interval_seconds,
            )
            renewer.start()

            #Envio em streaming gRPC diretamente aos nós de armazenamento em paralelo
            chunk_receipts: Dict[str, List[pb.BlockReceipt]] = {
                c.block_id: [] for c in prepared.chunks
            }
            nodes_with_failures_this_attempt: Set[str] = set()
            placement_by_block_id = {p.block_id: p for p in allocation.chunks}

            upload_tasks = []
            with ThreadPoolExecutor(max_workers=min(32, max(4, len(prepared.chunks) * 3))) as executor:
                for chunk in prepared.chunks:
                    placement = placement_by_block_id.get(chunk.block_id)
                    if not placement:
                        continue

                    for target_node in placement.targets:
                        header = pb.StoreBlockHeader(
                            chunk.block_id,
                            upload_id,
                            allocation.version,
                            chunk.stored_size_bytes,
                            chunk.storage_sha256,
                        )
                        task = executor.submit(
                            self._send_single_block,
                            target_node,
                            header,
                            chunk.stored_bytes,
                            self.config.grpc_frame_size_bytes,
                        )
                        upload_tasks.append((task, chunk.block_id, target_node.node_id))

                for future, block_id, node_id in upload_tasks:
                    try:
                        receipt = future.result()
                        # Valida que o recibo é idêntico ao solicitado
                        if receipt.block_id == block_id:
                            chunk_receipts[block_id].append(receipt)
                        else:
                            nodes_with_failures_this_attempt.add(node_id)
                    except Exception as exc:
                        logger.warning(
                            f"Falha no envio do bloco {block_id} para o nó {node_id}: {exc}"
                        )
                        nodes_with_failures_this_attempt.add(node_id)

            # Para o renovador de lease
            renewer.stop()

            # 6. Verificação do quórum W de recibos por bloco
            quorum_satisfied = True
            for chunk in prepared.chunks:
                # Contabiliza recibos únicos de nós distintos
                distinct_receipts = {
                    r.node_id: r for r in chunk_receipts[chunk.block_id]
                }
                if len(distinct_receipts) < write_quorum:
                    quorum_satisfied = False
                    logger.warning(
                        f"Bloco {chunk.block_id} obteve {len(distinct_receipts)} recibos, "
                        f"mas o quórum exigido é {write_quorum}."
                    )
                    break

            if quorum_satisfied:
                # Coleta todos os recibos e publica atomicamente
                all_receipts: List[pb.BlockReceipt] = []
                for chunk in prepared.chunks:
                    distinct_map = {
                        r.node_id: r for r in chunk_receipts[chunk.block_id]
                    }
                    all_receipts.extend(distinct_map.values())

                try:
                    committed_file = self.controller.commit_upload(
                        upload_id, all_receipts
                    )
                    logger.info(
                        f"Upload concluído com sucesso para {remote_path} "
                        f"(versão {committed_file.version}, {len(all_receipts)} réplicas confirmadas)."
                    )
                    return committed_file
                except Exception as commit_exc:
                    logger.error(f"Erro no CommitUpload: {commit_exc}")
                    # Tenta abortar
                    try:
                        self.controller.abort_upload(upload_id)
                    except Exception:
                        pass
                    raise UploadError(
                        f"Falha ao confirmar publicação do arquivo: {commit_exc}"
                    ) from commit_exc

            #Falha de quórum: Aborta a tentativa e registra os nós que falharam
            logger.warning(
                f"Tentativa {attempt} de upload falhou em atingir o quórum W={write_quorum}. "
                f"Abortando upload_id {upload_id}."
            )
            try:
                self.controller.abort_upload(upload_id)
            except Exception as abort_exc:
                logger.debug(f"Aviso ao abortar upload: {abort_exc}")

            # Acumula os nós problemáticos para excluir da próxima tentativa
            excluded_node_ids.update(nodes_with_failures_this_attempt)
            last_error = QuorumError(
                f"Quórum de escrita W={write_quorum} não atingido na tentativa {attempt}. "
                f"Nós com falhas: {list(nodes_with_failures_this_attempt)}"
            )

        raise UploadError(
            f"Upload falhou após {max_attempts} tentativas. Último erro: {last_error}"
        )
        
    #Envia um bloco a um nó específico
    def _send_single_block(
        self,
        target_node: pb.NodeAddress,
        header: pb.StoreBlockHeader,
        data: bytes,
        chunk_size: int,
    ) -> pb.BlockReceipt:
        return self.storage.store_block(
            target_node.grpc_target,
            header,
            data,
            chunk_size,
        )

