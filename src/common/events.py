"""catalogo fechado de eventos de log do dcomp drive (rf12).

a ideia e todo mundo usar os mesmos nomes, porque na parte 2 os tempos de deteccao e
recuperacao saem da subtracao de timestamps entre eventos (ex: fault.injected ->
node.state_changed para DEAD -> recovery.cluster_healthy). nome inventado na hora quebra
esses scripts sem ninguem perceber.

regras:
- nomes no formato dominio.acao, minusculos, com ponto separando.
- cada evento tem dono (P1..P5 ou "todos"); o dono e quem instrumenta o proprio modulo.
- campos listados sao os esperados, nao uma lista fechada; pode mandar mais se ajudar.
- para criar evento novo, adicione aqui no mesmo commit que passa a usar o nome.

para gerar a tabela em markdown usada em docs/OBSERVABILIDADE.md:

    python -m common.events
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

EVENT_NAME_PATTERN = r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$"


@dataclass(frozen=True)
class EventSpec:
    name: str
    owners: tuple[str, ...]
    description: str
    fields: tuple[str, ...] = ()


def _e(name: str, owners: str, description: str, fields: str = "") -> EventSpec:
    return EventSpec(
        name=name,
        owners=tuple(o.strip() for o in owners.split(",")),
        description=description,
        fields=tuple(f.strip() for f in fields.split(",") if f.strip()),
    )


_SPECS: tuple[EventSpec, ...] = (
    # eventos comuns a todos os processos
    _e("service.started", "todos", "processo iniciou e passou a aceitar chamadas", "role, grpc_bind, advertise_addr"),
    _e("service.stopping", "todos", "processo recebeu sinal de parada e esta encerrando", "reason"),
    _e("config.loaded", "todos", "configuracao efetiva apos juntar yaml, ambiente e linha de comando", "source, config"),
    _e("config.invalid", "todos", "configuracao rejeitada na inicializacao; o processo encerra", "problems"),
    _e("log.record", "P5", "mensagem de biblioteca externa repassada pelo logging padrao do python", "logger"),
    _e("rpc.completed", "P5", "chamada grpc terminada, emitida pelo interceptor comum", "grpc_method, grpc_code, duration_ms"),
    _e("metrics.server_started", "P5", "endpoint http de metricas no ar", "port"),
    # pessoa 1: metadados e placement
    _e("upload.allocated", "P1", "versao PENDING criada e destinos escolhidos", "upload_id, operation_id, file_id, path, version, replication_factor, write_quorum, chunks"),
    _e("upload.lease_renewed", "P1", "lease de upload renovado", "upload_id, lease_expires_at_unix_ms"),
    _e("upload.committed", "P1", "versao publicada como COMMITTED apos quorum por bloco", "upload_id, file_id, path, version, duration_ms"),
    _e("upload.aborted", "P1", "tentativa de upload invalidada (pedido do cliente ou falha)", "upload_id, reason"),
    _e("upload.lease_expired", "P1", "lease venceu sem commit; tentativa descartada", "upload_id, path"),
    _e("upload.rejected", "P1", "pedido de upload recusado", "path, operation_id, grpc_code, reason"),
    _e("placement.insufficient_nodes", "P1", "nao ha N destinos distintos elegiveis", "path, required, eligible, excluded_node_ids"),
    _e("file.deleted", "P1", "arquivo virou tombstone (DELETED)", "path, file_id, version"),
    _e("file.renamed", "P1", "arquivo ou diretorio movido no namespace", "source_path, target_path"),
    _e("dir.created", "P1", "diretorio criado", "path"),
    _e("dir.removed", "P1", "diretorio removido", "path, recursive"),
    _e("replica.registered", "P1", "replica gravada registrada nos metadados", "block_id, node_id, source"),
    _e("replica.invalidated", "P1", "replica marcada como invalida (corrupcao, no morto, drain)", "block_id, node_id, reason"),
    _e("metadata.recovered_on_boot", "P1", "estado de metadados recarregado do disco no boot (rf10)", "files, blocks, duration_ms"),
    # pessoa 2: no de armazenamento
    _e("node.identity_created", "P2", "node_id gerado na primeira execucao e gravado no volume", "node_id, node_id_file"),
    _e("node.registered", "P2, P3", "registro aceito pelo central (logado dos dois lados)", "node_id, boot_id, machine_id, disk_id, grpc_target"),
    _e("node.registration_failed", "P2", "tentativa de registro falhou; o no tenta de novo com backoff", "central_target, attempt, error, retry_in_seconds"),
    _e("heartbeat.sent", "P2", "heartbeat enviado (DEBUG)", "node_id, used_bytes, available_bytes"),
    _e("heartbeat.failed", "P2", "heartbeat nao foi aceito", "node_id, grpc_code, error, will_reregister"),
    _e("block.stored", "P2", "bloco gravado, hash conferido e renomeado atomicamente", "block_id, upload_id, bytes, duration_ms"),
    _e("block.store_rejected", "P2", "gravacao recusada (hash, tamanho, espaco, frames)", "block_id, upload_id, grpc_code, reason"),
    _e("block.served", "P2", "bloco entregue via GetBlock", "block_id, bytes, duration_ms"),
    _e("block.deleted", "P2", "bloco removido fisicamente por ordem do central", "block_id"),
    _e("block.copy_started", "P2", "copia no->no iniciada para recuperacao ou drain", "block_id, target_node_id"),
    _e("block.copy_completed", "P2", "copia no->no concluida com recibo do destino", "block_id, target_node_id, bytes, duration_ms"),
    _e("block.copy_failed", "P2", "copia no->no falhou", "block_id, target_node_id, error"),
    _e("block.corruption_detected", "P2", "hash nao confere (source = write, read ou scrub)", "block_id, source, expected_sha256, observed_sha256"),
    _e("scrub.cycle_completed", "P2", "rodada de scrubbing terminou", "blocks_checked, corrupted, duration_ms"),
    # pessoa 3: controle, falhas e recuperacao
    _e("heartbeat.received", "P3", "heartbeat aceito pelo central (DEBUG)", "node_id, boot_id"),
    _e("heartbeat.rejected", "P3", "heartbeat recusado (no desconhecido ou boot_id antigo)", "node_id, boot_id, reason"),
    _e("node.reboot_detected", "P3", "no conhecido voltou com boot_id novo", "node_id, old_boot_id, boot_id"),
    _e("node.state_changed", "P3", "transicao ALIVE/SUSPECT/DEAD/DRAINING; base do tempo de deteccao", "node_id, from_state, to_state, silence_seconds"),
    _e("corruption.reported", "P3", "relato de corrupcao recebido de cliente ou no", "block_id, suspected_node_id, reporter_id"),
    _e("recovery.block_queued", "P3", "bloco com deficit entrou na fila de recuperacao", "block_id, live_replicas, desired_replicas"),
    _e("recovery.job_started", "P3", "copia de recuperacao disparada", "job_id, block_id, source_node_id, target_node_id"),
    _e("recovery.job_completed", "P3", "copia de recuperacao registrada", "job_id, block_id, target_node_id, bytes, duration_ms"),
    _e("recovery.job_failed", "P3", "copia de recuperacao falhou", "job_id, block_id, error"),
    _e("recovery.cluster_healthy", "P3", "fila de deficit zerou; base do tempo de recuperacao", "recovered_blocks, duration_ms"),
    _e("reconcile.completed", "P3", "inventario de no que voltou foi conferido", "node_id, reported_blocks, obsolete_blocks"),
    _e("drain.started", "P3", "no entrou em DRAINING", "node_id, job_id, blocks"),
    _e("drain.completed", "P3", "drain terminou; no pode ser retirado", "node_id, job_id, duration_ms"),
    _e("gc.block_deleted", "P3", "ordem de remocao fisica enviada a um no", "block_id, node_id, reason"),
    # pessoa 4: cliente e cli
    _e("client.operation_started", "P4", "comando da cli iniciou", "operation, operation_id, path"),
    _e("client.operation_completed", "P4", "comando terminou, com sucesso ou nao; a cli vive pouco, entao este evento e a metrica de latencia e vazao", "operation, operation_id, path, success, bytes, duration_ms, error_code"),
    _e("client.retry", "P4", "nova tentativa de upload com exclusoes acumuladas", "attempt, excluded_node_ids, error, backoff_seconds"),
    _e("client.replica_fallback", "P4", "leitura trocou de replica", "block_id, failed_node_id, next_node_id, reason"),
    _e("client.corruption_reported", "P4", "cliente reportou corrupcao ao central", "block_id, node_id"),
    # pessoa 5: falhas injetadas e verificacao
    _e("fault.injected", "P5", "falha injetada por script; marca a hora zero dos experimentos", "kind, target"),
    _e("fault.reverted", "P5", "falha injetada foi desfeita", "kind, target"),
    _e("verify.step", "P5", "passo de verificacao executado", "scenario, step, status"),
)

CATALOG: dict[str, EventSpec] = {spec.name: spec for spec in _SPECS}

if len(CATALOG) != len(_SPECS):
    # nome repetido no catalogo e erro de quem editou, melhor quebrar no import
    _seen: set[str] = set()
    _dups = [s.name for s in _SPECS if s.name in _seen or _seen.add(s.name)]
    raise RuntimeError(f"eventos duplicados no catalogo: {_dups}")


def is_known(name: str) -> bool:
    return name in CATALOG


def to_markdown() -> str:
    """tabela do catalogo agrupada por dono, pronta para colar na documentacao."""
    order = ["todos", "P1", "P2", "P3", "P4", "P5"]
    lines = ["| Evento | Dono | Descrição | Campos esperados |", "|---|---|---|---|"]
    ranked = sorted(_SPECS, key=lambda s: min(order.index(o) if o in order else 99 for o in s.owners))
    for spec in ranked:
        fields = ", ".join(f"`{f}`" for f in spec.fields) or "-"
        lines.append(f"| `{spec.name}` | {', '.join(spec.owners)} | {spec.description} | {fields} |")
    return "\n".join(lines)


if __name__ == "__main__":
    sys.stdout.write(to_markdown() + "\n")

