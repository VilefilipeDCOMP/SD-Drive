# Relatório Técnico — SD-Drive Parte 1

**Disciplina:** Sistemas Distribuídos — 6º Período  
**Turma:** DCOMP/UFS  
**Data de Entrega:** 18/11/2026  
**Repositório:** https://github.com/VilefilipeDCOMP/SD-Drive

---

## 1. Introdução e Arquitetura

O SD-Drive é um sistema de armazenamento distribuído tolerante a falhas, implementado em Python com comunicação gRPC. O sistema é composto por um processo central que coordena os nós de armazenamento e uma CLI que permite aos usuários interagir com o cluster.

### Arquitetura geral

```
┌─────────────────────────────────────────────────────┐
│                   Docker Network (dcomp_net)         │
│                                                     │
│  ┌────────────────────────────────────────┐         │
│  │  central (:50051)                      │         │
│  │  ├── MetadataService  (Pessoa 1)        │         │
│  │  └── ControlService   (Pessoa 3)        │         │
│  └─────────────────┬──────────────────────┘         │
│                    │ gRPC                            │
│     ┌──────────────┼──────────────────┐             │
│     ▼              ▼                  ▼             │
│  storage-1      storage-3          storage-5        │
│  (:50052)       (:50052)            (:50052)        │
│  StorageService  (Pessoa 2)                         │
│                                                     │
│  cli  [--profile tools]  (Pessoa 4)                 │
└─────────────────────────────────────────────────────┘
```

### Decisões de projeto

- **Imagem Docker única:** todos os serviços (central, storage, cli) usam a mesma imagem `dcomp-drive:dev`, com o papel definido pelo `command` do Compose. Isso simplifica o build e garante que todos os serviços usem a mesma versão do código.
- **gRPC com stubs compilados no build:** os arquivos `*_pb2.py` são gerados durante o `docker build`, não commitados no repositório, garantindo que o proto seja sempre a fonte da verdade.
- **Configuração hierárquica:** YAML base → variáveis de ambiente → padrões em código. Cada serviço lê apenas as variáveis que lhe dizem respeito.
- **Logs estruturados JSONL:** todos os serviços emitem eventos JSON Lines em arquivos montados em `./logs/`, facilitando auditoria e correlação entre serviços.

---

## 2. MetadataService — Pessoa 1 (Filipe)

> *Esta seção deve ser preenchida pela Pessoa 1.*

### Responsabilidades
- Alocação de uploads (`AllocateUpload`)
- Commit de uploads (`CommitUpload`)
- Consulta de localização de blocos (`GetBlockLocations`)
- Gestão de namespace (ls, stat, mkdir, rmdir, rename, delete)
- Rastreamento de réplicas e detecção de sub-replicação

### Implementação
*(Filipe: descreva aqui o design do MetadataService, store SQLite WAL, estratégia de posicionamento de blocos, etc.)*

---

## 3. StorageService — Pessoa 2

> *Esta seção deve ser preenchida pela Pessoa 2.*

### Responsabilidades
- Armazenamento de blocos (`StoreBlock` — client-streaming)
- Leitura de blocos (`GetBlock` — server-streaming)
- Scrubbing periódico de integridade (SHA-256)
- Reporte de corrupção ao ControlService
- Heartbeats para o central

### Implementação
*(Pessoa 2: descreva design do storage, scrubber, configuração via env vars, etc.)*

---

## 4. ControlService e Recuperação — Pessoa 3 (Sidnei)

> *Esta seção deve ser preenchida pela Pessoa 3.*

### Responsabilidades
- Registro de nós (`RegisterNode`)
- Processamento de heartbeats (`Heartbeat`)
- Monitoramento de estado: ALIVE → SUSPECT → DEAD
- Gerenciamento de recuperação de blocos (`RecoveryManager`)
- Drenagem de nós (`DrainNode`)
- Reconciliação de inventário (`Reconcile`)

### Implementação
*(Sidnei: descreva NodeRegistry, timeouts, RecoveryManager, reconciliação, etc.)*

---

## 5. CLI e Cliente — Pessoa 4

> *Esta seção deve ser preenchida pela Pessoa 4.*

### Responsabilidades
- 10 comandos oficiais: put, get, ls, stat, mkdir, rmdir, rename, delete, status, drain
- Fatiamento de arquivos em blocos (chunker)
- Criptografia opcional AES-256-GCM (`--key`)
- Tolerância a falhas no upload/download (fallback entre nós)
- Log de operações em `/logs/client.jsonl`

### Implementação
*(Pessoa 4: descreva arquitetura do cliente, chunker, crypto, fallback, etc.)*

---

## 6. Infraestrutura, Observabilidade e Documentação — Pessoa 5 (Arthur)

### 6.1 Infraestrutura Docker

A infraestrutura é definida por um único `Dockerfile` e um `docker-compose.yml` que orquestra todos os serviços.

**Dockerfile:**
- Imagem base `python:3.12-slim` com build multi-stage implícito
- `PYTHONPATH=/app/src` configurado globalmente
- Stubs gRPC compilados em tempo de build via `RUN python scripts/gerar_proto.py`
- Suporte a qualquer papel (central, storage, cli) pelo mesmo binário

**Docker Compose:**
- 1 serviço `central` (MetadataService + ControlService, porta 50051)
- 5 serviços `storage-N` com healthcheck e dependência do central
- 1 serviço `storage-6` com `profile: extra` para demonstração de descoberta automática
- 1 serviço `cli` com `profile: tools` para operações sob demanda
- Alias de rede `controller` no serviço central para compatibilidade

**Volumes compartilhados:**
- `central-data`, `storage-N-data`: persistência de dados por serviço
- `./logs`: diretório compartilhado para logs JSONL de todos os serviços
- `./workspace`: montagem local para a CLI acessar arquivos do host

### 6.2 Configuração

O módulo `src/common/config.py` implementa leitura hierárquica de configuração:
1. YAML base (`config.yaml` ou `config.example.yaml`)
2. Variáveis de ambiente (sobrescrevem o YAML)
3. Padrões em código (fallback)

A classe `Config` é um `dataclass` com validação de tipos e campos obrigatórios. Cada subsistema declara sua própria seção no YAML (`controller`, `cluster`, `observability`).

### 6.3 Logging Estruturado (JSONL)

O módulo `src/common/logging.py` implementa o logger `EventLogger` com as seguintes garantias:

- **Formato JSONL:** uma linha JSON por evento com campos obrigatórios `ts` (UTC ISO-8601), `level`, `component`, `event`
- **Sanitização automática:** campos com nomes sensíveis (key, secret, password, token) são substituídos por `[redacted]`
- **Serialização segura:** bytes curtos viram hex; bytes longos viram `<N bytes>`; mensagens protobuf mostram apenas o nome do tipo
- **Contexto fixo:** `get_logger("storage", node_id="storage-1")` adiciona `node_id` em todas as linhas
- **Catálogo de eventos:** `src/common/events.py` define os eventos oficiais; eventos fora do catálogo são marcados com `uncatalogued=true`
- **Medição de tempo:** `with log.timed("block.stored", block_id=bid) as extra:` adiciona `duration_ms` automaticamente

### 6.4 Métricas Prometheus

O módulo `src/common/metrics.py` expõe um endpoint `/metrics` em cada serviço:

- **Contador de blocos:** `dcomp_blocks_stored_total`, `dcomp_blocks_retrieved_total`
- **Histograma de latência:** `dcomp_grpc_request_duration_seconds` por método
- **Gauge de capacidade:** `dcomp_storage_used_bytes`, `dcomp_storage_available_bytes`
- **Interceptor gRPC:** `MetricsInterceptor` registra automaticamente todas as chamadas
- **StatusCollector:** agrega dados de todos os nós para o comando `status`

### 6.5 CLI de Status (`src/observability/status_cli.py`)

O comando `python -m observability.status` (ou `cli status` via Docker) exibe:
- Estado de saúde de cada nó (ALIVE/SUSPECT/DEAD/DRAINING)
- Capacidade utilizada e disponível por nó
- Contagem de arquivos saudáveis, sub-replicados e em recuperação
- Modo `--watch` para atualização contínua
- Modo `--json` para integração com scripts
- Código de saída: 0 (saudável), 1 (degradado), 2 (crítico)

### 6.6 Scripts de Suporte

| Script | Função |
|---|---|
| `scripts/gerar_proto.py` | Compila stubs gRPC Python a partir de `proto/dcomp_drive.proto` |
| `scripts/validar_proto.py` | Verifica serviços, RPCs, streaming e campos obrigatórios |
| `scripts/verificar_parte1.py` | Valida os 4 cenários mínimos do projeto |

### 6.7 Testes

A suíte de testes do módulo de infraestrutura cobre:

| Arquivo | Cobertura | Testes |
|---|---|---|
| `test_config.py` | Config dataclass, YAML, env vars, validação | ~8 |
| `test_logging.py` | JSON output, sanitização, timed context, bytes | ~12 |
| `test_metrics.py` | Interceptor gRPC, counters, histograms | ~9 |
| `test_status_cli.py` | Status CLI, watch, JSON, exit codes | ~8 |
| **Total** | | **~37 testes** |

Todos os 37 testes passam com `python -m pytest tests/test_config.py tests/test_logging.py tests/test_metrics.py tests/test_status_cli.py -q`.

---

## 7. Conclusão e Limitações

### O que foi entregue na Parte 1

- Cluster totalmente containerizado com Docker Compose
- Comunicação real via gRPC (sem mocks em produção)
- Replicação de blocos com fator 3
- Detecção automática de falhas por heartbeat (ALIVE → SUSPECT → DEAD)
- Recuperação automática de réplicas perdidas
- Detecção de corrupção por scrubbing periódico (SHA-256)
- 10 comandos de CLI funcionais
- Logging estruturado JSONL em todos os serviços
- Métricas Prometheus por serviço
- Suíte de testes unitários com cobertura das camadas críticas

### Limitações conhecidas

- **Sem TLS:** a comunicação gRPC é plaintext (adequado para ambiente de laboratório)
- **SQLite como metadados:** adequado para a Parte 1; em produção seria necessário replicação do metadados
- **Sem autenticação:** a CLI não exige credenciais
- **Volume compartilhado `./logs`:** em um ambiente de produção os logs seriam enviados a um coletor centralizado (ex: Loki, Elasticsearch)
- **Stubs não commitados:** os arquivos `*_pb2.py` são gerados no build; requer `grpc_tools` instalado para testes locais

---

*Relatório gerado em 18/11/2026 — SD-Drive Parte 1*
