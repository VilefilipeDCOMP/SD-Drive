# SD-Drive — Parte 1

Sistema de armazenamento distribuído com replicação, detecção de falhas e recuperação automática.

## Pré-requisitos

| Ferramenta | Versão mínima |
|---|---|
| Docker + Compose plugin | 24+ |
| Python | 3.11+ (apenas para testes locais) |

---

## Subir o cluster

```bash
git clone https://github.com/VilefilipeDCOMP/SD-Drive.git
cd SD-Drive

# gera stubs gRPC localmente (necessário para testes)
pip install -r requirements-dev.txt
python scripts/gerar_proto.py

# sobe 1 central + 5 nós de storage em background
docker compose up -d --build

# acompanha os logs
docker compose logs -f
```

Após alguns segundos, `docker compose ps` deve mostrar todos os serviços **healthy**.

---

## Comandos da CLI

A CLI roda dentro da rede Docker e enxerga os serviços pelos nomes internos.

### Arquivos

```bash
# enviar arquivo
docker compose run --rm --profile tools cli put /work/meu_arquivo.txt /drive/meu_arquivo.txt

# baixar arquivo
docker compose run --rm --profile tools cli get /drive/meu_arquivo.txt /work/baixado.txt

# enviar com criptografia AES-256-GCM
docker compose run --rm --profile tools cli put /work/secreto.bin /drive/secreto.bin --key "minha-chave-32b"
docker compose run --rm --profile tools cli get /drive/secreto.bin /work/secreto.bin --key "minha-chave-32b"
```

### Namespace

```bash
docker compose run --rm --profile tools cli ls /drive/
docker compose run --rm --profile tools cli stat /drive/meu_arquivo.txt
docker compose run --rm --profile tools cli mkdir /drive/pasta
docker compose run --rm --profile tools cli rename /drive/antigo.txt /drive/novo.txt
docker compose run --rm --profile tools cli delete /drive/meu_arquivo.txt
docker compose run --rm --profile tools cli rmdir /drive/pasta
```

### Operacional

```bash
# saúde do cluster e capacidade dos nós
docker compose run --rm --profile tools cli status

# modo watch (atualiza a cada 2s)
docker compose run --rm --profile tools cli status --watch

# drenar um nó (migra blocos para outros nós)
docker compose run --rm --profile tools cli drain storage-3
```

> **Dica:** monte arquivos locais em `/work` usando `-v $(pwd)/meus_arquivos:/work`.

---

## Métricas Prometheus

Cada serviço expõe `/metrics` em sua porta:

| Serviço | Porta |
|---|---|
| central | `localhost:9100` |
| storage-1 | `localhost:9101` |
| storage-2 | `localhost:9102` |
| storage-3 | `localhost:9103` |
| storage-4 | `localhost:9104` |
| storage-5 | `localhost:9105` |

```bash
curl http://localhost:9100/metrics
```

---

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `DCOMP_CONFIG` | `config.yaml` | caminho do arquivo de configuração |
| `DCOMP_CONTROLLER_TARGET` | `central:50051` | endereço do central (storage) |
| `DCOMP_CLIENT__CENTRAL_TARGET` | `central:50051` | endereço do central (cli) |
| `DCOMP_STORAGE_ADVERTISE` | — | endereço que o nó anuncia ao central |
| `DCOMP_MACHINE_ID` | — | id da máquina física |
| `DCOMP_DISK_ID` | — | id do disco |
| `DCOMP_STORAGE_CAPACITY_BYTES` | `2147483648` | capacidade lógica do nó (2 GiB) |
| `DCOMP_OBSERVABILITY__METRICS_PORT` | `9100` | porta do endpoint `/metrics` |
| `DCOMP_OBSERVABILITY__LOG_FILE` | `/logs/central.jsonl` | arquivo de log estruturado |

---

## Verificação dos cenários

```bash
python scripts/verificar_parte1.py
```

Verifica automaticamente os 4 cenários mínimos:
- **V1** — put/get com verificação SHA-256
- **V2** — replicação em 3 nós distintos
- **V3** — falha de nó e recuperação automática
- **V4** — detecção de corrupção por scrubbing

---

## Validar proto

```bash
python scripts/validar_proto.py
```

Recompila os stubs e verifica serviços, RPCs e assinaturas de streaming.

---

## Testes unitários

```bash
# instalar dependências
pip install -r requirements-dev.txt

# gerar stubs
python scripts/gerar_proto.py

# rodar todos os testes
python -m pytest -q

# somente testes que não precisam de rede
python -m pytest tests/test_config.py tests/test_logging.py tests/test_metrics.py tests/test_status_cli.py -q
```

---

## Adicionar sexto nó (descoberta automática)

```bash
docker compose --profile extra up -d storage-6
```

O central detecta e registra o novo nó automaticamente, sem reiniciar o cluster.

---

## Parar e limpar

```bash
docker compose down          # para e remove containers
docker compose down -v       # também remove volumes (apaga dados)
```

---

## Estrutura do projeto

```
SD-Drive/
├── proto/                  # contrato gRPC
│   └── dcomp_drive.proto
├── src/
│   ├── central/            # processo central (entrypoint)
│   ├── common/             # config, logging json, métricas, eventos
│   ├── controller/         # ControlService + NodeRegistry + RecoveryManager (pessoa 3)
│   ├── metadata/           # MetadataService + SQLite WAL (pessoa 1)
│   ├── storage/            # StorageService + scrubber + corrupt_tool (pessoa 2)
│   ├── client/             # CLI + chunker + criptografia AES-256-GCM (pessoa 4)
│   └── observability/      # status CLI + métricas (pessoa 5)
├── tests/                  # testes unitários e de integração
├── scripts/
│   ├── gerar_proto.py      # compila stubs gRPC
│   ├── validar_proto.py    # valida serviços e assinaturas
│   └── verificar_parte1.py # valida os 4 cenários mínimos
├── docs/
│   ├── RELATORIO_PARTE1.md
│   └── ROTEIRO_DEMO.md
├── Dockerfile
├── docker-compose.yml
├── config.example.yaml
└── requirements.txt
```
