# SD-Drive — Parte 1

Sistema de armazenamento distribuído com replicação, detecção de falhas e recuperação automática.

## Pré-requisitos

| Ferramenta | Versão ou requisito |
|---|---|
| Docker Engine | 24+ |
| Docker Compose | v2 |
| Python | 3.11+ (apenas para testes e scripts locais) |

---

## Subir o cluster

```bash
git clone https://github.com/VilefilipeDCOMP/SD-Drive.git
cd SD-Drive

docker compose up -d --build --wait
docker compose ps
```

O Dockerfile instala as dependências e gera os stubs gRPC. O `ps` deve mostrar o central e os cinco nós iniciais como **healthy**. Para acompanhar a inicialização, use `docker compose logs -f central`.

---

## Comandos da CLI

A CLI roda na rede Docker. O Compose monta `workspace/` do host em `/work` no contêiner. Coloque os arquivos de entrada em `workspace/` e salve os downloads em `/work` para acessá-los no host.

### Arquivos

Crie o diretório remoto com `docker compose run --rm cli mkdir /drive` se ele ainda não existir. Para os exemplos abaixo, coloque `meu_arquivo.txt` e `secreto.bin` em `workspace/`. O exemplo de criptografia usa `openssl` no host para gerar uma chave de demonstração.

```bash
# enviar arquivo
docker compose run --rm cli put /work/meu_arquivo.txt /drive/meu_arquivo.txt

# baixar arquivo
docker compose run --rm cli get /drive/meu_arquivo.txt /work/baixado.txt

# enviar e baixar com criptografia AES-256-GCM
CHAVE_DEMO=$(openssl rand -hex 32)
docker compose run --rm cli put /work/secreto.bin /drive/secreto.bin --key "$CHAVE_DEMO"
docker compose run --rm cli get /drive/secreto.bin /work/secreto-baixado.bin --key "$CHAVE_DEMO"
```

### Namespace

```bash
docker compose run --rm cli ls /drive
docker compose run --rm cli stat /drive/meu_arquivo.txt
docker compose run --rm cli mkdir /drive/pasta
docker compose run --rm cli rename /drive/meu_arquivo.txt /drive/novo.txt
docker compose run --rm cli delete /drive/novo.txt
docker compose run --rm cli rmdir /drive/pasta
```

### Operacional

```bash
# saúde do cluster e capacidade dos nós
docker compose run --rm cli status

# modo watch (atualiza a cada 2s)
docker compose run --rm --entrypoint python cli -m observability.status_cli --target central:50051 --watch 2
```

Para solicitar o esvaziamento de um nó, copie o UUID exibido no `status` e passe-o como argumento de `docker compose run --rm cli drain`. A resposta indica que a solicitação foi aceita. Use `stat` para conferir as réplicas do arquivo, pois os contadores de arquivos do `status` têm um defeito conhecido.

---

## Métricas Prometheus

O Compose publica `/metrics` dos cinco storages iniciais nas portas abaixo. O processo central atual não serve métricas na porta `9100`.

| Serviço | Porta |
|---|---|
| storage-1 | `localhost:9101` |
| storage-2 | `localhost:9102` |
| storage-3 | `localhost:9103` |
| storage-4 | `localhost:9104` |
| storage-5 | `localhost:9105` |

```bash
curl -fsS http://localhost:9101/metrics
```

---

## Variáveis de ambiente

| Variável | Valor no Compose | Descrição |
|---|---|---|
| `DCOMP_CONFIG` | `/app/config.yaml` | caminho de configuração usado pelo central e pela CLI |
| `DCOMP_CONTROLLER_TARGET` | `central:50051` | endereço do central (storage) |
| `DCOMP_CLIENT__CENTRAL_TARGET` | `central:50051` | endereço do central (cli) |
| `DCOMP_STORAGE_ADVERTISE` | — | endereço que o nó anuncia ao central |
| `DCOMP_MACHINE_ID` | varia por nó | domínio de falha da máquina (simulado no Compose) |
| `DCOMP_DISK_ID` | varia por nó | domínio de falha do disco (simulado no Compose) |
| `DCOMP_STORAGE_CAPACITY_BYTES` | `2147483648` | capacidade lógica do nó (2 GiB) |
| `DCOMP_METRICS_PORT` | `9100` por padrão no storage | porta interna de métricas lida pelo storage |
| `DCOMP_OBSERVABILITY__LOG_FILE` | varia por serviço | caminho de eventos usado pela CLI; para ver os logs dos processos, use `docker compose logs` |

---

## Verificação dos cenários

Os quatro cenários mínimos para verificar no cluster são:
- **V1** — put/get com verificação SHA-256
- **V2** — replicação em 3 nós distintos
- **V3** — falha de nó e recuperação automática
- **V4** — detecção de corrupção por scrubbing

O script `scripts/verificar_parte1.py` ainda precisa usar os caminhos montados e identificar o nó que contém a réplica de teste.

---

## Testes e validação do contrato gRPC

Para executar a suíte local no Bash, crie um ambiente virtual e gere os stubs:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python scripts/gerar_proto.py
python scripts/validar_proto.py
PYTHONPATH=src:tests python -m pytest -q
```

A suíte inclui testes que abrem sockets locais para gRPC e HTTP. Para executar apenas os testes de configuração e logging:

```bash
python -m pytest -q tests/test_config.py tests/test_logging.py
```

---

## Adicionar sexto nó (descoberta automática)

```bash
docker compose --profile extra up -d --no-deps --wait storage-6
docker compose run --rm cli status
```

O central detecta e registra o novo nó automaticamente, sem reiniciar o cluster.

---

## Parar e limpar

```bash
docker compose --profile extra down
```

O comando preserva os volumes. Para zerar um ambiente descartável, use `docker compose --profile extra down -v`, que também apaga os volumes de metadados e blocos.

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
│   └── verificar_parte1.py # verificador experimental
├── docs/
│   └── RELATORIO_PARTE1.md
├── Dockerfile
├── docker-compose.yml
├── config.example.yaml
└── requirements.txt
```
