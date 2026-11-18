# Roteiro de Demonstração — SD-Drive Parte 1

**Tempo total:** ≤ 5 minutos  
**Requisito:** Docker e Docker Compose instalados, repositório clonado.

---

## Preparação (antes da demo — não conta no tempo)

```bash
git clone https://github.com/VilefilipeDCOMP/SD-Drive.git
cd SD-Drive
pip install -r requirements-dev.txt
python scripts/gerar_proto.py
docker compose build
```

---

## Demo (5 minutos)

### [0:00 – 0:30] Subir o cluster

```bash
docker compose up -d
docker compose ps
```

**Mostrar:** 7 serviços (central + storage-1..5 + storage-6 parado) todos **healthy**.

---

### [0:30 – 1:30] Put e Get de arquivo (cenário V1)

```bash
# criar arquivo de teste
echo "SD-Drive funciona!" > /tmp/teste.txt

# enviar para o cluster
docker compose run --rm --profile tools cli put /tmp/teste.txt /drive/teste.txt

# baixar de volta
docker compose run --rm --profile tools cli get /drive/teste.txt /tmp/baixado.txt

# verificar conteúdo idêntico
diff /tmp/teste.txt /tmp/baixado.txt && echo "ARQUIVOS IDENTICOS"

# listar namespace
docker compose run --rm --profile tools cli ls /drive/
```

---

### [1:30 – 2:00] Status do cluster e replicação (cenário V2)

```bash
docker compose run --rm --profile tools cli status
```

**Mostrar:** 5 nós ALIVE, capacidade de cada um, blocos distribuídos em 3 nós distintos.

```bash
# ver métricas brutas de um nó
curl http://localhost:9101/metrics | grep dcomp_blocks
```

---

### [2:00 – 3:30] Falha e recuperação automática (cenário V3)

```bash
# derrubar um nó
docker compose stop storage-3
```

**Aguardar 15 segundos**, depois:

```bash
# mostrar detecção
docker compose run --rm --profile tools cli status
```

**Mostrar:** storage-3 aparece como SUSPECT ou DEAD. Blocos sub-replicados sendo recuperados.

```bash
# reiniciar o nó
docker compose start storage-3
```

**Aguardar 15 segundos**, depois:

```bash
docker compose run --rm --profile tools cli status
```

**Mostrar:** storage-3 voltou para ALIVE, cluster saudável.

---

### [3:30 – 4:30] Detecção de corrupção (cenário V4)

```bash
# simular corrupção em um bloco do storage-1
docker compose exec storage-1 python -m storage.corrupt_tool --random
```

**Aguardar o scrubber detectar (~30-60s)**, depois:

```bash
# ver log de corrupção detectada
docker compose exec storage-1 grep corruption /logs/storage-1.jsonl | tail -3
```

**Mostrar:** evento de corrupção detectada com `block_id` e ação tomada (re-replicação).

---

### [4:30 – 5:00] Descoberta de novo nó

```bash
# adicionar sexto nó sem reiniciar o cluster
docker compose --profile extra up -d storage-6

# verificar que o nó foi registrado
docker compose run --rm --profile tools cli status
```

**Mostrar:** storage-6 aparece como ALIVE, cluster agora com 6 nós.

---

## Comandos de limpeza

```bash
docker compose --profile extra down
docker compose down -v
```

---

## Pontos de atenção durante a demo

- Se o `docker compose ps` mostrar algum serviço `unhealthy`, aguardar mais 10–15s e verificar `docker compose logs central`
- O `cli status` precisa do `--profile tools`: `docker compose run --rm --profile tools cli status`
- Os logs ficam em `./logs/` na raiz do projeto — boa forma de mostrar observabilidade
- Para Windows, substitua `/tmp/` por um caminho local: `C:\Temp\`
