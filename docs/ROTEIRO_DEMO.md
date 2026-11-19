# Roteiro de Demonstração — SD-Drive Parte 1

**Tempo alvo:** até 5 minutos. As etapas de falha e corrupção dependem dos intervalos do cluster.  
**Requisito:** Docker e Docker Compose instalados, repositório clonado.

---

## Preparação (antes da demo — não conta no tempo)

Execute os comandos na raiz do repositório e no mesmo terminal para manter as variáveis. O Dockerfile instala as dependências e gera os stubs. Para demonstrar a entrada do sexto nó, comece com o `storage-6` parado.

```bash
docker compose build
```

---

## Demo (5 minutos)

### [0:00 – 0:30] Subir o cluster

```bash
docker compose up -d --wait
docker compose ps
```

**Mostrar:** seis serviços **healthy**: o central e os cinco storages iniciais.

---

### [0:30 – 1:30] Put e Get de arquivo (cenário V1)

Crie o diretório remoto uma vez. Se `/drive` já existir, pule o `mkdir` da CLI.

```bash
docker compose run --rm cli mkdir /drive
```

A pasta `workspace/` do host é montada em `/work` na CLI. Cada execução usa um nome novo para preservar os arquivos das demos anteriores.

```bash
mkdir -p workspace
PASTA_DEMO=$(mktemp -d workspace/demo.XXXXXX)
# Permitir que o usuário da CLI no contêiner grave os arquivos baixados.
chmod a+rwx "$PASTA_DEMO"
NOME_DEMO=${PASTA_DEMO##*/}
ARQUIVO_REMOTO="/drive/${NOME_DEMO}.txt"
printf '%s\n' 'SD-Drive funciona!' > "$PASTA_DEMO/original.txt"

docker compose run --rm cli put "/work/$NOME_DEMO/original.txt" "$ARQUIVO_REMOTO"
docker compose run --rm cli get "$ARQUIVO_REMOTO" "/work/$NOME_DEMO/baixado.txt"
diff "$PASTA_DEMO/original.txt" "$PASTA_DEMO/baixado.txt" && echo "ARQUIVOS IDÊNTICOS"
docker compose run --rm cli ls /drive
```

---

### [1:30 – 2:00] Status do cluster e replicação (cenário V2)

```bash
docker compose run --rm cli status
docker compose run --rm cli stat "$ARQUIVO_REMOTO"
```

**Mostrar:** cinco nós `NODE_ALIVE` e suas capacidades no `status`. Use o `stat` para mostrar o bloco e suas três réplicas em nós distintos. Os contadores de arquivos do `status` têm um defeito conhecido.

```bash
# ver métricas brutas de um nó
curl -fsS http://localhost:9101/metrics | grep '^dcomp_storage_blocks '
```

---

### [2:00 – 3:30] Falha e recuperação automática (cenário V3)

Escolha um dos nós com réplica saudável no `stat`.

```bash
read -r -p 'Nó com réplica (ex.: storage-3): ' NO_FALHA
docker compose stop "$NO_FALHA"
```

Consulte o estado do nó e as réplicas enquanto ele estiver parado. Repita as consultas até o `stat` mostrar a recomposição.

```bash
docker compose run --rm cli status
docker compose run --rm cli stat "$ARQUIVO_REMOTO"
```

**Mostrar:** o nó parado aparece como `NODE_SUSPECT` ou `NODE_DEAD`. A recuperação fica comprovada quando o arquivo volta a `REPLICATION_HEALTHY` com três réplicas saudáveis e um novo destino.

```bash
docker compose start "$NO_FALHA"
docker compose run --rm cli status
```

**Mostrar:** o nó voltou para `NODE_ALIVE`. Se ainda estiver registrando, repita o `status`.

---

### [3:30 – 4:30] Detecção de corrupção (cenário V4)

Escolha uma réplica saudável do arquivo da demo no `stat`. Na listagem do storage, copie o UUID completo da primeira coluna que corresponde ao prefixo exibido pelo `stat`. A segunda coluna é o tamanho em bytes.

```bash
docker compose run --rm cli stat "$ARQUIVO_REMOTO"
read -r -p 'Nó com réplica do arquivo da demo: ' NO_CORRUPCAO
docker compose exec "$NO_CORRUPCAO" python -m storage.corrupt_tool --list
read -r -p 'UUID completo do bloco da demo: ' BLOCO_ID
docker compose exec "$NO_CORRUPCAO" python -m storage.corrupt_tool --block-id "$BLOCO_ID"
```

Após a próxima verificação do scrubber (intervalo padrão de 60 segundos), consulte os eventos do bloco:

```bash
docker compose logs --since 2m "$NO_CORRUPCAO" central | grep -F "$BLOCO_ID"
docker compose run --rm cli stat "$ARQUIVO_REMOTO"
docker compose run --rm cli get "$ARQUIVO_REMOTO" "/work/$NOME_DEMO/apos-corrupcao.txt"
diff "$PASTA_DEMO/original.txt" "$PASTA_DEMO/apos-corrupcao.txt" && echo "ARQUIVOS IDÊNTICOS"
```

**Mostrar:** detecção e descarte da cópia corrompida, além do download íntegro. Se restarem menos de três réplicas saudáveis, confira a recomposição no `stat`. Se já restarem três, nenhuma nova cópia precisa ser criada.

---

### [4:30 – 5:00] Descoberta de novo nó

```bash
docker compose ps
docker compose --profile extra up -d --no-deps --wait storage-6
docker compose run --rm cli status
docker compose --profile extra ps
```

**Mostrar:** seis nós registrados, com o `storage-6` em `NODE_ALIVE`. Compare o tempo de atividade do central e dos cinco nós originais antes e depois.

---

## Encerrar a demo

Para parar o cluster e preservar os volumes:

```bash
docker compose --profile extra down
```

---

Para zerar um ambiente descartável, use `docker compose --profile extra down -v` no lugar do comando acima. A opção `-v` apaga os volumes de metadados e blocos.

## Pontos de atenção durante a demo

- Se algum serviço ficar `unhealthy`, consulte `docker compose logs --tail 50 <serviço>`.
- Execute os blocos em Bash. No Windows, use Bash via WSL e mantenha os arquivos da demo em `workspace/`.
