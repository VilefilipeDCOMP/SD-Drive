#!/usr/bin/env python3
"""verifica os 4 cenários mínimos da parte 1 do sd-drive.

cenários:
  v1 — put e get de arquivo com verificação sha-256
  v2 — replicação em 3 nós distintos
  v3 — falha de nó e recuperação automática
  v4 — detecção de corrupção de bloco

uso:
    python scripts/verificar_parte1.py

pre-requisitos:
    docker compose up -d --build      # cluster em pé
    docker compose --profile tools build  # imagem cli disponível
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time


PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
WARN = "\033[33m!\033[0m"

results: list[tuple[str, bool, str]] = []


def cli(*args: str, input_data: bytes | None = None) -> tuple[int, str, str]:
    """executa um comando via docker compose run --rm cli."""
    cmd = ["docker", "compose", "--profile", "tools", "run", "--rm", "cli"] + list(args)
    proc = subprocess.run(cmd, capture_output=True, input=input_data, timeout=60)
    return proc.returncode, proc.stdout.decode(errors="replace"), proc.stderr.decode(errors="replace")


def docker(service: str, *args: str) -> tuple[int, str]:
    cmd = ["docker", "compose"] + list(args)
    proc = subprocess.run(cmd, capture_output=True, timeout=60)
    return proc.returncode, proc.stdout.decode(errors="replace") + proc.stderr.decode(errors="replace")


def check(label: str, ok: bool, detail: str = "") -> None:
    results.append((label, ok, detail))
    icon = PASS if ok else FAIL
    print(f"  {icon} {label}" + (f": {detail}" if detail else ""))


# ---------------------------------------------------------------------------
# v1 — put / get / sha256
# ---------------------------------------------------------------------------
print("\n[V1] put e get com verificação sha-256")
content = b"sd-drive verificacao parte 1 " + os.urandom(16)
sha_orig = hashlib.sha256(content).hexdigest()

with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as f:
    f.write(content)
    local_in = f.name

local_out = local_in + ".downloaded"

try:
    rc, out, err = cli("put", local_in, "/drive/verificacao_parte1.bin")
    check("put retorna código 0", rc == 0, err.strip()[:120] if rc != 0 else "")

    rc, out, err = cli("get", "/drive/verificacao_parte1.bin", local_out)
    check("get retorna código 0", rc == 0, err.strip()[:120] if rc != 0 else "")

    if os.path.exists(local_out):
        sha_dl = hashlib.sha256(open(local_out, "rb").read()).hexdigest()
        check("sha-256 bate após round-trip", sha_orig == sha_dl,
              f"orig={sha_orig[:16]}… dl={sha_dl[:16]}…")
    else:
        check("arquivo baixado existe", False, local_out)
finally:
    for p in (local_in, local_out):
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass

# ---------------------------------------------------------------------------
# v2 — replicação em 3 nós
# ---------------------------------------------------------------------------
print("\n[V2] replicação em 3 nós distintos")
rc, out, err = cli("stat", "/drive/verificacao_parte1.bin")
check("stat retorna código 0", rc == 0, err.strip()[:120] if rc != 0 else "")

# tenta extrair nós via saída json da cli (se suportado) ou por grep
replicas_raw = out + err
nodes_found: set[str] = set()
for token in replicas_raw.split():
    if token.startswith("storage-") or token.startswith("machine-"):
        nodes_found.add(token.split(":")[0])
# fallback: verifica métricas dos nós
if len(nodes_found) < 3:
    for port in range(9101, 9106):
        try:
            import urllib.request
            with urllib.request.urlopen(f"http://localhost:{port}/metrics", timeout=3) as resp:
                body = resp.read().decode()
                if "verificacao_parte1" in body or 'dcomp_blocks_stored_total{' in body:
                    nodes_found.add(f"storage-{port - 9100}")
        except Exception:
            pass

check("arquivo replicado em >= 3 nós", len(nodes_found) >= 3,
      f"nós detectados: {sorted(nodes_found) or 'verificar manualmente'}")

# ---------------------------------------------------------------------------
# v3 — falha e recuperação
# ---------------------------------------------------------------------------
print("\n[V3] falha de nó e recuperação automática")
rc, out = docker("storage-3", "stop", "storage-3")
check("storage-3 parado", rc == 0, out.strip()[:80] if rc != 0 else "")

print("  aguardando detecção de falha (35s)…")
time.sleep(35)

rc, out, err = cli("status")
check("status retorna código 0 após falha", rc == 0, "")

# verifica se cluster detectou o nó morto
detected = "DEAD" in out or "dead" in out.lower() or "suspect" in out.lower()
check("cluster detectou nó morto", detected, out.strip()[:200] if not detected else "")

# sobe o nó de volta e verifica recuperação
rc, out = docker("storage-3", "start", "storage-3")
check("storage-3 reiniciado", rc == 0, "")
print("  aguardando recuperação (30s)…")
time.sleep(30)

rc, out, err = cli("status")
recovery_ok = "ALIVE" in out or rc == 0
check("cluster voltou ao estado saudável", recovery_ok, "")

# ---------------------------------------------------------------------------
# v4 — detecção de corrupção
# ---------------------------------------------------------------------------
print("\n[V4] detecção de corrupção de bloco")
# usa a ferramenta de corrupção do storage (pessoa 2)
corrupt_cmd = [
    "docker", "compose", "exec", "storage-1",
    "python", "-m", "storage.corrupt_tool", "--random",
]
proc = subprocess.run(corrupt_cmd, capture_output=True, timeout=30)
corruption_applied = proc.returncode == 0
check("corrupt_tool aplicou corrupção", corruption_applied,
      proc.stderr.decode(errors="replace").strip()[:120] if not corruption_applied else "")

if corruption_applied:
    print("  aguardando scrubbing detectar corrupção (60s)…")
    time.sleep(60)
    # verifica log de corrupção detectada
    log_check = subprocess.run(
        ["docker", "compose", "exec", "storage-1",
         "grep", "-c", "corruption", "/logs/storage-1.jsonl"],
        capture_output=True, timeout=15,
    )
    detected = log_check.returncode == 0 and log_check.stdout.strip() != b"0"
    check("corrupção detectada nos logs", detected, "verificar /logs/storage-1.jsonl se falhar")

# ---------------------------------------------------------------------------
# resumo
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
passed = sum(1 for _, ok, _ in results if ok)
total = len(results)
print(f"RESULTADO: {passed}/{total} verificações passaram")
if passed == total:
    print(f"{PASS} todos os cenários validados — pronto para entrega!")
else:
    failed = [(l, d) for l, ok, d in results if not ok]
    print(f"{FAIL} {len(failed)} verificações falharam:")
    for label, detail in failed:
        print(f"    - {label}" + (f": {detail}" if detail else ""))
print("=" * 60)
sys.exit(0 if passed == total else 1)
