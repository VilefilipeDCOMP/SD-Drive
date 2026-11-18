# SD-Drive

## Ambiente de Execução DEV

Na raiz do projeto, crie e ative o ambiente virtual conforme o seu sistema operacional:

**Linux / macOS:**
```bash
python -m venv .venv
source .venv/bin/activate
```

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```
*(Caso o PowerShell bloqueie a execução do script, rode `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` uma única vez e tente ativar novamente.)*

Com o ambiente `(.venv)` ativo, instale as dependências e gere os stubs do gRPC:

```bash
python -m pip install -r requirements-dev.txt
python -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src --pyi_out=src proto/dcomp_drive.proto
```

### Executar os testes

```bash
python -m pytest -q
```

### Executar demonstração (alocação com três nós simulados)

**Linux / macOS:**
```bash
PYTHONPATH=src python -m metadata.demo
```

**Windows (PowerShell):**
```powershell
$env:PYTHONPATH="src"; python -m metadata.demo
```