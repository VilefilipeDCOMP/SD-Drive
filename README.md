# SD-Drive

## Ambiente de Execução DEV

Para preparar o ambiente, execute na raiz do projeto:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src --pyi_out=src proto/dcomp_drive.proto
```

Para executar os testes:

```bash
python -m pytest -q
```

Para ver uma alocação com três nós simulados:

```bash
PYTHONPATH=src python -m metadata.demo
```
