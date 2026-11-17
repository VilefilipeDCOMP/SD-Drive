# imagem unica para central, nos de armazenamento e cli; o papel e escolhido pelo command do compose
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# dependencias primeiro, para o cache do docker nao refazer o pip a cada mudanca de codigo
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY proto ./proto
COPY src ./src
COPY config.example.yaml ./config.yaml

# stubs gerados no build: ninguem precisa de protoc no host e a imagem nunca fica com stub velho
RUN python -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/dcomp_drive.proto

# usuario comum, porque nao ha motivo para rodar como root
RUN useradd --create-home --uid 10001 dcomp && mkdir -p /data /logs && chown -R dcomp /data /logs /app
USER dcomp

CMD ["python", "-m", "placeholder", "central"]

