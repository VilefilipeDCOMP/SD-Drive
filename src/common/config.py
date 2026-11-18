"""leitor de configuracao do dcomp drive.

junta quatro camadas, nesta ordem de prioridade (a ultima ganha):
defaults do codigo < arquivo yaml < variaveis DCOMP_<SECAO>__<CHAVE> < overrides da linha de comando.

tudo e validado de uma vez so e os problemas voltam juntos num ConfigError, assim quem
sobe o ambiente ve a lista inteira em vez de corrigir um erro por vez.
o resultado e imutavel (dataclasses frozen) e cada modulo le apenas a sua secao.

uso tipico:

    from common.config import load_config
    cfg = load_config()                       # usa DCOMP_CONFIG se estiver definido
    n = cfg.cluster.replication_factor
    w = cfg.cluster.resolve_write_quorum(n)   # "all" vira N, inteiro e conferido contra N

para conferir uma configuracao sem subir nada:

    python -m common.config --config config.example.yaml --set cluster.replication_factor=2
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import types
import typing
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

ENV_PREFIX = "DCOMP_"
CONFIG_PATH_ENV = "DCOMP_CONFIG"

NODE_STATES = ("ALIVE", "SUSPECT", "DEAD", "DRAINING")
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
LOG_FORMATS = ("json", "console")
ENCRYPTION_ALGORITHMS = ("AES-256-GCM",)

# limite padrao de mensagem do grpc; o frame precisa caber com folga para o envelope protobuf
GRPC_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
GRPC_ENVELOPE_MARGIN_BYTES = 64 * 1024


class ConfigError(ValueError):
    """configuracao invalida; `problems` traz cada problema encontrado em uma linha."""

    def __init__(self, problems: Iterable[str]):
        self.problems = list(problems)
        lines = "\n".join(f"  - {p}" for p in self.problems)
        super().__init__(f"configuracao invalida ({len(self.problems)} problema(s)):\n{lines}")


class _CoerceError(Exception):
    pass


@dataclass(frozen=True)
class ClusterConfig:
    replication_factor: int = 3
    write_quorum: int | str = "all"
    reject_when_insufficient_distinct_nodes: bool = True
    max_disk_usage_fraction: float = 0.90

    def resolve_write_quorum(self, replication_factor: int | None = None) -> int:
        """devolve o W efetivo para um N (o N do cluster se nenhum for passado).

        "all" vira N. um inteiro fora de 1..N gera ValueError de proposito: o contrato
        proibe trocar por min(W, N) sem avisar, porque isso muda a durabilidade prometida.
        """
        n = self.replication_factor if replication_factor is None else replication_factor
        if n < 1:
            raise ValueError(f"replication_factor precisa ser >= 1 (recebido {n})")
        if self.write_quorum == "all":
            return n
        w = int(self.write_quorum)
        if not 1 <= w <= n:
            raise ValueError(
                f"write_quorum={w} invalido para N={n}: precisa ficar entre 1 e N "
                "(o sistema nao reduz W automaticamente)"
            )
        return w


@dataclass(frozen=True)
class ControllerConfig:
    grpc_bind: str = "0.0.0.0:50051"
    metadata_db_path: str = "/data/metadata.db"
    heartbeat_interval_seconds: int = 3
    suspect_timeout_seconds: int = 10
    dead_timeout_seconds: int = 30
    max_concurrent_recoveries: int = 2
    upload_lease_seconds: int = 900
    upload_max_lifetime_seconds: int = 14400
    placement_eligible_node_states: tuple[str, ...] = ("ALIVE",)


@dataclass(frozen=True)
class StorageConfig:
    grpc_bind: str = "0.0.0.0:50052"
    central_target: str = "controller:50051"
    advertise_addr: str = ""
    machine_id: str = ""
    disk_id: str = ""
    node_id_file: str = "/data/node_id"
    block_directory: str = "/data/blocks"
    capacity_bytes: int = 0
    scrub_interval_seconds: int = 60

    def resolve_advertise_addr(self, hostname: str) -> str:
        """endereco anunciado ao central; sem valor explicito usa hostname + porta do bind."""
        if self.advertise_addr:
            return self.advertise_addr
        port = self.grpc_bind.rpartition(":")[2]
        return f"{hostname}:{port}"

    def resolve_machine_id(self, hostname: str) -> str:
        return self.machine_id or hostname

    def resolve_disk_id(self, hostname: str) -> str:
        return self.disk_id or f"{self.resolve_machine_id(hostname)}-disk0"


@dataclass(frozen=True)
class ClientConfig:
    central_target: str = "controller:50051"
    logical_chunk_size_bytes: int = 4 * 1024 * 1024
    grpc_frame_size_bytes: int = 256 * 1024
    lease_renew_interval_seconds: int = 180
    upload_rpc_deadline_seconds: float = 45.0
    upload_retry_max_attempts: int = 3
    upload_retry_backoff_initial_seconds: float = 1.0
    upload_retry_backoff_max_seconds: float = 4.0
    upload_retry_backoff_jitter_fraction: float = 0.2
    encryption_enabled: bool = False
    encryption_algorithm: str = "AES-256-GCM"


@dataclass(frozen=True)
class ObservabilityConfig:
    metrics_port: int = 9100
    log_format: str = "json"
    log_level: str = "INFO"
    log_file: str = ""
    strict_events: bool = False


@dataclass(frozen=True)
class Config:
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    controller: ControllerConfig = field(default_factory=ControllerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    client: ClientConfig = field(default_factory=ClientConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    # de onde veio o arquivo yaml, so para aparecer no log config.loaded
    source: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """versao serializavel em json (tuplas viram listas)."""
        return json.loads(json.dumps(asdict(self)))


def _section_types() -> dict[str, type]:
    hints = typing.get_type_hints(Config)
    return {name: hint for name, hint in hints.items() if is_dataclass(hint)}


def load_config(
    path: str | os.PathLike[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> Config:
    """carrega, mescla e valida a configuracao.

    path: arquivo yaml; se ficar None usa a variavel DCOMP_CONFIG e, sem ela, so os defaults.
    env: ambiente a considerar (os.environ por padrao; os testes passam um dict proprio).
    overrides: valores da linha de comando, aninhados ({"cluster": {"replication_factor": 2}})
        ou com ponto ({"cluster.replication_factor": 2}).
    """
    env = os.environ if env is None else env
    problems: list[str] = []
    # secao -> chave -> (valor bruto, origem); camadas posteriores sobrescrevem as anteriores
    layers: dict[str, dict[str, tuple[Any, str]]] = {}

    if path is None:
        path = env.get(CONFIG_PATH_ENV) or None
    source = None
    if path is not None:
        source = str(path)
        _merge_layer(layers, _read_yaml(Path(path)), f"arquivo {source}", problems)

    _merge_layer(layers, _env_layer(env, problems), "ambiente", problems)

    if overrides:
        _merge_layer(layers, _normalize_overrides(overrides, problems), "linha de comando", problems)

    section_types = _section_types()
    built: dict[str, Any] = {}
    for section_name, raw_values in layers.items():
        if section_name not in section_types:
            origins = sorted({origin for _, origin in raw_values.values()})
            problems.append(f"secao desconhecida '{section_name}' (origem: {', '.join(origins)})")
    for section_name, section_cls in section_types.items():
        built[section_name] = _build_section(section_name, section_cls, layers.get(section_name, {}), problems)

    if problems:
        raise ConfigError(problems)

    cfg = Config(**built, source=source)
    problems.extend(validate(cfg))
    if problems:
        raise ConfigError(problems)
    return cfg


def _read_yaml(path: Path) -> Mapping[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError([f"arquivo de configuracao nao encontrado: {path}"]) from None
    except OSError as exc:
        raise ConfigError([f"nao foi possivel ler {path}: {exc}"]) from None
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError([f"yaml invalido em {path}: {exc}"]) from None
    if data is None:
        return {}
    if not isinstance(data, Mapping):
        raise ConfigError([f"{path}: o documento precisa ser um mapa de secoes (cluster:, controller:, ...)"])
    return data


def _env_layer(env: Mapping[str, str], problems: list[str]) -> dict[str, dict[str, Any]]:
    # so entram variaveis no formato DCOMP_SECAO__CHAVE; DCOMP_CONFIG, DCOMP_INSTANCE etc. ficam de fora
    layer: dict[str, dict[str, Any]] = {}
    for name, value in env.items():
        upper = name.upper()
        if not upper.startswith(ENV_PREFIX):
            continue
        rest = upper[len(ENV_PREFIX):]
        if "__" not in rest:
            continue
        section, _, key = rest.partition("__")
        if not section or not key:
            problems.append(f"variavel de ambiente mal formada: {name} (use DCOMP_SECAO__CHAVE)")
            continue
        layer.setdefault(section.lower(), {})[key.lower()] = value
    return layer


def _normalize_overrides(overrides: Mapping[str, Any], problems: list[str]) -> dict[str, dict[str, Any]]:
    layer: dict[str, dict[str, Any]] = {}
    for key, value in overrides.items():
        if isinstance(value, Mapping):
            layer.setdefault(str(key), {}).update(value)
            continue
        section, dot, option = str(key).partition(".")
        if not dot or not section or not option:
            problems.append(f"override invalido '{key}': use secao.chave=valor")
            continue
        layer.setdefault(section, {})[option] = value
    return layer


def _merge_layer(
    layers: dict[str, dict[str, tuple[Any, str]]],
    layer: Mapping[str, Any],
    origin: str,
    problems: list[str],
) -> None:
    for section, values in layer.items():
        if values is None:
            continue
        if not isinstance(values, Mapping):
            problems.append(f"{origin}: a secao '{section}' precisa ser um mapa de chave: valor")
            continue
        target = layers.setdefault(str(section), {})
        for key, value in values.items():
            target[str(key)] = (value, origin)


def _build_section(
    section_name: str,
    section_cls: type,
    raw_values: Mapping[str, tuple[Any, str]],
    problems: list[str],
) -> Any:
    hints = typing.get_type_hints(section_cls)
    known = {f.name for f in fields(section_cls)}
    kwargs: dict[str, Any] = {}
    for key, (value, origin) in raw_values.items():
        where = f"{section_name}.{key} ({origin})"
        if key not in known:
            suggestion = _closest(key, known)
            hint = f"; voce quis dizer '{suggestion}'?" if suggestion else ""
            problems.append(f"chave desconhecida {section_name}.{key} ({origin}){hint}")
            continue
        try:
            kwargs[key] = _coerce(value, hints[key], where)
        except _CoerceError as exc:
            problems.append(str(exc))
    try:
        return section_cls(**kwargs)
    except TypeError as exc:
        problems.append(f"{section_name}: {exc}")
        return section_cls()


def _closest(key: str, candidates: Iterable[str]) -> str | None:
    import difflib

    matches = difflib.get_close_matches(key, list(candidates), n=1, cutoff=0.75)
    return matches[0] if matches else None


def _coerce(value: Any, hint: Any, where: str) -> Any:
    # converte o valor bruto (do yaml ou texto vindo do ambiente) para o tipo declarado no dataclass
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        args = set(typing.get_args(hint))
        if args == {int, str}:
            return _coerce_int_or_text(value, where)
        raise _CoerceError(f"{where}: tipo nao suportado pelo leitor ({hint!r})")
    if origin is tuple:
        return _coerce_text_tuple(value, where)
    if hint is bool:
        return _coerce_bool(value, where)
    if hint is int:
        return _coerce_int(value, where)
    if hint is float:
        return _coerce_float(value, where)
    if hint is str:
        if isinstance(value, str):
            return value
        raise _CoerceError(f"{where}: esperado texto, recebido {type(value).__name__} ({value!r})")
    raise _CoerceError(f"{where}: tipo nao suportado pelo leitor ({hint!r})")


_TRUE = {"true", "1", "yes", "y", "on", "sim", "s"}
_FALSE = {"false", "0", "no", "n", "off", "nao"}


def _coerce_bool(value: Any, where: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
    raise _CoerceError(f"{where}: esperado booleano (true/false), recebido {value!r}")


def _coerce_int(value: Any, where: str) -> int:
    # bool e subclasse de int em python, entao precisa ser barrado antes
    if isinstance(value, bool):
        raise _CoerceError(f"{where}: esperado inteiro, recebido booleano {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"\s*[+-]?\d+\s*", value):
        return int(value)
    raise _CoerceError(f"{where}: esperado inteiro, recebido {value!r}")


def _coerce_float(value: Any, where: str) -> float:
    if isinstance(value, bool):
        raise _CoerceError(f"{where}: esperado numero, recebido booleano {value!r}")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
    raise _CoerceError(f"{where}: esperado numero, recebido {value!r}")


def _coerce_int_or_text(value: Any, where: str) -> int | str:
    # usado no write_quorum: aceita "all" ou um inteiro, inclusive "2" vindo do ambiente
    if isinstance(value, bool):
        raise _CoerceError(f"{where}: esperado \"all\" ou inteiro, recebido booleano {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"[+-]?\d+", text):
            return int(text)
        return text.lower()
    raise _CoerceError(f"{where}: esperado \"all\" ou inteiro, recebido {value!r}")


def _coerce_text_tuple(value: Any, where: str) -> tuple[str, ...]:
    # no ambiente aceita lista json (["ALIVE"]) ou texto separado por virgula (ALIVE,SUSPECT)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                raise _CoerceError(f"{where}: lista json invalida: {value!r}") from None
        else:
            value = [part for part in (p.strip() for p in text.split(",")) if part]
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
        return tuple(value)
    raise _CoerceError(f"{where}: esperado lista de textos, recebido {value!r}")


def _check_address(value: str, where: str, problems: list[str]) -> None:
    host, sep, port = value.rpartition(":")
    if not sep or not host:
        problems.append(f"{where}: endereco '{value}' precisa ter o formato host:porta")
        return
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        problems.append(f"{where}: porta invalida em '{value}' (use 1..65535)")


def validate(cfg: Config) -> list[str]:
    """regras de negocio e coerencia entre campos; devolve a lista de problemas (vazia = ok)."""
    p: list[str] = []
    cl, ct, st, cli, obs = cfg.cluster, cfg.controller, cfg.storage, cfg.client, cfg.observability

    if cl.replication_factor < 1:
        p.append(f"cluster.replication_factor precisa ser >= 1 (recebido {cl.replication_factor})")
    if isinstance(cl.write_quorum, str):
        if cl.write_quorum != "all":
            p.append(f"cluster.write_quorum aceita \"all\" ou inteiro 1..N (recebido {cl.write_quorum!r})")
    elif cl.replication_factor >= 1 and not 1 <= cl.write_quorum <= cl.replication_factor:
        p.append(
            f"cluster.write_quorum={cl.write_quorum} fora de 1..{cl.replication_factor} (N); "
            "corrija o valor, o sistema nao reduz W sozinho"
        )
    if not 0 < cl.max_disk_usage_fraction <= 1:
        p.append(f"cluster.max_disk_usage_fraction precisa estar em (0, 1] (recebido {cl.max_disk_usage_fraction})")

    _check_address(ct.grpc_bind, "controller.grpc_bind", p)
    if not ct.metadata_db_path.strip():
        p.append("controller.metadata_db_path nao pode ser vazio")
    if ct.heartbeat_interval_seconds <= 0:
        p.append("controller.heartbeat_interval_seconds precisa ser > 0")
    if ct.suspect_timeout_seconds <= ct.heartbeat_interval_seconds:
        p.append(
            "controller.suspect_timeout_seconds precisa ser maior que heartbeat_interval_seconds "
            f"({ct.suspect_timeout_seconds} <= {ct.heartbeat_interval_seconds})"
        )
    if ct.dead_timeout_seconds <= ct.suspect_timeout_seconds:
        p.append(
            "controller.dead_timeout_seconds precisa ser maior que suspect_timeout_seconds "
            f"({ct.dead_timeout_seconds} <= {ct.suspect_timeout_seconds})"
        )
    if ct.max_concurrent_recoveries < 1:
        p.append("controller.max_concurrent_recoveries precisa ser >= 1")
    if ct.upload_lease_seconds <= 0:
        p.append("controller.upload_lease_seconds precisa ser > 0")
    if ct.upload_max_lifetime_seconds < ct.upload_lease_seconds:
        p.append("controller.upload_max_lifetime_seconds precisa ser >= upload_lease_seconds")
    states = tuple(s.upper() for s in ct.placement_eligible_node_states)
    unknown_states = [s for s in states if s not in NODE_STATES]
    if unknown_states:
        p.append(f"controller.placement_eligible_node_states tem estados desconhecidos: {unknown_states}")
    elif set(states) != {"ALIVE"}:
        # o contrato (secao 2) fixa que so ALIVE recebe blocos novos
        p.append("controller.placement_eligible_node_states precisa ser [\"ALIVE\"] (CONTRATO.md, secao 2)")

    _check_address(st.grpc_bind, "storage.grpc_bind", p)
    _check_address(st.central_target, "storage.central_target", p)
    if st.advertise_addr:
        _check_address(st.advertise_addr, "storage.advertise_addr", p)
    if not st.node_id_file.strip():
        p.append("storage.node_id_file nao pode ser vazio")
    if not st.block_directory.strip():
        p.append("storage.block_directory nao pode ser vazio")
    if st.capacity_bytes < 0:
        p.append("storage.capacity_bytes precisa ser >= 0 (0 usa o disco real)")
    if st.scrub_interval_seconds <= 0:
        p.append("storage.scrub_interval_seconds precisa ser > 0")

    _check_address(cli.central_target, "client.central_target", p)
    if cli.logical_chunk_size_bytes <= 0:
        p.append("client.logical_chunk_size_bytes precisa ser > 0")
    if cli.grpc_frame_size_bytes <= 0:
        p.append("client.grpc_frame_size_bytes precisa ser > 0")
    elif cli.grpc_frame_size_bytes > GRPC_MAX_MESSAGE_BYTES - GRPC_ENVELOPE_MARGIN_BYTES:
        p.append(
            "client.grpc_frame_size_bytes precisa ficar abaixo do limite de 4 MiB do grpc com folga "
            f"(maximo {GRPC_MAX_MESSAGE_BYTES - GRPC_ENVELOPE_MARGIN_BYTES}); use streaming em vez de aumentar o limite"
        )
    if 0 < cli.logical_chunk_size_bytes < cli.grpc_frame_size_bytes:
        p.append("client.grpc_frame_size_bytes nao pode ser maior que logical_chunk_size_bytes")
    if cli.lease_renew_interval_seconds <= 0:
        p.append("client.lease_renew_interval_seconds precisa ser > 0")
    elif cli.lease_renew_interval_seconds >= ct.upload_lease_seconds:
        p.append(
            "client.lease_renew_interval_seconds precisa ser menor que controller.upload_lease_seconds, "
            "senao o lease expira antes da renovacao"
        )
    if cli.upload_rpc_deadline_seconds <= 0:
        p.append("client.upload_rpc_deadline_seconds precisa ser > 0")
    if cli.upload_retry_max_attempts < 1:
        p.append("client.upload_retry_max_attempts precisa ser >= 1 (conta a primeira tentativa)")
    if cli.upload_retry_backoff_initial_seconds <= 0:
        p.append("client.upload_retry_backoff_initial_seconds precisa ser > 0")
    if cli.upload_retry_backoff_max_seconds < cli.upload_retry_backoff_initial_seconds:
        p.append("client.upload_retry_backoff_max_seconds precisa ser >= upload_retry_backoff_initial_seconds")
    if not 0 <= cli.upload_retry_backoff_jitter_fraction < 1:
        p.append("client.upload_retry_backoff_jitter_fraction precisa estar em [0, 1)")
    if cli.encryption_algorithm not in ENCRYPTION_ALGORITHMS:
        p.append(f"client.encryption_algorithm precisa ser um de {list(ENCRYPTION_ALGORITHMS)}")

    if not 0 <= obs.metrics_port <= 65535:
        p.append("observability.metrics_port precisa estar em 0..65535 (0 desliga)")
    if obs.log_format not in LOG_FORMATS:
        p.append(f"observability.log_format precisa ser um de {list(LOG_FORMATS)}")
    if obs.log_level.upper() not in LOG_LEVELS:
        p.append(f"observability.log_level precisa ser um de {list(LOG_LEVELS)}")

    return p


def parse_set_args(pairs: Iterable[str]) -> dict[str, str]:
    """transforma ["cluster.replication_factor=2", ...] no formato aceito por overrides."""
    result: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep:
            raise ConfigError([f"override invalido '{pair}': use secao.chave=valor"])
        result[key.strip()] = value
    return result


def add_config_arguments(parser: argparse.ArgumentParser) -> None:
    """adiciona --config e --set a um argparse, para todos os processos aceitarem o mesmo formato."""
    parser.add_argument("--config", help=f"arquivo yaml (padrao: variavel {CONFIG_PATH_ENV})")
    parser.add_argument(
        "--set",
        dest="config_overrides",
        action="append",
        default=[],
        metavar="SECAO.CHAVE=VALOR",
        help="sobrescreve uma chave; pode repetir",
    )


def load_from_args(args: argparse.Namespace) -> Config:
    """atalho para quem usou add_config_arguments."""
    return load_config(args.config, overrides=parse_set_args(args.config_overrides))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="valida e mostra a configuracao efetiva do dcomp drive")
    add_config_arguments(parser)
    args = parser.parse_args(argv)
    try:
        cfg = load_from_args(args)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 2
    print(json.dumps(cfg.as_dict(), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

