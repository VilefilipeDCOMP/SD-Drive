from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ClientConfig:
    central_target: str = "controller:50051"
    logical_chunk_size_bytes: int = 4 * 1024 * 1024  # 4 MiB (4194304)
    grpc_frame_size_bytes: int = 256 * 1024          # 256 KiB (262144)
    lease_renew_interval_seconds: float = 180.0      # 3 minutos
    upload_rpc_deadline_seconds: float = 45.0
    upload_retry_max_attempts: int = 3
    upload_retry_backoff_initial_seconds: float = 1.0
    upload_retry_backoff_max_seconds: float = 4.0
    upload_retry_backoff_jitter_fraction: float = 0.2
    encryption_enabled: bool = False
    encryption_algorithm: str = "AES-256-GCM"
    client_events_path: str = "/logs/client.jsonl"


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "sim", "y", "t")
    return False


def load_client_config(config_path: str | Path | None = None) -> ClientConfig:
    #Carrega as configurações do cliente a partir de um arquivo YAML e sobrescreve por variáveis de ambiente
    cfg = ClientConfig()

    search_paths = []
    if config_path:
        search_paths.append(Path(config_path))
    else:
        env_cfg_path = os.getenv("DCOMP_CONFIG")
        if env_cfg_path:
            search_paths.append(Path(env_cfg_path))
        # Tenta localizar config.yaml ou config.example.yaml
        cwd = Path.cwd()
        search_paths.extend([
            cwd / "config.yaml",
            cwd / "config.example.yaml",
            cwd / "SD-Drive" / "config.yaml",
            cwd / "SD-Drive" / "config.example.yaml",
            cwd.parent / "config.yaml",
            cwd.parent / "config.example.yaml",
            Path("/app/config.yaml"),
        ])

    data: dict[str, Any] = {}
    for p in search_paths:
        if p.is_file():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    content = yaml.safe_load(f)
                    if isinstance(content, dict):
                        data = content
                        break
            except Exception:
                pass

    client_sec = data.get("client", {})
    if isinstance(client_sec, dict):
        if "central_target" in client_sec:
            cfg.central_target = str(client_sec["central_target"])
        if "logical_chunk_size_bytes" in client_sec:
            cfg.logical_chunk_size_bytes = int(client_sec["logical_chunk_size_bytes"])
        if "grpc_frame_size_bytes" in client_sec:
            cfg.grpc_frame_size_bytes = int(client_sec["grpc_frame_size_bytes"])
        if "lease_renew_interval_seconds" in client_sec:
            cfg.lease_renew_interval_seconds = float(client_sec["lease_renew_interval_seconds"])
        if "upload_rpc_deadline_seconds" in client_sec:
            cfg.upload_rpc_deadline_seconds = float(client_sec["upload_rpc_deadline_seconds"])
        if "upload_retry_max_attempts" in client_sec:
            cfg.upload_retry_max_attempts = int(client_sec["upload_retry_max_attempts"])
        if "upload_retry_backoff_initial_seconds" in client_sec:
            cfg.upload_retry_backoff_initial_seconds = float(client_sec["upload_retry_backoff_initial_seconds"])
        if "upload_retry_backoff_max_seconds" in client_sec:
            cfg.upload_retry_backoff_max_seconds = float(client_sec["upload_retry_backoff_max_seconds"])
        if "upload_retry_backoff_jitter_fraction" in client_sec:
            cfg.upload_retry_backoff_jitter_fraction = float(client_sec["upload_retry_backoff_jitter_fraction"])
        if "encryption_enabled" in client_sec:
            cfg.encryption_enabled = _parse_bool(client_sec["encryption_enabled"])
        if "encryption_algorithm" in client_sec:
            cfg.encryption_algorithm = str(client_sec["encryption_algorithm"])

    obs_sec = data.get("observability", {})
    if isinstance(obs_sec, dict):
        if "client_events_path" in obs_sec and obs_sec["client_events_path"]:
            cfg.client_events_path = str(obs_sec["client_events_path"])
        elif "log_file" in obs_sec and obs_sec["log_file"]:
            cfg.client_events_path = str(obs_sec["log_file"]).replace("{instance}", "client")

    env_central = (
        os.getenv("DCOMP_CLIENT__CENTRAL_TARGET")
        or os.getenv("DCOMP_CONTROLLER_TARGET")
        or os.getenv("DCOMP_CENTRAL_TARGET")
    )
    if env_central:
        cfg.central_target = env_central.strip()

    env_events = (
        os.getenv("DCOMP_CLIENT_EVENTS_PATH")
        or os.getenv("DCOMP_OBSERVABILITY__LOG_FILE")
    )
    if env_events:
        cfg.client_events_path = env_events.strip().replace("{instance}", "client")

    env_enc = (
        os.getenv("DCOMP_CLIENT__ENCRYPTION_ENABLED")
        if os.getenv("DCOMP_CLIENT__ENCRYPTION_ENABLED") is not None
        else os.getenv("DCOMP_ENCRYPTION_ENABLED")
    )
    if env_enc is not None:
        cfg.encryption_enabled = _parse_bool(env_enc)

    return cfg

