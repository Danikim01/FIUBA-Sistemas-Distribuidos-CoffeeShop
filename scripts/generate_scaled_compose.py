#!/usr/bin/env python3
"""Generate docker-compose.yml based on the worker scaling config."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, TypedDict


class WorkerDefinition(TypedDict):
    display_name: str
    base_service_name: str
    command: List[str]
    needs_worker_id: bool
    required_environment: List[str]
    scalable: bool


@dataclass
class WorkerConfig:
    count: int
    environment: Dict[str, str]


WORKER_DEFINITIONS: Dict[str, WorkerDefinition] = {
    "year_filter": {
        "display_name": "Year Filter Workers",
        "base_service_name": "year-filter-worker",
        "command": ["python", "filter/year.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_EXCHANGE"],
        "scalable": True,
    },
    "items_year_filter": {
        "display_name": "Items Year Filter Workers",
        "base_service_name": "items-year-filter-worker",
        "command": ["python", "filter/year.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "time_filter": {
        "display_name": "Time Filter Workers",
        "base_service_name": "time-filter-worker",
        "command": ["python", "filter/time.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "OUTPUT_EXCHANGE"],
        "scalable": True,
    },
    "amount_filter": {
        "display_name": "Amount Filter Workers",
        "base_service_name": "amount-filter-worker",
        "command": ["python", "filter/amount.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "tpv": {
        "display_name": "TPV Workers",
        "base_service_name": "tpv-worker",
        "command": ["python", "top/tpv.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
    "tpv_aggregator": {
        "display_name": "TPV Aggregator",
        "base_service_name": "tpv-aggregator",
        "command": ["python", "aggregator/final_tvp.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
    "items_top": {
        "display_name": "Top Items Workers",
        "base_service_name": "items-top-worker",
        "command": ["python", "top/items.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
    "items_aggregator": {
        "display_name": "Top Items Aggregator",
        "base_service_name": "items-aggregator",
        "command": ["python", "aggregator/final_items.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
    "top_clients": {
        "display_name": "Top Clients Workers",
        "base_service_name": "top-clients-worker",
        "command": ["python", "top/clients.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_EXCHANGE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
    "top_clients_birthdays": {
        "display_name": "Top Clients Birthdays Aggregator",
        "base_service_name": "top-clients-birthdays-aggregator",
        "command": ["python", "aggregator/birthdays.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
}


FOOTER = """networks:\n  middleware-network:\n    driver: bridge\n\nvolumes:\n  rabbitmq_data:\n"""


def ensure_mapping(value: object, context: str) -> Optional[Mapping[str, Any]]:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value
    raise SystemExit(f"{context} must be an object if provided")


def ensure_int(value: object, context: str, allow_zero: bool = True) -> int:
    if not isinstance(value, int):
        raise SystemExit(f"{context} must be an integer (got {value!r})")
    if value < 0 or (value == 0 and not allow_zero):
        comparator = "> 0" if not allow_zero else ">= 0"
        raise SystemExit(f"{context} must be {comparator} (got {value})")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("workers_config.json"),
        help="Path to the JSON file with worker scaling configuration",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docker-compose.yml"),
        help="Path where the generated docker-compose file will be written",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=None,
        help="Override every scalable worker count with this value",
    )
    return parser.parse_args()


def read_config(path: Path) -> Dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"Config file not found: {path}") from exc

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc


def apply_uniform_scale(config: Dict[str, Any], raw_scale: Optional[int]) -> None:
    if raw_scale is None:
        return

    scale_value = ensure_int(raw_scale, "'--scale' value", allow_zero=False)

    workers_section = config.get("workers")
    if not isinstance(workers_section, dict):
        raise SystemExit("Config file must contain a 'workers' object to apply scaling")

    for worker_key, worker_cfg in workers_section.items():
        meta = WORKER_DEFINITIONS.get(worker_key)
        if meta is None or not meta["scalable"]:
            continue
        if not isinstance(worker_cfg, dict):
            raise SystemExit(
                f"Worker '{worker_key}' configuration must be an object to apply scaling"
            )
        worker_cfg["count"] = scale_value


def normalize_environment(overrides: Optional[Mapping[str, Any]], context: str) -> Dict[str, str]:
    mapping = ensure_mapping(overrides, context)
    if not mapping:
        return {}
    return {key: str(value) for key, value in mapping.items()}


def render_base_services(service_env_cfg: object, base_env: Mapping[str, str]) -> str:
    service_env_map = ensure_mapping(service_env_cfg, "'service_environment'") or {}

    gateway_env = dict(base_env)
    gateway_env.update(
        normalize_environment(
            service_env_map.get("gateway"),
            "Service 'gateway' environment",
        )
    )
    client_env = normalize_environment(
        service_env_map.get("client"),
        "Service 'client' environment",
    )

    lines = ["services:"]

    lines.extend(
        [
            "  # RabbitMQ Server",
            "  rabbitmq:",
            "    image: rabbitmq:3.12-management",
            "    container_name: rabbitmq-server",
            "    ports:",
            "      - \"5672:5672\"",
            "      - \"15672:15672\"",
            "    environment:",
            "      RABBITMQ_DEFAULT_USER: guest",
            "      RABBITMQ_DEFAULT_PASS: guest",
            "    networks:",
            "      - middleware-network",
            "    healthcheck:",
            "      test: [\"CMD\", \"rabbitmq-diagnostics\", \"ping\"]",
            "      interval: 30s",
            "      timeout: 10s",
            "      retries: 5",
            "    volumes:",
            "      - rabbitmq_data:/var/lib/rabbitmq",
            "",
        ]
    )

    lines.extend(
        [
            "  # Gateway Service",
            "  gateway:",
            "    build:",
            "      context: .",
            "      dockerfile: ./src/gateway/Dockerfile",
            "    container_name: coffee-gateway",
            "    ports:",
            "      - \"12345:12345\"",
            "    networks:",
            "      - middleware-network",
            "    depends_on:",
            "      rabbitmq:",
            "        condition: service_healthy",
        ]
    )
    if gateway_env:
        lines.append("    environment:")
        lines.extend(format_environment(gateway_env, indent="      "))
    lines.append("    restart: unless-stopped")
    lines.append("")

    lines.extend(
        [
            "  # Client Service",
            "  client:",
            "    build:",
            "      context: ./src/client",
            "      dockerfile: Dockerfile",
            "    container_name: coffee-client",
            "    networks:",
            "      - middleware-network",
            "    depends_on:",
            "      - gateway",
            "      - rabbitmq",
        ]
    )
    if client_env:
        lines.append("    environment:")
        lines.extend(format_environment(client_env, indent="      "))
    lines.extend(
        [
            "    volumes:",
            "      - ./src/client/.data:/app/.data",
            "      - ./results:/app/.results",
            "",
        ]
    )

    return "\n".join(lines)


def parse_common_environment(raw_env: object) -> Tuple[Dict[str, str], Optional[str]]:
    mapping = ensure_mapping(raw_env, "'common_environment'") or {}
    env: Dict[str, str] = {}
    prefetch: Optional[str] = None

    for key, value in mapping.items():
        if key == "prefetch_count":
            prefetch_value = ensure_int(
                value,
                "'common_environment.prefetch_count'",
                allow_zero=False,
            )
            prefetch = str(prefetch_value)
        else:
            env[key] = str(value)

    if not env:
        env = {"RABBITMQ_HOST": "rabbitmq", "RABBITMQ_PORT": "5672"}

    return env, prefetch


def load_worker_settings(raw_workers: Mapping[str, Any]) -> Dict[str, WorkerConfig]:
    unknown = set(raw_workers) - set(WORKER_DEFINITIONS)
    if unknown:
        unknown_list = ", ".join(sorted(unknown))
        raise SystemExit(f"Unknown workers in configuration: {unknown_list}")

    settings: Dict[str, WorkerConfig] = {}
    for name, meta in WORKER_DEFINITIONS.items():
        worker_raw = raw_workers.get(name)
        if worker_raw is None:
            raise SystemExit(f"Missing configuration for worker '{name}'")

        worker_cfg = ensure_mapping(worker_raw, f"Worker '{name}' configuration")
        if worker_cfg is None:
            raise SystemExit(f"Worker '{name}' configuration must be an object")

        count_value = worker_cfg.get("count")
        if count_value is None:
            raise SystemExit(f"Worker '{name}' configuration is missing 'count'")
        count = ensure_int(count_value, f"Worker '{name}' count")

        env_cfg = ensure_mapping(worker_cfg.get("environment"), f"Worker '{name}' environment")
        environment = {key: str(value) for key, value in (env_cfg or {}).items()}

        missing = [key for key in meta["required_environment"] if key not in environment]
        if missing:
            missing_str = ", ".join(missing)
            raise SystemExit(
                f"Worker '{name}' is missing required environment keys: {missing_str}"
            )

        settings[name] = WorkerConfig(count=count, environment=environment)

    return settings


def format_environment(environment: Dict[str, str], indent: str = "      ") -> Iterable[str]:
    for key, value in environment.items():
        yield f"{indent}- {key}={value}"


def format_command(command: List[str]) -> str:
    quoted = ", ".join(f'\"{part}\"' for part in command)
    return f"[{quoted}]"


def build_service_name(base_service: str, index: int, total_count: int) -> str:
    if total_count == 1:
        return base_service
    return f"{base_service}-{index}"


def generate_worker_sections(
    workers: Dict[str, WorkerConfig],
    common_env: Dict[str, str],
    global_prefetch: Optional[str],
) -> List[str]:
    sections: List[str] = []

    for key in WORKER_DEFINITIONS:
        worker_cfg = workers.get(key)
        if worker_cfg is None or worker_cfg.count <= 0:
            continue

        meta = WORKER_DEFINITIONS[key]
        total_count = worker_cfg.count
        plural = "instancias" if total_count != 1 else "instancia"
        sections.append(f"  # {meta['display_name']} ({total_count} {plural})")

        for index in range(1, total_count + 1):
            service_name = build_service_name(meta["base_service_name"], index, total_count)

            lines = [f"  {service_name}:"]
            lines.append("    build:")
            lines.append("      context: .")
            lines.append("      dockerfile: ./src/workers/Dockerfile")
            lines.append(f"    container_name: {service_name}")
            lines.append("    networks:")
            lines.append("      - middleware-network")
            lines.append("    depends_on:")
            lines.append("      rabbitmq:")
            lines.append("        condition: service_healthy")

            environment = dict(common_env)
            environment.update(worker_cfg.environment)
            if global_prefetch is not None and "PREFETCH_COUNT" not in environment:
                environment["PREFETCH_COUNT"] = global_prefetch
            environment.setdefault("REPLICA_COUNT", str(total_count))
            if meta["needs_worker_id"]:
                environment["WORKER_ID"] = str(index)

            if environment:
                lines.append("    environment:")
                lines.extend(format_environment(environment))

            lines.append(f"    command: {format_command(meta['command'])}")
            lines.append("    restart: unless-stopped")

            sections.append("\n".join(lines))

        sections.append("")

    return sections


def generate_compose(config: Dict[str, Any]) -> str:
    raw_workers = config.get("workers")
    if not isinstance(raw_workers, Mapping):
        raise SystemExit("Config file must contain a 'workers' object")

    common_env_raw = config.get("common_environment", {})
    common_env, global_prefetch = parse_common_environment(common_env_raw)

    worker_settings = load_worker_settings(raw_workers)
    worker_sections = generate_worker_sections(worker_settings, common_env, global_prefetch)

    base_services = render_base_services(config.get("service_environment"), common_env)

    compose_parts = [base_services.rstrip()]
    if worker_sections:
        compose_parts.append("")
        compose_parts.append("\n".join(worker_sections).rstrip())
        compose_parts.append("")
    compose_parts.append(FOOTER.rstrip())
    compose_parts.append("")
    return "\n".join(compose_parts)


def write_output(path: Path, content: str) -> None:
    path.write_text(content + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    config = read_config(args.config)
    apply_uniform_scale(config, args.scale)
    content = generate_compose(config)
    write_output(args.output, content)
    print(f"Generated {args.output} from {args.config}")


if __name__ == "__main__":
    main()
