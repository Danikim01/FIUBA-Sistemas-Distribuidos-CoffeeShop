#!/usr/bin/env python3
"""Generate docker-compose.yml based on worker scaling config."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, TypedDict

# Static metadata for each worker type we know how to generate.


class WorkerDefinition(TypedDict):
    display_name: str
    base_service_name: str
    command: List[str]
    needs_worker_id: bool
    supports_prefetch: bool
    default_prefetch: Optional[int]
    default_environment: Dict[str, str]
    required_environment: List[str]
    scalable: bool


@dataclass
class WorkerGroup:
    count: int
    environment: Dict[str, str]
    name_suffix: Optional[str]
    prefetch_count: Optional[int]


WORKER_DEFINITIONS: Dict[str, WorkerDefinition] = {
    "year_filter": {
        "display_name": "Year Filter Workers",
        "base_service_name": "year-filter-worker",
        "command": ["python", "year_filter_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "INPUT_QUEUE": "transactions_raw",
            "OUTPUT_QUEUE": "transactions_year_filtered"
        },
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "time_filter": {
        "display_name": "Time Filter Workers",
        "base_service_name": "time-filter-worker",
        "command": ["python", "time_filter_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "INPUT_QUEUE": "transactions_enriched",
            "OUTPUT_QUEUE": "transactions_time_filtered",
        },
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "amount_filter": {
        "display_name": "Amount Filter Workers",
        "base_service_name": "amount-filter-worker",
        "command": ["python", "amount_filter_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "INPUT_QUEUE": "transactions_time_filtered",
            "OUTPUT_QUEUE": "transactions_final_results",
        },
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "transactions_projection": {
        "display_name": "Transactions Projection Workers",
        "base_service_name": "transactions-projection-worker",
        "command": ["python", "transactions_projection_worker.py"],
        "needs_worker_id": True,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "INPUT_QUEUE": "transactions_year_for_top_clients",
            "OUTPUT_QUEUE": "transactions_final_results",
        },
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "tpv": {
        "display_name": "TPV Aggregation Workers",
        "base_service_name": "tpv-worker",
        "command": ["python", "tpv_worker.py"],
        "needs_worker_id": False,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "INPUT_QUEUE": "transactions_time_filtered_tpv",
            "STORES_QUEUE": "stores_raw",
            "OUTPUT_QUEUE": "transactions_final_results",
        },
        "required_environment": ["INPUT_QUEUE", "STORES_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
    "items_top": {
        "display_name": "Top Items Workers",
        "base_service_name": "items-top-worker",
        "command": ["python", "items_top_worker.py"],
        "needs_worker_id": False,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "ITEMS_INPUT_QUEUE": "transaction_items_raw",
            "MENU_ITEMS_INPUT_QUEUE": "menu_items_raw",
            "OUTPUT_QUEUE": "transactions_final_results",
        },
        "required_environment": [
            "ITEMS_INPUT_QUEUE",
            "MENU_ITEMS_INPUT_QUEUE",
            "OUTPUT_QUEUE",
        ],
        "scalable": False,
    },
    "top_clients": {
        "display_name": "Top Clients Workers",
        "base_service_name": "top-clients-worker",
        "command": ["python", "top_clients_worker.py"],
        "needs_worker_id": False,
        "supports_prefetch": True,
        "default_prefetch": 20,
        "default_environment": {
            "TRANSACTIONS_QUEUE": "transactions_compact",
            "USERS_QUEUE": "users_raw",
            "STORES_QUEUE": "stores_for_top_clients",
            "OUTPUT_QUEUE": "transactions_final_results",
        },
        "required_environment": [
            "TRANSACTIONS_QUEUE",
            "USERS_QUEUE",
            "STORES_QUEUE",
            "OUTPUT_QUEUE",
        ],
        "scalable": False,
    },
    "results": {
        "display_name": "Results Workers",
        "base_service_name": "results-worker",
        "command": ["python", "results_worker.py"],
        "needs_worker_id": False,
        "supports_prefetch": False,
        "default_prefetch": None,
        "default_environment": {
            "INPUT_QUEUE": "transactions_final_results",
            "OUTPUT_QUEUE": "gateway_results",
        },
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
    },
}

SERVICE_ENV_DEFAULTS: Dict[str, Dict[str, str]] = {
    "gateway": {
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PORT": "5672",
        "OUTPUT_QUEUE": WORKER_DEFINITIONS["year_filter"]["default_environment"]["INPUT_QUEUE"],
        "STORES_QUEUE": "stores_raw",
        "STORES_QUEUES": "stores_raw,stores_for_top_clients",
        "USERS_QUEUE": WORKER_DEFINITIONS["top_clients"]["default_environment"]["USERS_QUEUE"],
        "TRANSACTION_ITEMS_QUEUE": WORKER_DEFINITIONS["items_top"]["default_environment"]["ITEMS_INPUT_QUEUE"],
        "MENU_ITEMS_QUEUE": WORKER_DEFINITIONS["items_top"]["default_environment"]["MENU_ITEMS_INPUT_QUEUE"],
        "RESULTS_QUEUE": "gateway_results",
    },
    "client": {
        "GATEWAY_HOST": "gateway",
        "GATEWAY_PORT": "12345",
    },
}


def ensure_mapping(value: object, context: str) -> Optional[Mapping[str, Any]]:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value
    raise SystemExit(f"{context} must be an object if provided")


def normalize_environment(
    defaults: Mapping[str, str],
    overrides: Optional[Mapping[str, Any]],
    context: str,
) -> Dict[str, str]:
    env = {key: str(value) for key, value in defaults.items()}
    if overrides is None:
        return env
    for key, value in overrides.items():
        env[key] = str(value)
    return env


def render_base_services(service_env_cfg: object) -> str:
    service_env_map = ensure_mapping(service_env_cfg, "'service_environment'") or {}

    gateway_overrides = ensure_mapping(
        service_env_map.get("gateway"),
        "Service 'gateway' environment",
    )
    client_overrides = ensure_mapping(
        service_env_map.get("client"),
        "Service 'client' environment",
    )

    gateway_env = normalize_environment(
        SERVICE_ENV_DEFAULTS["gateway"],
        gateway_overrides,
        "Service 'gateway' environment",
    )
    client_env = normalize_environment(
        SERVICE_ENV_DEFAULTS["client"],
        client_overrides,
        "Service 'client' environment",
    )

    lines = ["services:"]

    # RabbitMQ service definition
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

    # Gateway service definition
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

    # Client service definition
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
            "",
        ]
    )

    return "\n".join(lines)

FOOTER = """networks:\n  middleware-network:\n    driver: bridge\n\nvolumes:\n  rabbitmq_data:\n"""


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
        help="Override every worker group count with this value",
    )
    return parser.parse_args()


def read_config(path: Path) -> Dict[str, object]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"Config file not found: {path}") from exc

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc


def ensure_int(value: object, context: str, allow_zero: bool = True) -> int:
    if not isinstance(value, int):
        raise SystemExit(f"{context} must be an integer (got {value!r})")
    if value < 0 or (value == 0 and not allow_zero):
        comparator = "> 0" if not allow_zero else ">= 0"
        raise SystemExit(f"{context} must be {comparator} (got {value})")
    return value


def _override_group_count(
    worker_key: str,
    group_index: int,
    group_raw: object,
    target_count: int,
) -> Dict[str, object]:
    if not isinstance(group_raw, dict):
        raise SystemExit(
            f"Worker '{worker_key}' group #{group_index} must be an object to apply scaling"
        )

    updated_group = dict(group_raw)
    updated_group["count"] = target_count
    return updated_group


def apply_uniform_scale(config: Dict[str, object], raw_scale: Optional[int]) -> None:
    if raw_scale is None:
        return

    scale_value = ensure_int(raw_scale, "'--scale' value", allow_zero=False)

    workers_section = config.get("workers")
    if not isinstance(workers_section, dict):
        raise SystemExit("Config file must contain a 'workers' object to apply scaling")

    for worker_key, worker_cfg in list(workers_section.items()):
        meta = WORKER_DEFINITIONS.get(worker_key)
        if meta is not None and not meta.get("scalable", False):
            continue

        if isinstance(worker_cfg, int):
            workers_section[worker_key] = scale_value
            continue

        if isinstance(worker_cfg, list):
            workers_section[worker_key] = [
                _override_group_count(worker_key, index, group_cfg, scale_value)
                for index, group_cfg in enumerate(worker_cfg, start=1)
            ]
            continue

        if isinstance(worker_cfg, dict):
            updated_worker = dict(worker_cfg)

            # Update default count if present (used when no groups defined).
            if "count" in updated_worker and not isinstance(updated_worker["count"], (list, dict)):
                updated_worker["count"] = scale_value

            groups_cfg = updated_worker.get("groups")
            if groups_cfg is None:
                updated_worker["count"] = scale_value
            else:
                if not isinstance(groups_cfg, list):
                    raise SystemExit(
                        f"Worker '{worker_key}' groups configuration must be a list to apply scaling"
                    )
                updated_worker["groups"] = [
                    _override_group_count(worker_key, index, group_cfg, scale_value)
                    for index, group_cfg in enumerate(groups_cfg, start=1)
                ]

            workers_section[worker_key] = updated_worker
            continue

        raise SystemExit(
            f"Worker '{worker_key}' configuration type {type(worker_cfg).__name__} is not supported for scaling"
        )


def merge_worker_environment(
    worker_key: str,
    group_index: int,
    meta: WorkerDefinition,
    worker_level_env: Optional[Mapping[str, Any]],
    group_env: Optional[Mapping[str, Any]],
) -> Dict[str, str]:
    env = dict(meta["default_environment"])

    for source, source_name in (
        (worker_level_env, "worker-level"),
        (group_env, "group-level"),
    ):
        if source is None:
            continue
        for key, value in source.items():
            env[key] = str(value)

    required_keys = meta["required_environment"]
    missing = [key for key in required_keys if key not in env]
    if missing:
        missing_str = ", ".join(missing)
        raise SystemExit(
            f"Worker '{worker_key}' group #{group_index} missing required environment keys: {missing_str}"
        )

    return env


def normalize_worker_groups(
    worker_key: str,
    worker_raw: object,
    meta: WorkerDefinition,
) -> List[WorkerGroup]:
    # Handle the shorthand where the worker is configured as an integer count.
    if isinstance(worker_raw, int):
        count = ensure_int(worker_raw, f"Worker '{worker_key}' count")
        if count == 0:
            return []
        environment = merge_worker_environment(worker_key, 1, meta, None, None)
        prefetch_value = meta["default_prefetch"] if meta["supports_prefetch"] else None
        prefetch = (
            ensure_int(
                prefetch_value,
                f"Worker '{worker_key}' default prefetch",
                allow_zero=False,
            )
            if prefetch_value is not None
            else None
        )
        return [
            WorkerGroup(
                count=count,
                environment=environment,
                name_suffix=None,
                prefetch_count=prefetch,
            )
        ]

    if isinstance(worker_raw, list):
        worker_defaults: Dict[str, object] = {}
        groups_raw = worker_raw
    elif isinstance(worker_raw, dict):
        worker_defaults = dict(worker_raw)
        groups_raw = worker_defaults.pop("groups", [worker_defaults])
    else:
        raise SystemExit(
            f"Worker configuration for '{worker_key}' must be an integer, object, or list"
        )

    if not isinstance(groups_raw, list):
        raise SystemExit(
            f"Worker '{worker_key}' groups configuration must be a list of objects"
        )

    worker_level_env = ensure_mapping(
        worker_defaults.get("environment"),
        f"Worker '{worker_key}' environment",
    )
    worker_level_prefetch = worker_defaults.get("prefetch_count")
    worker_level_count = worker_defaults.get("count")
    worker_level_suffix = worker_defaults.get("name_suffix")

    groups: List[WorkerGroup] = []
    for index, group_raw in enumerate(groups_raw, start=1):
        if not isinstance(group_raw, dict):
            raise SystemExit(
                f"Worker '{worker_key}' group #{index} must be an object"
            )

        count_value = group_raw.get("count", worker_level_count)
        if count_value is None:
            raise SystemExit(
                f"Worker '{worker_key}' group #{index} is missing a 'count' value"
            )
        count = ensure_int(count_value, f"Worker '{worker_key}' group #{index} count")
        if count == 0:
            continue

        environment = merge_worker_environment(
            worker_key,
            index,
            meta,
            worker_level_env,
            ensure_mapping(
                group_raw.get("environment"),
                f"Worker '{worker_key}' group #{index} environment",
            ),
        )

        name_suffix = group_raw.get("name_suffix", worker_level_suffix)
        if name_suffix is not None and not isinstance(name_suffix, str):
            raise SystemExit(
                f"Worker '{worker_key}' group #{index} name_suffix must be a string"
            )

        group_prefetch: Optional[int]
        if meta["supports_prefetch"]:
            prefetch_value = group_raw.get("prefetch_count", worker_level_prefetch)
            if prefetch_value is None:
                prefetch_value = meta["default_prefetch"]
            group_prefetch = ensure_int(
                prefetch_value,
                f"Worker '{worker_key}' group #{index} prefetch_count",
                allow_zero=False,
            )
        else:
            group_prefetch = None

        groups.append(
            WorkerGroup(
                count=count,
                environment=environment,
                name_suffix=name_suffix,
                prefetch_count=group_prefetch,
            )
        )

    return groups


def load_worker_settings(raw_workers: Dict[str, object]) -> Dict[str, List[WorkerGroup]]:
    settings: Dict[str, List[WorkerGroup]] = {}
    for key, meta in WORKER_DEFINITIONS.items():
        worker_raw = raw_workers.get(key)
        if worker_raw is None:
            raise SystemExit(f"Missing configuration for worker '{key}'")

        groups = normalize_worker_groups(key, worker_raw, meta)
        if groups:
            settings[key] = groups
    return settings


def format_environment(environment: Dict[str, str], indent: str = "      ") -> Iterable[str]:
    for key, value in environment.items():
        yield f"{indent}- {key}={value}"


def format_command(command: List[str]) -> str:
    quoted = ", ".join(f'\"{part}\"' for part in command)
    return f"[{quoted}]"


def build_service_name(
    base_service: str,
    total_count: int,
    name_suffix: Optional[str],
    worker_id: int,
    instance_index: int,
) -> str:
    if total_count == 1 and not name_suffix:
        return base_service

    parts = [base_service]
    if name_suffix:
        parts.append(name_suffix)
        parts.append(str(instance_index))
    else:
        parts.append(str(worker_id))
    return "-".join(parts)


def generate_worker_sections(
    workers: Dict[str, List[WorkerGroup]],
    common_env: Dict[str, str],
) -> List[str]:
    sections: List[str] = []

    for key, groups in workers.items():
        if not groups:
            continue

        meta = WORKER_DEFINITIONS[key]
        display_name = meta["display_name"]
        base_service = meta["base_service_name"]

        total_count = sum(group.count for group in groups)
        if total_count == 0:
            continue

        plural = "instancias" if total_count != 1 else "instancia"
        sections.append(f"  # {display_name} ({total_count} {plural})")

        worker_id_counter = 0
        for group_index, group in enumerate(groups, start=1):
            if group.count == 0:
                continue

            for instance_index in range(1, group.count + 1):
                worker_id_counter += 1
                service_name = build_service_name(
                    base_service,
                    total_count,
                    group.name_suffix if isinstance(group.name_suffix, str) else None,
                    worker_id_counter,
                    instance_index,
                )

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

                environment = {k: str(v) for k, v in common_env.items()}
                environment.update(group.environment)

                if meta["needs_worker_id"]:
                    environment["WORKER_ID"] = str(worker_id_counter)

                if meta["supports_prefetch"] and group.prefetch_count is not None:
                    environment["PREFETCH_COUNT"] = str(group.prefetch_count)

                if environment:
                    lines.append("    environment:")
                    lines.extend(format_environment(environment))

                command = format_command(meta["command"])
                lines.append(f"    command: {command}")
                lines.append("    restart: unless-stopped")

                sections.append("\n".join(lines))

        sections.append("")  # Blank line between worker groups

    return sections


def generate_compose(config: Dict[str, object]) -> str:
    raw_workers = config.get("workers")
    if not isinstance(raw_workers, dict):
        raise SystemExit("Config file must contain a 'workers' object")

    common_env_raw = config.get("common_environment", {})
    if not isinstance(common_env_raw, dict):
        raise SystemExit("'common_environment' must be an object if provided")

    common_env = {k: str(v) for k, v in common_env_raw.items()}
    if not common_env:
        common_env = {"RABBITMQ_HOST": "rabbitmq", "RABBITMQ_PORT": "5672"}

    worker_settings = load_worker_settings(raw_workers)
    worker_sections = generate_worker_sections(worker_settings, common_env)

    base_services = render_base_services(config.get("service_environment"))

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
