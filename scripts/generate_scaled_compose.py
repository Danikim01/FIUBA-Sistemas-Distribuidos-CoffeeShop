#!/usr/bin/env python3
"""Generate docker-compose.yml based on the worker scaling config."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, TypedDict

# Add src directory to path to import distribute_nodes
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from healthcheck.distribute_nodes import distribute_nodes, get_next_healthchecker, get_healthchecker_name

class WorkerDefinitionRequired(TypedDict):
    display_name: str
    base_service_name: str
    command: List[str]
    needs_worker_id: bool
    required_environment: List[str]
    scalable: bool


class WorkerDefinition(WorkerDefinitionRequired, total=False):
    extra_volumes: List[str]
    stateful: bool


@dataclass
class WorkerConfig:
    count: int
    environment: Dict[str, str]


WORKER_DEFINITIONS: Dict[str, WorkerDefinition] = {
    "year_filter_sharding_router": {
        "display_name": "Year Filter Sharding Router",
        "base_service_name": "year-filter-sharding-router",
        "command": ["python", "sharding/sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_EXCHANGE", "NUM_SHARDS"],
        "scalable": False,
    },
    "year_filter": {
        "display_name": "Year Filter Workers",
        "base_service_name": "year-filter-worker",
        "command": ["python", "filter/year.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE", "IS_SHARDED_WORKER"],
        "scalable": True,
    },
    "year_filter_eof_barrier": {
        "display_name": "Year Filter EOF Barrier",
        "base_service_name": "year-filter-eof-barrier",
        "command": ["python", "barrier/filter_eof_barrier.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_EXCHANGE", "REPLICA_COUNT"],
        "scalable": False,
    },
    "items_year_filter_sharding_router": {
        "display_name": "Items Year Filter Sharding Router",
        "base_service_name": "items-year-filter-sharding-router",
        "command": ["python", "sharding/items_sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_EXCHANGE", "NUM_SHARDS"],
        "scalable": False,
    },
    "items_year_filter": {
        "display_name": "Items Year Filter Workers",
        "base_service_name": "items-year-filter-worker",
        "command": ["python", "filter/year.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE", "IS_SHARDED_WORKER"],
        "scalable": True,
    },
    "items_year_filter_eof_barrier": {
        "display_name": "Items Year Filter EOF Barrier",
        "base_service_name": "items-year-filter-eof-barrier",
        "command": ["python", "barrier/filter_eof_barrier.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE", "REPLICA_COUNT"],
        "scalable": False,
    },
    "time_filter_sharding_router": {
        "display_name": "Time Filter Sharding Router",
        "base_service_name": "time-filter-sharding-router",
        "command": ["python", "sharding/sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_EXCHANGE", "NUM_SHARDS"],
        "scalable": False,
    },
    "time_filter": {
        "display_name": "Time Filter Workers",
        "base_service_name": "time-filter-worker",
        "command": ["python", "filter/time.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE", "IS_SHARDED_WORKER"],
        "scalable": True,
    },
    "time_filter_eof_barrier": {
        "display_name": "Time Filter EOF Barrier",
        "base_service_name": "time-filter-eof-barrier",
        "command": ["python", "barrier/filter_eof_barrier.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_EXCHANGE", "REPLICA_COUNT"],
        "scalable": False,
    },
    "amount_filter_sharding_router": {
        "display_name": "Amount Filter Sharding Router",
        "base_service_name": "amount-filter-sharding-router",
        "command": ["python", "sharding/sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_EXCHANGE", "NUM_SHARDS"],
        "scalable": False,
    },
    "amount_filter": {
        "display_name": "Amount Filter Workers",
        "base_service_name": "amount-filter-worker",
        "command": ["python", "filter/amount.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE", "IS_SHARDED_WORKER"],
        "scalable": True,
    },
    "amount_filter_eof_barrier": {
        "display_name": "Amount Filter EOF Barrier",
        "base_service_name": "amount-filter-eof-barrier",
        "command": ["python", "barrier/filter_eof_barrier.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE", "REPLICA_COUNT"],
        "scalable": False,
    },
    "tpv": {
        "display_name": "TPV Workers",
        "base_service_name": "tpv-worker",
        "command": ["python", "local_top_scaling/tpv.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "tpv_sharding_router": {
        "display_name": "TPV Sharding Router",
        "base_service_name": "tpv-sharding-router",
        "command": ["python", "sharding/tpv_sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_EXCHANGE"],
        "scalable": False,
    },
    "tpv_sharded": {
        "display_name": "TPV Sharded Workers",
        "base_service_name": "tpv-worker-sharded",
        "command": ["python", "local_top_scaling/tpv_sharded.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
        "extra_volumes": ["./logs:/app/logs"],
        "stateful": True,
    },
    "tpv_aggregator": {
        "display_name": "TPV Aggregator",
        "base_service_name": "tpv-aggregator",
        "command": ["python", "final_top_aggregators/final_tvp.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
        "stateful": True,
    },
    "items_top": {
        "display_name": "Top Items Workers",
        "base_service_name": "items-top-worker",
        "command": ["python", "local_top_scaling/items.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
    },
    "items_sharding_router": {
        "display_name": "Items Sharding Router",
        "base_service_name": "items-sharding-router",
        "command": ["python", "sharding/items_sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_EXCHANGE"],
        "scalable": False,
    },
    "items_sharded": {
        "display_name": "Items Sharded Workers",
        "base_service_name": "items-worker-sharded",
        "command": ["python", "local_top_scaling/items_sharded.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
        "stateful": True,
    },
    "items_aggregator": {
        "display_name": "Top Items Aggregator",
        "base_service_name": "items-aggregator",
        "command": ["python", "final_top_aggregators/final_items.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
        "stateful": True,
    },
    "top_clients_sharding_router": {
        "display_name": "Top Clients Sharding Router",
        "base_service_name": "top-clients-sharding-router",
        "command": ["python", "sharding/sharding_router.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_EXCHANGE"],
        "scalable": False,
    },
    "top_clients": {
        "display_name": "Top Clients Workers",
        "base_service_name": "top-clients-worker-sharded",
        "command": ["python", "local_top_scaling/users_sharded.py"],
        "needs_worker_id": True,
        "required_environment": ["INPUT_EXCHANGE", "INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": True,
        "stateful": True,
    },
    "top_clients_birthdays": {
        "display_name": "Top Clients Birthdays Aggregator",
        "base_service_name": "top-clients-birthdays-aggregator",
        "command": ["python", "final_top_aggregators/final_users.py"],
        "needs_worker_id": False,
        "required_environment": ["INPUT_QUEUE", "OUTPUT_QUEUE"],
        "scalable": False,
        "stateful": True,
    },
}


SHARDED_WORKER_KEYS = {
    "tpv_sharded",
    "items_sharded",
    "top_clients",
    "year_filter",
    "items_year_filter",
    "time_filter",
    "amount_filter",
}


ROUTER_TO_SHARDED: Dict[str, str] = {
    "tpv_sharding_router": "tpv_sharded",
    "items_sharding_router": "items_sharded",
    "top_clients_sharding_router": "top_clients",
    "year_filter_sharding_router": "year_filter",
    "items_year_filter_sharding_router": "items_year_filter",
    "time_filter_sharding_router": "time_filter",
    "amount_filter_sharding_router": "amount_filter",
}

# Map aggregators to their corresponding sharded workers
# This is used to set REPLICA_COUNT for aggregators to match the number of sharded workers
AGGREGATOR_TO_SHARDED: Dict[str, str] = {
    "tpv_aggregator": "tpv_sharded",
    "items_aggregator": "items_sharded",
    "top_clients_birthdays": "top_clients",
    "year_filter_eof_barrier": "year_filter",
    "items_year_filter_eof_barrier": "items_year_filter",
    "time_filter_eof_barrier": "time_filter",
    "amount_filter_eof_barrier": "amount_filter",
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
        # Skip top_clients - it's always 4 shards regardless of scale parameter
        if worker_key == "top_clients":
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
    client_cfg = ensure_mapping(service_env_map.get("client"), "Service 'client' configuration") or {}
    client_env = normalize_environment(client_cfg, "Service 'client' environment")
    client_count = ensure_int(client_cfg.get("count", 1), "Client count", allow_zero=False)

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
        # Order gateway environment variables to match original docker-compose.yml
        gateway_order = [
            "RABBITMQ_HOST",
            "RABBITMQ_PORT",
            "TRANSACTIONS_EXCHANGE",
            "RESULTS_QUEUE",
            "STORES_EXCHANGE",
            "USERS_QUEUE",
            "TRANSACTION_ITEMS_EXCHANGE",
            "MENU_ITEMS_QUEUE",
            "CHUNK_SIZE",
            "FILTER_REPLICA_COUNT",
        ]
        lines.extend(format_environment(gateway_env, indent="      ", order=gateway_order))
    lines.append("    restart: unless-stopped")
    lines.append("")

    # Generate multiple client services
    plural = "instancias" if client_count != 1 else "instancia"
    lines.append(f"  # Client Service ({client_count} {plural})")
    
    for client_index in range(1, client_count + 1):
        service_name = f"client-{client_index}"
        container_name = f"coffee-client-{client_index}"
        
        lines.extend([
            f"  {service_name}:",
            "    build:",
            "      context: ./src/client",
            "      dockerfile: Dockerfile",
            f"    container_name: {container_name}",
            "    networks:",
            "      - middleware-network",
            "    depends_on:",
            "      - gateway",
            "      - rabbitmq",
        ])
        
        if client_env:
            lines.append("    environment:")
            lines.extend(format_environment(client_env, indent="      "))
        
        lines.extend([
            "    volumes:",
            "      - ./src/client/.data:/app/.data:ro",
            "      - ./results:/app/.results",
            "",
        ])

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
        if meta["scalable"]:
            # Force top_clients to always have 4 shards
            if name == "top_clients":
                count = 4
            elif count_value is None:
                count = 1
            else:
                count = ensure_int(count_value, f"Worker '{name}' count", allow_zero=False)
        else:
            count = 1

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


def format_environment(environment: Dict[str, str], indent: str = "      ", order: Optional[List[str]] = None) -> Iterable[str]:
    """
    Format environment variables, optionally in a specific order.
    If order is provided, variables in order come first, then the rest.
    """
    if order:
        # First, yield variables in the specified order
        for key in order:
            if key in environment:
                yield f"{indent}- {key}={environment[key]}"
        # Then yield the rest
        for key, value in environment.items():
            if key not in order:
                yield f"{indent}- {key}={value}"
    else:
        for key, value in environment.items():
            yield f"{indent}- {key}={value}"


def format_command(command: List[str]) -> str:
    quoted = ", ".join(f'\"{part}\"' for part in command)
    return f"[{quoted}]"


def build_service_name(base_service: str, index: int, total_count: int) -> str:
    if total_count == 1:
        return base_service
    # For sharded workers, use 0-based indexing (0, 1, 2, ...)
    if "sharded" in base_service:
        return f"{base_service}-{index - 1}"
    return f"{base_service}-{index}"


def generate_worker_sections(
    workers: Dict[str, WorkerConfig],
    common_env: Dict[str, str],
    global_prefetch: Optional[str],
    is_reduced_dataset: bool = False,
) -> List[str]:
    sections: List[str] = []

    sharded_counts = {
        key: workers[key].count
        for key in SHARDED_WORKER_KEYS
        if key in workers
    }

    for key in WORKER_DEFINITIONS:
        worker_cfg = workers.get(key)
        if worker_cfg is None or worker_cfg.count <= 0:
            continue

        meta = WORKER_DEFINITIONS[key]
        
        # Skip old workers if sharded versions exist
        if key == "tpv" and "tpv_sharded" in workers:
            continue
        # Always skip items_top when items_sharded exists (always use sharding)
        if key == "items_top" and "items_sharded" in workers:
            continue
        
        # Skip sharding routers for year_filter and items_year_filter because gateway does the sharding
        if key in ["year_filter_sharding_router", "items_year_filter_sharding_router"]:
            continue
        
        # Add comment for workers (skip comment for time and amount sharding routers to match original)
        if "sharding_router" in key and key not in ["time_filter_sharding_router", "amount_filter_sharding_router"]:
            sections.append(f"  # {meta['display_name']} (1 instancia)")
        elif "sharding_router" not in key:
            # Note: top_clients is already the sharded version, so we don't skip it
            total_count = worker_cfg.count
            plural = "instancias" if total_count != 1 else "instancia"
            sections.append(f"  # {meta['display_name']} ({total_count} {plural})")
        
        # Note: total_count is needed for the loop below
        if "sharding_router" in key:
            total_count = 1
        else:
            total_count = worker_cfg.count

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

            # Special handling for sharding router
            if "sharding_router" in key:
                # TPV sharding router uses BATCH_SIZE=5000, others use 5000
                environment.setdefault("BATCH_SIZE", "5000")
                # Add BATCH_TIMEOUT for TPV, items, and top_clients sharding routers
                if key == "tpv_sharding_router":
                    environment.setdefault("BATCH_TIMEOUT", "1.0")
                elif key == "items_sharding_router":
                    environment.setdefault("BATCH_TIMEOUT", "1.0")
                elif key == "top_clients_sharding_router":
                    environment.setdefault("BATCH_TIMEOUT", "1.0")
                # For time-filter-sharding-router, use REPLICA_COUNT=3 (matching sharded workers)
                # For amount-filter-sharding-router, also use REPLICA_COUNT=3
                # For others, use REPLICA_COUNT=1
                if key == "time_filter_sharding_router":
                    environment["REPLICA_COUNT"] = "3"
                    # Fix INPUT_QUEUE name: original uses transactions_filtered_time_sharding_router
                    # but config has transactions_year_filtered_time_sharding_router
                    if "INPUT_QUEUE" in environment:
                        env_queue = environment["INPUT_QUEUE"]
                        if "transactions_year_filtered_time_sharding_router" in env_queue:
                            environment["INPUT_QUEUE"] = env_queue.replace(
                                "transactions_year_filtered_time_sharding_router",
                                "transactions_filtered_time_sharding_router"
                            )
                    # Remove PREFETCH_COUNT and WORKER_ID for time-filter-sharding-router
                    environment.pop("PREFETCH_COUNT", None)
                    environment.pop("WORKER_ID", None)
                elif key == "amount_filter_sharding_router":
                    environment["REPLICA_COUNT"] = "3"
                    # Remove PREFETCH_COUNT and WORKER_ID for amount-filter-sharding-router
                    environment.pop("PREFETCH_COUNT", None)
                    environment.pop("WORKER_ID", None)
                elif key == "items_sharding_router":
                    # items-sharding-router should have BATCH_TIMEOUT but not in the order we generate
                    # It will be included in the router_order
                    pass
                else:
                    environment["REPLICA_COUNT"] = "1"
                sharded_key = ROUTER_TO_SHARDED.get(key)
                if sharded_key is None or sharded_key not in sharded_counts:
                    raise SystemExit(
                        f"Unable to set NUM_SHARDS for '{key}': missing sharded worker configuration"
                    )
                environment["NUM_SHARDS"] = str(sharded_counts[sharded_key])
                # Only add WORKER_ID for certain routers (tpv, items, top_clients)
                # time and amount routers don't have WORKER_ID in original
                if key not in ["time_filter_sharding_router", "amount_filter_sharding_router"]:
                    environment["WORKER_ID"] = "0"
            
            # Special handling for aggregators that receive from sharded workers
            # They need REPLICA_COUNT equal to the number of sharded workers
            if key in AGGREGATOR_TO_SHARDED:
                sharded_key = AGGREGATOR_TO_SHARDED[key]
                if sharded_key not in sharded_counts:
                    raise SystemExit(
                        f"Unable to set REPLICA_COUNT for '{key}': missing sharded worker configuration for '{sharded_key}'"
                    )
                # Set REPLICA_COUNT to match the number of sharded workers
                # This ensures the aggregator waits for EOFs from all sharded workers
                environment["REPLICA_COUNT"] = str(sharded_counts[sharded_key])
            
            # Special handling for amount_filter_eof_barrier - enable batch deduplication
            # This prevents duplicate batches from amount-filter-sharding-router from being forwarded
            if key == "amount_filter_eof_barrier":
                environment["ENABLE_BATCH_DEDUPLICATION"] = "true"

            # Special handling for sharded workers - fix queue names and add sharded flag
            # Check both by base_service_name and by key to ensure all sharded workers are detected
            if "sharded" in meta["base_service_name"] or key in SHARDED_WORKER_KEYS:
                # Add IS_SHARDED_WORKER flag for sharded workers
                environment["IS_SHARDED_WORKER"] = "True"
                # Fix the INPUT_QUEUE to include the shard number
                if "INPUT_QUEUE" in environment:
                    base_queue = environment["INPUT_QUEUE"]
                    # For year_filter and items_year_filter, use pattern without "_shard"
                    # Original uses: transactions_year_filtered_0, not transactions_year_filtered_shard_0
                    if key in ["year_filter", "items_year_filter"]:
                        # Remove "_shard" suffix if present, then add just the number
                        base_queue_clean = re.sub(r'_shard$', '', base_queue)
                        base_queue_clean = re.sub(r'_\d+$', '', base_queue_clean)
                        environment["INPUT_QUEUE"] = f"{base_queue_clean}_{index - 1}"
                    else:
                        # For other sharded workers, remove any existing shard suffix
                        base_queue_clean = re.sub(r'_\d+$', '', base_queue)
                        # Add the correct shard number (0-based index)
                        environment["INPUT_QUEUE"] = f"{base_queue_clean}_{index - 1}"

            if key in SHARDED_WORKER_KEYS:
                environment["NUM_SHARDS"] = str(total_count)

            if meta["needs_worker_id"]:
                # For sharded workers, use 0-based indexing (0, 1, 2, ...)
                # For regular workers, use 1-based indexing (1, 2, 3, ...)
                if "sharded" in meta["base_service_name"] or key in SHARDED_WORKER_KEYS:
                    environment["WORKER_ID"] = str(index - 1)  # 0-based for sharded workers
                else:
                    environment["WORKER_ID"] = str(index)  # 1-based for regular workers
            elif "sharding_router" in key:
                # Sharding routers should use 0-based indexing to match sharded workers
                # But not for time and amount routers (they don't have WORKER_ID in original)
                # WORKER_ID is already set above for routers that need it (tpv, items, top_clients)
                if key not in ["time_filter_sharding_router", "amount_filter_sharding_router"]:
                    # WORKER_ID already set above, but ensure it's not duplicated
                    pass

            if environment:
                lines.append("    environment:")
                # Define order for common environment variables to match original docker-compose.yml
                # Different workers have different variable orders
                if "sharding_router" in key:
                    # For sharding routers: order depends on router type
                    if key == "tpv_sharding_router":
                        # TPV router: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_EXCHANGE, INPUT_QUEUE,
                        # OUTPUT_EXCHANGE, PREFETCH_COUNT, REPLICA_COUNT, BATCH_SIZE, BATCH_TIMEOUT, NUM_SHARDS, WORKER_ID
                        router_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_EXCHANGE",
                            "INPUT_QUEUE",
                            "OUTPUT_EXCHANGE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                            "BATCH_SIZE",
                            "BATCH_TIMEOUT",
                            "NUM_SHARDS",
                            "WORKER_ID",
                        ]
                    elif key == "items_sharding_router":
                        # Items router: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_QUEUE, OUTPUT_EXCHANGE,
                        # PREFETCH_COUNT, REPLICA_COUNT, BATCH_SIZE, BATCH_TIMEOUT, NUM_SHARDS, WORKER_ID
                        router_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_QUEUE",
                            "OUTPUT_EXCHANGE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                            "BATCH_SIZE",
                            "BATCH_TIMEOUT",
                            "NUM_SHARDS",
                            "WORKER_ID",
                        ]
                    elif key == "top_clients_sharding_router":
                        # Top clients router: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_EXCHANGE, INPUT_QUEUE,
                        # OUTPUT_EXCHANGE, PREFETCH_COUNT, REPLICA_COUNT, BATCH_SIZE, BATCH_TIMEOUT, NUM_SHARDS, WORKER_ID
                        router_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_EXCHANGE",
                            "INPUT_QUEUE",
                            "OUTPUT_EXCHANGE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                            "BATCH_SIZE",
                            "BATCH_TIMEOUT",
                            "NUM_SHARDS",
                            "WORKER_ID",
                        ]
                    else:
                        # Other routers (time, amount): RABBITMQ_HOST, RABBITMQ_PORT, INPUT_EXCHANGE, INPUT_QUEUE, 
                        # OUTPUT_EXCHANGE, BATCH_SIZE, NUM_SHARDS, REPLICA_COUNT
                        router_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_EXCHANGE",
                            "INPUT_QUEUE",
                            "OUTPUT_EXCHANGE",
                            "BATCH_SIZE",
                            "NUM_SHARDS",
                            "REPLICA_COUNT",
                        ]
                    lines.extend(format_environment(environment, indent="      ", order=router_order))
                elif key in AGGREGATOR_TO_SHARDED:
                    # For aggregators: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_QUEUE, OUTPUT_EXCHANGE/OUTPUT_QUEUE,
                    # REPLICA_COUNT, PREFETCH_COUNT (order depends on aggregator type)
                    if key == "tpv_aggregator":
                        # TPV aggregator: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_QUEUE, OUTPUT_QUEUE,
                        # PREFETCH_COUNT, REPLICA_COUNT
                        aggregator_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_QUEUE",
                            "OUTPUT_QUEUE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                        ]
                    elif key == "items_aggregator":
                        # Items aggregator: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_QUEUE, OUTPUT_QUEUE,
                        # PREFETCH_COUNT, REPLICA_COUNT
                        aggregator_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_QUEUE",
                            "OUTPUT_QUEUE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                        ]
                    elif key == "top_clients_birthdays":
                        # Top clients birthdays aggregator: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_QUEUE, OUTPUT_QUEUE,
                        # PREFETCH_COUNT, REPLICA_COUNT
                        aggregator_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_QUEUE",
                            "OUTPUT_QUEUE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                        ]
                    else:
                        # Other aggregators: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_QUEUE, OUTPUT_EXCHANGE/OUTPUT_QUEUE,
                        # REPLICA_COUNT, PREFETCH_COUNT
                        aggregator_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_QUEUE",
                            "OUTPUT_EXCHANGE",
                            "OUTPUT_QUEUE",
                            "REPLICA_COUNT",
                            "PREFETCH_COUNT",
                        ]
                    lines.extend(format_environment(environment, indent="      ", order=aggregator_order))
                else:
                    # For regular workers: order depends on worker type
                    if key in ["tpv_sharded", "items_sharded", "top_clients"]:
                        # For sharded stateful workers: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_EXCHANGE, INPUT_QUEUE,
                        # OUTPUT_QUEUE, PREFETCH_COUNT, REPLICA_COUNT, IS_SHARDED_WORKER, NUM_SHARDS, WORKER_ID
                        worker_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_EXCHANGE",
                            "INPUT_QUEUE",
                            "OUTPUT_QUEUE",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                            "IS_SHARDED_WORKER",
                            "NUM_SHARDS",
                            "WORKER_ID",
                        ]
                    else:
                        # For other workers: RABBITMQ_HOST, RABBITMQ_PORT, INPUT_EXCHANGE, INPUT_QUEUE,
                        # OUTPUT_QUEUE, IS_SHARDED_WORKER, PREFETCH_COUNT, REPLICA_COUNT, NUM_SHARDS, WORKER_ID
                        worker_order = [
                            "RABBITMQ_HOST",
                            "RABBITMQ_PORT",
                            "INPUT_EXCHANGE",
                            "INPUT_QUEUE",
                            "OUTPUT_EXCHANGE",
                            "OUTPUT_QUEUE",
                            "IS_SHARDED_WORKER",
                            "PREFETCH_COUNT",
                            "REPLICA_COUNT",
                            "NUM_SHARDS",
                            "WORKER_ID",
                            "BATCH_SIZE",
                            "BATCH_TIMEOUT",
                        ]
                    lines.extend(format_environment(environment, indent="      ", order=worker_order))

            lines.append(f"    command: {format_command(meta['command'])}")
            lines.append("    restart: unless-stopped")

            volume_mounts: List[str] = []
            extra_volumes = meta.get("extra_volumes")
            if extra_volumes:
                volume_mounts.extend(extra_volumes)
            if meta.get("stateful"):
                volume_mounts.append("./state:/app/state")

            if volume_mounts:
                lines.append("    volumes:")
                for mount in volume_mounts:
                    lines.append(f"      - {mount}")

            sections.append("\n".join(lines))

        sections.append("")

    return sections


def generate_healthchecker_section(
    workers: Dict[str, WorkerConfig],
    is_reduced_dataset: bool = False,
    healthcheck_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate the healthchecker service section with Ring topology.
    
    Args:
        workers: Dictionary of worker configurations
        is_reduced_dataset: Whether this is a reduced dataset configuration
        healthcheck_config: Optional healthchecker configuration from config file
        
    Returns:
        String with the healthchecker service section
    """
    # Default healthchecker configuration
    default_config = {
        "port": "9290",
        "interval_ms": "1000",
        "timeout_ms": "1500",
        "max_errors": "3",
        "count": 3, 
        "stop_grace_period": "10s",  
    }
    
    # Override with config file values if provided
    if healthcheck_config:
        for k, v in healthcheck_config.items():
            if k == "count":
                # Keep count as integer for validation
                default_config[k] = v
            else:
                # Convert other values to strings for environment variables
                default_config[k] = str(v)
    
    # Get number of healthcheckers (validate it's an integer)
    count_value = default_config.get("count", 3)
    total_healthcheckers = ensure_int(count_value, "healthchecker count", allow_zero=False)
    
    # Build list of all container names to monitor
    all_nodes = ["coffee-gateway"]  # Gateway is always included
    
    # Add all worker containers
    for key in WORKER_DEFINITIONS:
        worker_cfg = workers.get(key)
        if worker_cfg is None or worker_cfg.count <= 0:
            continue
        
        meta = WORKER_DEFINITIONS[key]
        
        # Skip old workers if sharded versions exist
        if key == "tpv" and "tpv_sharded" in workers:
            continue
        # Always skip items_top when items_sharded exists (always use sharding)
        if key == "items_top" and "items_sharded" in workers:
            continue
        
        # Skip sharding routers for year_filter and items_year_filter because gateway does the sharding
        if key in ["year_filter_sharding_router", "items_year_filter_sharding_router"]:
            continue
        
        total_count = worker_cfg.count
        
        for index in range(1, total_count + 1):
            service_name = build_service_name(meta["base_service_name"], index, total_count)
            all_nodes.append(service_name)
    
    # Generate healthchecker services
    sections = []
    plural = "instancias" if total_healthcheckers != 1 else "instancia"
    sections.append(f"  # Healthcheckers ({total_healthcheckers} {plural})")
    
    for hc_id in range(1, total_healthcheckers + 1):
        # Distribute nodes to this healthchecker using consistent hashing
        assigned_nodes = distribute_nodes(all_nodes, hc_id, total_healthcheckers)
        nodes_to_check_str = " ".join(assigned_nodes)
        
        lines = [
            f"  healthchecker-{hc_id}:",
            "    build:",
            "      context: .",
            "      dockerfile: ./src/healthcheck/Dockerfile",
            f"    container_name: healthchecker-{hc_id}",
            "    networks:",
            "      - middleware-network",
            "    depends_on:",
            "      rabbitmq:",
            "        condition: service_healthy",
            "    environment:",
            f"      - HEALTHCHECKER_ID={hc_id}",
            f"      - TOTAL_HEALTHCHECKERS={total_healthcheckers}",
            f"      - HEALTHCHECK_PORT={default_config['port']}",
            f"      - HEALTHCHECK_INTERVAL_MS={default_config['interval_ms']}",
            f"      - HEALTHCHECK_TIMEOUT_MS={default_config['timeout_ms']}",
            f"      - HEALTHCHECK_MAX_ERRORS={default_config['max_errors']}",
            f"      - NODES_TO_CHECK={nodes_to_check_str}",
            "    volumes:",
            "      - /var/run/docker.sock:/var/run/docker.sock",
            f"    stop_grace_period: {default_config['stop_grace_period']}",
            "    restart: unless-stopped",
        ]
        
        if hc_id < total_healthcheckers:
            lines.append("")
        
        sections.append("\n".join(lines))
    
    sections.append("")
    
    return "\n".join(sections)


def generate_compose(config: Dict[str, Any]) -> str:
    raw_workers = config.get("workers")
    if not isinstance(raw_workers, Mapping):
        raise SystemExit("Config file must contain a 'workers' object")

    common_env_raw = config.get("common_environment", {})
    common_env, global_prefetch = parse_common_environment(common_env_raw)

    # Check if this is a reduced dataset (only January data)
    service_env = config.get("service_environment", {})
    client_config = service_env.get("client", {})
    is_reduced_dataset = client_config.get("REDUCED", False)

    worker_settings = load_worker_settings(raw_workers)
    worker_sections = generate_worker_sections(worker_settings, common_env, global_prefetch, is_reduced_dataset)

    base_services = render_base_services(config.get("service_environment"), common_env)

    # Get healthchecker configuration from config file
    healthcheck_config = config.get("healthchecker")

    compose_parts = [base_services.rstrip()]
    if worker_sections:
        compose_parts.append("")
        compose_parts.append("\n".join(worker_sections).rstrip())
        compose_parts.append("")
    
    # Add healthchecker section
    healthchecker_section = generate_healthchecker_section(
        worker_settings,
        is_reduced_dataset,
        healthcheck_config
    )
    compose_parts.append(healthchecker_section.rstrip())
    
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
