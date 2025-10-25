"""Normalize raw worker result payloads into client-friendly messages."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)

ResultMessage = Dict[str, Any]


def _ensure_type(value: Any) -> str:
    return str(value or "").upper()


def _normalize_quantity_profit_bundle(
    bundle: Dict[str, Any],
    bundle_types: Dict[str, str] | None = None,
) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    quantity_rows = bundle.get("quantity")
    if isinstance(quantity_rows, list):
        quantity_type = _ensure_type(
            (bundle_types or {}).get("quantity") or "TOP_ITEMS_BY_QUANTITY"
        )
        normalized.append(
            {
                "type": quantity_type,
                "results": quantity_rows,
            }
        )

    profit_rows = bundle.get("profit")
    if isinstance(profit_rows, list):
        profit_type = _ensure_type(
            (bundle_types or {}).get("profit") or "TOP_ITEMS_BY_PROFIT"
        )
        normalized.append(
            {
                "type": profit_type,
                "results": profit_rows,
            }
        )

    return normalized


def _normalize_typed_entries(rows: List[Any]) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    for entry in rows:
        if not isinstance(entry, dict):
            return []

        entry_type = _ensure_type(entry.get("type"))
        if not entry_type:
            return []

        results = entry.get("results")
        if results is None and isinstance(entry.get("data"), list):
            results = entry.get("data")

        if not isinstance(results, list):
            results = []

        normalized.append(
            {
                "type": entry_type,
                "results": results,
            }
        )

    return normalized


def _normalize_with_metadata(data: Any, metadata: Dict[str, Any]) -> List[ResultMessage]:
    if not isinstance(metadata, dict) or not metadata:
        return []

    normalized: List[ResultMessage] = []

    list_type = metadata.get("list_type")
    bundle_types = metadata.get("bundle_types")

    logger.info(f"[RESULT NORMALIZER] Bundle types: {bundle_types}")

    if list_type and isinstance(data, list):
        normalized.append(
            {
                "type": _ensure_type(list_type),
                "results": data,
            }
        )
        return normalized
    if bundle_types:
        candidates: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            candidates.append(data)
        elif isinstance(data, list):
            candidates.extend([item for item in data if isinstance(item, dict)])

        for candidate in candidates:
            normalized.extend(
                _normalize_quantity_profit_bundle(candidate, bundle_types)
            )

        if normalized:
            return normalized

    return normalized


def _normalize_data_list(
    rows: List[Any],
    metadata: Dict[str, Any] | None = None,
) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    if not rows:
        return normalized

    metadata = metadata or {}

    # First leverage metadata if available
    if metadata:
        meta_normalized = _normalize_with_metadata(rows, metadata)
        if meta_normalized:
            return meta_normalized

    # Handle entries that already expose type info
    typed_entries = _normalize_typed_entries(rows)
    if typed_entries:
        return typed_entries

    first_entry = rows[0]
    #check if metadata has bundle_types field
    if isinstance(first_entry, dict) and {"quantity", "profit"}.issubset(first_entry.keys()):
        return _normalize_quantity_profit_bundle(first_entry)

    if not all(isinstance(row, dict) for row in rows):
        return normalized

    sample_keys = {key for key in rows[0] if isinstance(key, str)}

    if "transaction_id" in sample_keys or "final_amount" in sample_keys:
        normalized.append(
            {
                "type": "AMOUNT_FILTER_TRANSACTIONS",
                "results": rows,
            }
        )
        return normalized

    if {"year_month_created_at", "sellings_qty"}.issubset(sample_keys):
        normalized.append(
            {
                "type": "TOP_ITEMS_BY_QUANTITY",
                "results": rows,
            }
        )
        return normalized

    if {"year_month_created_at", "profit_sum"}.issubset(sample_keys):
        normalized.append(
            {
                "type": "TOP_ITEMS_BY_PROFIT",
                "results": rows,
            }
        )
        return normalized

    if "tpv" in sample_keys or {"year", "semester", "store_name"}.intersection(sample_keys):
        normalized.append(
            {
                "type": "TPV_SUMMARY",
                "results": rows,
            }
        )
        return normalized

    if {"user_id", "purchases_qty"}.issubset(sample_keys) or "birthdate" in sample_keys:
        normalized.append(
            {
                "type": "TOP_CLIENTS_BIRTHDAYS",
                "results": rows,
            }
        )
        return normalized

    return normalized


def _normalize_data_dict(
    data: Dict[str, Any],
    metadata: Dict[str, Any] | None = None,
) -> List[ResultMessage]:
    normalized: List[ResultMessage] = []

    metadata = metadata or {}

    if metadata:
        if metadata.get("bundle_types"):
            logger.info(f"[RESULT NORMALIZER] Metadata with bundle types: {metadata}")
        meta_normalized = _normalize_with_metadata(data, metadata)
        if meta_normalized:
            return meta_normalized

    if {"quantity", "profit"}.issubset(data.keys()) or metadata.get("bundle_types"):
        return _normalize_quantity_profit_bundle(data)

    results_section = data.get("results")
    nested_type = str(data.get("type") or "").upper()

    if isinstance(results_section, list):
        if nested_type:
            normalized.append(
                {
                    "type": nested_type,
                    "results": results_section,
                }
            )
            return normalized

        normalized.extend(_normalize_data_list(results_section, metadata))
        return normalized

    return normalized


def _extract_candidate_lists(flattened: Dict[str, Any]) -> Iterable[List[Any]]:
    for value in flattened.values():
        if isinstance(value, list):
            yield value


def normalize_queue_message(message: Dict[str, Any]) -> List[ResultMessage]:
    """Normalize a raw queue message into one or more client payloads."""

    message_type = str(message.get("type") or "").upper()
    raw_metadata = message.get("type_metadata")
    if raw_metadata:
        logger.info(f"[RESULT NORMALIZER] Raw metadata: {raw_metadata}")

    if message_type == "EOF":
        return []

    normalized: List[ResultMessage] = []

    metadata: Dict[str, Any] = {}
    if isinstance(raw_metadata, dict):
        metadata = raw_metadata

    data_section = message.get("data")
    if isinstance(data_section, dict):
        normalized.extend(_normalize_data_dict(data_section, metadata))
    elif isinstance(data_section, list):
        normalized.extend(_normalize_data_list(data_section, metadata))

    if normalized:
        return normalized

    if "results" in message and isinstance(message["results"], list):
        rows = message["results"]
        entry_type = message_type or ""
        if not entry_type:
            extra = _normalize_data_list(rows, metadata)
            if extra:
                return extra
        normalized.append(
            {
                "type": entry_type or "UNKNOWN",
                "results": rows,
            }
        )
        return normalized

    flattened = {k: v for k, v in message.items() if k not in {"client_id", "type", "data"}}
    if isinstance(flattened, dict) and flattened:
        normalized.extend(_normalize_data_dict(flattened, metadata))
        if normalized:
            return normalized
        for candidate in _extract_candidate_lists(flattened):
            extra = _normalize_data_list(candidate, metadata)
            if extra:
                normalized.extend(extra)
        if normalized:
            return normalized

    if message_type:
        logger.debug("No specific normalization matched; using fallback for type %s", message_type)
        normalized.append(
            {
                "type": message_type,
                "results": [],
            }
        )

    return normalized
