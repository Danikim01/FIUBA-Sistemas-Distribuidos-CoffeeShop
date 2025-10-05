"""Helper utilities for TPV aggregation normalization and sorting."""

from __future__ import annotations

from typing import Any, Dict, Tuple

YearHalf = Tuple[int, int]


def _parse_year(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _parse_half(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    text = str(value).strip().upper()
    if not text:
        return 0
    if text.startswith("H"):
        text = text[1:]
    if text.endswith("H"):
        text = text[:-1]
    return int(text) if text.isdigit() else 0


def normalize_year_half(
    raw_period: Any,
    *,
    year: Any | None = None,
    semester: Any | None = None,
) -> Tuple[str, int, int]:
    """Normalize year/half information and return display value plus numeric tuple."""

    normalized_year = 0
    normalized_half = 0

    if isinstance(raw_period, str) and raw_period:
        period = raw_period.strip()
        upper = period.upper()
        if "-H" in upper:
            year_part, half_part = upper.split("-H", 1)
            normalized_year = _parse_year(year_part)
            normalized_half = _parse_half(half_part)
        elif "-" in upper:
            year_part, half_part = upper.split("-", 1)
            normalized_year = _parse_year(year_part)
            normalized_half = _parse_half(half_part)
        else:
            normalized_year = _parse_year(upper)
            normalized_half = _parse_half(upper)

    if not normalized_year and year is not None:
        normalized_year = _parse_year(year)
    if not normalized_half and semester is not None:
        normalized_half = _parse_half(semester)

    if normalized_year and normalized_half:
        normalized_str = f"{normalized_year}-H{normalized_half}"
    elif isinstance(raw_period, str) and raw_period.strip():
        normalized_str = raw_period.strip()
    else:
        normalized_str = ""

    return normalized_str, normalized_year, normalized_half


def normalize_tpv_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize TPV entry fields in-place and return the entry."""

    period, year, half = normalize_year_half(
        entry.get("year_half_created_at"),
        year=entry.get("year"),
        semester=entry.get("semester"),
    )

    entry["year_half_created_at"] = period
    if year:
        entry["year"] = year
    else:
        entry.pop("year", None)

    if half:
        entry["semester"] = f"H{half}"
    else:
        entry.pop("semester", None)

    entry["store_name"] = (entry.get("store_name") or "").strip()
    entry["store_id"] = str(entry.get("store_id", "")).strip()

    try:
        entry["tpv"] = float(entry.get("tpv", 0.0) or 0.0)
    except (TypeError, ValueError):
        entry["tpv"] = 0.0

    return entry


def store_id_sort_key(value: Any) -> Tuple[int, str]:
    text = str(value or "").strip()
    if text.isdigit():
        return int(text), text
    try:
        numeric = int(float(text))
        return numeric, text
    except (ValueError, TypeError):
        return 0, text


def tpv_sort_key(entry: Dict[str, Any]) -> Tuple[YearHalf, str, Tuple[int, str]]:
    """Return a composite key to sort TPV rows by period, store name, then store id."""

    period_tuple = normalize_year_half(
        entry.get("year_half_created_at"),
        year=entry.get("year"),
        semester=entry.get("semester"),
    )[1:]

    store_name = entry.get("store_name") or ""
    store_key = store_id_sort_key(entry.get("store_id"))
    return period_tuple, store_name, store_key

