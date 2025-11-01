import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from data_processor import get_reduced

logger = logging.getLogger(__name__)

ResultOutcome = Tuple[bool, str]

class ResultsValidator:
    """Validates analytics results against the source CSV data set."""

    _RESULT_TYPE_LABELS: Dict[str, str] = {
        "AMOUNT_FILTER_TRANSACTIONS": "Transacciones por monto y horario",
        "TOP_ITEMS_BY_QUANTITY": "Top ítems por cantidad",
        "TOP_ITEMS_BY_PROFIT": "Top ítems por ganancia",
        "TPV_SUMMARY": "TPV por sucursal y semestre",
        "TOP_CLIENTS_BIRTHDAYS": "Top clientes por sucursal",
    }

    def __init__(self, data_dir: str | Path | None) -> None:
        self.reduced = get_reduced()
        self.enabled = False
        self._summary: Dict[str, ResultOutcome] = {}
        self._row_cache: Dict[str, List[Dict[str, Any]]] = {}

        if not data_dir:
            logger.info("No data directory provided; result validation disabled.")
            return

        self._data_dir = Path(data_dir)
        if not self._data_dir.exists():
            logger.info("Data directory %s not found; result validation disabled.", self._data_dir)
            return

        self.enabled = True
        self._datasets: Dict[str, pd.DataFrame] = {}
        self._validators = {
            "AMOUNT_FILTER_TRANSACTIONS": self._validate_amount_filter_transactions,
            "TOP_ITEMS_BY_QUANTITY": self._validate_top_items_quantity,
            "TOP_ITEMS_BY_PROFIT": self._validate_top_items_profit,
            "TPV_SUMMARY": self._validate_tpv_summary,
            "TOP_CLIENTS_BIRTHDAYS": self._validate_top_clients_birthdays,
        }


    def reset(self) -> None:
        """Clear cached validation outcomes."""
        self._summary.clear()
        self._row_cache.clear()

    def validate(
        self,
        result_type: str,
        rows: Iterable[Dict[str, Any]],
        *,
        final: bool = True,
    ) -> ResultOutcome | None:
        """Validate a result payload against the expected dataset."""
        if not self.enabled:
            return None

        validator = self._validators.get(result_type)
        if validator is None:
            return None

        data = list(rows)

        if not final:
            if data:
                self._row_cache.setdefault(result_type, []).extend(data)
            return None

        cached = self._row_cache.pop(result_type, [])
        if cached:
            data = cached + data

        outcome = validator(data)
        self._summary[result_type] = outcome
        return outcome

    def get_label(self, result_type: str) -> str:
        """Return a human-readable label for the given result type."""
        return self._RESULT_TYPE_LABELS.get(result_type, result_type)

    def _load_dataset(self, key: str, folder: str, *, parse_dates: List[str] | None = None) -> pd.DataFrame:
        """Load and cache DataFrame for the given folder inside the data directory."""
        if key in self._datasets:
            return self._datasets[key].copy()

        directory = self._data_dir / folder
        frames: List[pd.DataFrame] = []
        if directory.exists():
            csv_files = sorted(directory.glob("*.csv"))
            if self.reduced and folder in {"transactions", "transaction_items"}:
                csv_files = [path for path in csv_files if path.name.endswith("01.csv")]
            for csv_file in csv_files:
                frame = pd.read_csv(csv_file, parse_dates=parse_dates)
                frames.append(frame)

        dataset = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        self._datasets[key] = dataset
        return dataset.copy()

    def _transactions(self) -> pd.DataFrame:
        data = self._load_dataset("transactions", "transactions", parse_dates=["created_at"])
        if data.empty:
            return data
        mask_year = data["created_at"].dt.year.isin([2024, 2025])
        return data.loc[mask_year].copy()

    def _transactions_between_hours(self) -> pd.DataFrame:
        data = self._transactions()
        if data.empty:
            return data
        indexed = data.set_index("created_at")
        filtered = indexed.between_time("06:00", "23:00", inclusive="both")
        return filtered.reset_index()

    def _transaction_items(self) -> pd.DataFrame:
        data = self._load_dataset("transaction_items", "transaction_items", parse_dates=["created_at"])
        if data.empty:
            return data
        mask_year = data["created_at"].dt.year.isin([2024, 2025])
        return data.loc[mask_year].copy()

    def _menu_items(self) -> pd.DataFrame:
        return self._load_dataset("menu_items", "menu_items")

    def _stores(self) -> pd.DataFrame:
        return self._load_dataset("stores", "stores")

    def _users(self) -> pd.DataFrame:
        data = self._load_dataset("users", "users", parse_dates=["registered_at"])
        if data.empty:
            return data
        data = data.copy()
        data["birthdate"] = pd.to_datetime(data["birthdate"], errors="coerce")
        return data


    def _validate_amount_filter_transactions(self, rows: List[Dict[str, Any]]) -> ResultOutcome:
        expected = self._expected_amount_filter_transactions()
        actual = self._prepare_dataframe(rows, ["transaction_id", "final_amount"])
        if expected.empty and actual.empty:
            return True, "Sin resultados esperados para la consulta 1."

        if actual.empty and not expected.empty:
            return False, f"Se esperaban {len(expected)} filas pero no se recibió ninguna."

        actual["final_amount"] = pd.to_numeric(actual["final_amount"], errors="coerce").fillna(0.0)
        expected["final_amount"] = expected["final_amount"].astype(float)

        actual_sorted = actual.sort_values("transaction_id").reset_index(drop=True)
        expected_sorted = expected.sort_values("transaction_id").reset_index(drop=True)

        if len(actual_sorted) != len(expected_sorted):
            return (
                False,
                f"Se esperaban {len(expected_sorted)} transacciones pero se recibieron {len(actual_sorted)}.",
            )

        id_match = actual_sorted["transaction_id"].astype(str) == expected_sorted["transaction_id"].astype(str)
        amount_match = np.isclose(actual_sorted["final_amount"], expected_sorted["final_amount"], atol=0.01)
        mismatches = ~(id_match & amount_match)
        if mismatches.any():
            indices = mismatches[mismatches].index.tolist()[:5]
            samples = []
            for idx in indices:
                exp_row = expected_sorted.iloc[idx].to_dict()
                act_row = actual_sorted.iloc[idx].to_dict()
                samples.append(f"{exp_row} vs {act_row}")
            return False, "Las transacciones no coinciden; ejemplos: " + "; ".join(samples)

        return True, f"{len(expected_sorted)} transacciones verificadas correctamente."

    def _validate_top_items_quantity(self, rows: List[Dict[str, Any]]) -> ResultOutcome:
        expected = self._expected_top_items_quantity()
        actual = self._prepare_dataframe(rows, ["year_month_created_at", "item_name", "sellings_qty"])

        if expected.empty and actual.empty:
            return True, "Sin resultados esperados para top ítems por cantidad."

        if len(actual) != len(expected):
            return (
                False,
                f"Se esperaban {len(expected)} filas pero se recibieron {len(actual)}.",
            )

        actual["sellings_qty"] = pd.to_numeric(actual["sellings_qty"], errors="coerce").fillna(0).astype(int)
        expected["sellings_qty"] = expected["sellings_qty"].astype(int)

        actual_sorted = actual.sort_values("year_month_created_at").reset_index(drop=True)
        expected_sorted = expected.sort_values("year_month_created_at").reset_index(drop=True)

        if not actual_sorted.equals(expected_sorted):
            deltas = self._diff_rows(expected_sorted, actual_sorted)
            return False, "Diferencias detectadas en top ítems por cantidad: " + deltas

        return True, "Top ítems por cantidad verificados correctamente."

    def _validate_top_items_profit(self, rows: List[Dict[str, Any]]) -> ResultOutcome:
        expected = self._expected_top_items_profit()
        actual = self._prepare_dataframe(rows, ["year_month_created_at", "item_name", "profit_sum"])

        if expected.empty and actual.empty:
            return True, "Sin resultados esperados para top ítems por ganancia."

        if len(actual) != len(expected):
            return (
                False,
                f"Se esperaban {len(expected)} filas pero se recibieron {len(actual)}.",
            )

        actual["profit_sum"] = pd.to_numeric(actual["profit_sum"], errors="coerce").fillna(0.0)
        expected["profit_sum"] = expected["profit_sum"].astype(float)

        actual_sorted = actual.sort_values("year_month_created_at").reset_index(drop=True)
        expected_sorted = expected.sort_values("year_month_created_at").reset_index(drop=True)

        if not np.isclose(actual_sorted["profit_sum"], expected_sorted["profit_sum"], atol=0.5).all():
            deltas = self._diff_rows(expected_sorted, actual_sorted)
            return False, "Diferencias detectadas en top ítems por ganancia: " + deltas

        if not actual_sorted[["year_month_created_at", "item_name"]].equals(
            expected_sorted[["year_month_created_at", "item_name"]]
        ):
            deltas = self._diff_rows(expected_sorted, actual_sorted)
            return False, "Diferencias detectadas en top ítems por ganancia: " + deltas

        return True, "Top ítems por ganancia verificados correctamente."

    def _validate_tpv_summary(self, rows: List[Dict[str, Any]]) -> ResultOutcome:
        expected = self._expected_tpv_summary()
        actual = self._prepare_dataframe(rows, ["year_half_created_at", "store_id", "store_name", "tpv"])

        if expected.empty and actual.empty:
            return True, "Sin resultados esperados para el resumen de TPV."

        if len(actual) != len(expected):
            return (
                False,
                f"Se esperaban {len(expected)} filas pero se recibieron {len(actual)}.",
            )

        expected_sorted = expected.sort_values(["year_half_created_at", "store_name"]).reset_index(drop=True)
        actual_sorted = actual.sort_values(["year_half_created_at", "store_name"]).reset_index(drop=True)

        actual_sorted["store_id"] = actual_sorted["store_id"].astype(str)
        expected_sorted["store_id"] = expected_sorted["store_id"].astype(str)
        actual_sorted["tpv"] = pd.to_numeric(actual_sorted["tpv"], errors="coerce").fillna(0.0)
        expected_sorted["tpv"] = expected_sorted["tpv"].astype(float)

        same_store = actual_sorted[["year_half_created_at", "store_id", "store_name"]].equals(
            expected_sorted[["year_half_created_at", "store_id", "store_name"]]
        )
        same_value = np.isclose(actual_sorted["tpv"], expected_sorted["tpv"], atol=0.5).all()

        if not (same_store and same_value):
            deltas = self._diff_rows(expected_sorted, actual_sorted)
            return False, "Diferencias detectadas en el resumen de TPV: " + deltas

        return True, "Resumen de TPV verificado correctamente."

    def _validate_top_clients_birthdays(self, rows: List[Dict[str, Any]]) -> ResultOutcome:
        expected = self._expected_top_clients_birthdays()
        if expected.empty and not rows:
            return True, "Sin resultados esperados para top clientes por sucursal."

        columns = ["store_name", "birthdate", "purchases_qty", "user_id"]
        actual = self._prepare_dataframe(rows, columns)

        if actual.empty and not rows and expected.empty:
            return True, "Sin resultados esperados para top clientes por sucursal."

        if actual.empty and not expected.empty:
            return False, "Se esperaban resultados para top clientes, pero se recibió una lista vacía."

        actual["store_name"] = actual["store_name"].astype(str)
        actual["birthdate"] = pd.to_datetime(actual["birthdate"], errors="coerce").dt.date.astype(str)
        actual["purchases_qty"] = pd.to_numeric(actual["purchases_qty"], errors="coerce").fillna(0).astype(int)
        actual["user_id"] = pd.to_numeric(actual["user_id"], errors="coerce").fillna(0).astype(int)

        expected["store_name"] = expected["store_name"].astype(str)
        expected["birthdate"] = pd.to_datetime(expected["birthdate"], errors="coerce").dt.date.astype(str)
        expected["purchases_qty"] = pd.to_numeric(expected["purchases_qty"], errors="coerce").fillna(0).astype(int)
        expected["user_id"] = pd.to_numeric(expected["user_id"], errors="coerce").fillna(0).astype(int)

        expected_groups = {
            store_name: group.copy()
            for store_name, group in expected.groupby("store_name", sort=False)
        }

        invalid_rows: List[str] = []
        for _, row in actual.iterrows():
            store_name = row["store_name"]
            user_id = row["user_id"]

            if store_name not in expected_groups:
                invalid_rows.append(f"{store_name}#{user_id}")
                continue

            group = expected_groups[store_name]
            top_counts = group["purchases_qty"].nlargest(3).tolist()
            if not top_counts:
                invalid_rows.append(f"{store_name}#{user_id}")
                continue
            threshold = top_counts[-1]
            candidates = group[group["purchases_qty"] >= threshold]

            match = (
                (candidates["user_id"] == user_id)
                & (candidates["birthdate"] == row["birthdate"])
            )

            if not match.any():
                invalid_rows.append(f"{store_name}#{user_id}")

        if invalid_rows:
            sample = ", ".join(invalid_rows[:5])
            return False, "Clientes fuera del top 3 (con empates) detectados: " + sample

        return True, "Top clientes por sucursal verificados correctamente."


    def _expected_amount_filter_transactions(self) -> pd.DataFrame:
        data = self._transactions_between_hours()
        if data.empty:
            return data
        data = data.copy()
        data["final_amount"] = pd.to_numeric(data["final_amount"], errors="coerce").fillna(0.0)
        filtered = data.loc[data["final_amount"] >= 75.0, ["transaction_id", "final_amount"]]
        return filtered.sort_values("transaction_id").reset_index(drop=True)

    def _expected_top_items_quantity(self) -> pd.DataFrame:
        items = self._transaction_items()
        if items.empty:
            return items

        menu_items = self._menu_items()
        items = items.copy()
        items["year_month_created_at"] = items["created_at"].dt.strftime("%Y-%m")
        grouped = (
            items.groupby(["year_month_created_at", "item_id"], dropna=False)["quantity"]
            .sum()
            .reset_index(name="sellings_qty")
        )

        merged = grouped.merge(menu_items[["item_id", "item_name"]], on="item_id", how="left")
        merged = merged.sort_values(["year_month_created_at", "sellings_qty"], ascending=[True, False])
        top_by_month = merged.groupby("year_month_created_at", as_index=False).head(1)
        return top_by_month.reset_index(drop=True)[["year_month_created_at", "item_name", "sellings_qty"]]

    def _expected_top_items_profit(self) -> pd.DataFrame:
        items = self._transaction_items()
        if items.empty:
            return items

        menu_items = self._menu_items()
        items = items.copy()
        items["year_month_created_at"] = items["created_at"].dt.strftime("%Y-%m")
        grouped = (
            items.groupby(["year_month_created_at", "item_id"], dropna=False)["subtotal"]
            .sum()
            .reset_index(name="profit_sum")
        )

        merged = grouped.merge(menu_items[["item_id", "item_name"]], on="item_id", how="left")
        merged = merged.sort_values(["year_month_created_at", "profit_sum"], ascending=[True, False])
        top_by_month = merged.groupby("year_month_created_at", as_index=False).head(1)
        return top_by_month.reset_index(drop=True)[["year_month_created_at", "item_name", "profit_sum"]]

    def _expected_tpv_summary(self) -> pd.DataFrame:
        transactions = self._transactions_between_hours()
        if transactions.empty:
            return transactions

        stores = self._stores()
        transactions = transactions.copy()
        transactions["half_created_at"] = transactions["created_at"].dt.month.apply(lambda month: "1" if month <= 6 else "2")
        transactions["year_half_created_at"] = transactions["created_at"].dt.year.astype(str) + "-H" + transactions["half_created_at"]

        grouped = (
            transactions.groupby(["year_half_created_at", "store_id"], dropna=False)["final_amount"]
            .sum()
            .reset_index(name="tpv")
        )

        merged = grouped.merge(stores[["store_id", "store_name"]], on="store_id", how="left")
        return merged[["year_half_created_at", "store_id", "store_name", "tpv"]]

    def _expected_top_clients_birthdays(self) -> pd.DataFrame:
        transactions = self._transactions()
        if transactions.empty:
            return pd.DataFrame(columns=["store_name", "birthdate", "purchases_qty", "user_id"])

        stores = self._stores()
        users = self._users()

        filtered = transactions.dropna(subset=["user_id"]).copy()
        grouped = (
            filtered.groupby(["store_id", "user_id"])["transaction_id"]
            .count()
            .reset_index(name="purchases_qty")
        )

        top_records: List[pd.DataFrame] = []
        for store_id, group in grouped.groupby("store_id", sort=False):
            group_sorted = group.sort_values(["purchases_qty", "user_id"], ascending=[False, True]).reset_index(drop=True)
            if group_sorted.empty:
                continue
            unique_counts = group_sorted["purchases_qty"].drop_duplicates().tolist()
            threshold_index = min(len(unique_counts), 3) - 1
            threshold = unique_counts[threshold_index]
            top_records.append(group_sorted[group_sorted["purchases_qty"] >= threshold])

        if not top_records:
            return pd.DataFrame(columns=["store_name", "birthdate", "purchases_qty", "user_id"])

        top_df = pd.concat(top_records, ignore_index=True)
        merged = top_df.merge(stores[["store_id", "store_name"]], on="store_id", how="left")
        merged = merged.merge(users[["user_id", "birthdate"]], on="user_id", how="left")
        merged["birthdate"] = merged["birthdate"].dt.date.astype(str)

        result = merged[["store_name", "birthdate", "purchases_qty", "user_id"]].copy()
        result["purchases_qty"] = pd.to_numeric(result["purchases_qty"], errors="coerce").fillna(0).astype(int)
        result["user_id"] = pd.to_numeric(result["user_id"], errors="coerce").fillna(0).astype(int)
        result = result.sort_values(
            ["store_name", "purchases_qty", "user_id", "birthdate"],
            ascending=[True, False, True, True],
        ).reset_index(drop=True)
        return result


    def _prepare_dataframe(self, rows: List[Dict[str, Any]], expected_columns: List[str]) -> pd.DataFrame:
        """Convert list of dictionaries into a DataFrame with the expected columns."""
        if not rows:
            return pd.DataFrame(columns=expected_columns)

        frame = pd.DataFrame(rows)
        for column in expected_columns:
            if column not in frame.columns:
                frame[column] = np.nan if np is not None else None
        return frame[expected_columns].copy()

    def _diff_rows(self, expected: pd.DataFrame, actual: pd.DataFrame) -> str:
        """Create a short human-readable diff summary."""
        differences = []
        length = min(len(expected), len(actual))
        for idx in range(length):
            expected_row = expected.iloc[idx].to_dict()
            actual_row = actual.iloc[idx].to_dict()
            if expected_row != actual_row:
                differences.append(f"#{idx}: {expected_row} vs {actual_row}")
            if len(differences) >= 3:
                break

        if len(expected) != len(actual):
            differences.append(f"Tamaño esperado {len(expected)} vs recibido {len(actual)}")

        return "; ".join(differences)
