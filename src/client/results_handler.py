"""Results handling module for processing and persisting client results."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

logger = logging.getLogger(__name__)

class ResultsHandler:
    """Handles result processing and stores formatted output into files."""

    def __init__(self) -> None:
        self.queries_expected = int(os.getenv("QUERIES_EXPECTED", "4")) # Default to 4 queries
        self.queries_completed = 0

        self.query1_items_received = 0
        self.results_dir = Path(".results")
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self._file_headers: Dict[str, List[str]] = {
            "amount_filter_transactions.txt": [
                "=" * 60,
                "Transacciones (Id y monto) realizadas durante 2024 y 2025",
                "entre las 06:00 AM y las 11:00 PM con monto total >= $75",
                "=" * 60,
                "",
            ],
            "top_items_by_quantity.txt": [
                "=" * 60,
                "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
                "=" * 60,
                "",
            ],
            "top_items_by_profit.txt": [
                "=" * 60,
                "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
                "=" * 60,
                "",
            ],
            "top_clients_birthdays.txt": [
                "=" * 60,
                "Cumpleaños de los clientes con más compras por sucursal (2024-2025)",
                "=" * 60,
                "",
            ],
            "tpv_summary.txt": [
                "=" * 60,
                "RESUMEN TPV POR SEMESTRE Y SUCURSAL (2024-2025)",
                "Transacciones entre las 06:00 y las 23:00",
                "=" * 60,
                "",
            ],
        }

        self._initialized_clients: set[str] = set()
        self.current_client_id: str = ""

    def _initialize_client_files(self, client_id: str) -> Path:
        """Create client-specific directory and initialize headers if needed."""

        client_dir = self.results_dir / client_id
        client_dir.mkdir(parents=True, exist_ok=True)

        if client_id not in self._initialized_clients:
            for filename, header_lines in self._file_headers.items():
                file_path = client_dir / filename
                try:
                    with file_path.open("w", encoding="utf-8") as file_handle:
                        for line in header_lines:
                            file_handle.write(f"{line}\n")
                    logger.info("Inicializado archivo de resultados %s", file_path)
                except Exception as exc:  # noqa: BLE001
                    logger.error("No se pudo inicializar el archivo %s: %s", file_path, exc)
            self._initialized_clients.add(client_id)

        return client_dir

    def _append_to_file(
        self,
        filename: str,
        body_lines: Iterable[str],
        client_id: str,
    ) -> Path:
        """Append formatted lines into the client-specific results file."""

        client_dir = self._initialize_client_files(client_id)
        file_path = client_dir / filename

        try:
            with file_path.open("a", encoding="utf-8") as file_handle:
                for line in body_lines:
                    file_handle.write(f"{line}\n")
            # Log the file path for all files except the amount filter
            # Too many lines in that one
            if (filename != "amount_filter_transactions.txt"):
                logger.info("Resultados escritos en %s", file_path)
        except Exception as exc:  # noqa: BLE001
            logger.error("No se pudieron escribir resultados en %s: %s", file_path, exc)

        return file_path

    def _render_amount_transactions(self, payload: Dict[str, Any], client_id: str) -> None:
        """Persist amount-filtered transactions into a dedicated file."""

        transactions = payload.get("data") or payload.get("results") or []
        if not isinstance(transactions, list):
            logger.warning(
                "Estructura de transacciones inesperada para amount_filter: %s",
                type(transactions),
            )
            transactions = []

        total = len(transactions)
        body_lines: List[str] = []
        for transaction in transactions:
                transaction_id = transaction.get("transaction_id", "desconocido")
                final_amount_raw = transaction.get("final_amount")
                try:
                    amount_value = float(final_amount_raw)
                    amount_str = f"{amount_value:.2f}"
                except (TypeError, ValueError):
                    amount_str = "0.00"
                body_lines.append(f"{transaction_id} - {amount_str}")

        self._append_to_file("amount_filter_transactions.txt", body_lines, client_id)
        self.query1_items_received += total

    def _render_top_items_table(
        self,
        payload: Dict[str, Any],
        title: str,
        metric_key: str,
        filename: str,
        client_id: str,
    ) -> None:
        """Render generic top items results into the corresponding file."""

        rows = payload.get("results") or []
        if not isinstance(rows, list):
            logger.warning("Estructura de resultados inesperada para %s", title)
            rows = []

        body_lines: List[str] = []
        if not rows:
            body_lines.append("Sin registros que cumplan las condiciones.")
            body_lines.append("-" * 50)
        else:
            metric_label = metric_key
            body_lines.append(
                f"year_month_created_at - item_name - {metric_label}"
            )
            for row in rows:
                year_month = row.get("year_month_created_at", "")
                item_name = row.get("item_name", "Desconocido")
                value = row.get(metric_key, 0)

                if metric_key == "profit_sum":
                    try:
                        value_str = f"{float(value):.2f}"
                    except (TypeError, ValueError):
                        value_str = "0.00"
                else:
                    try:
                        value_str = f"{int(value)}"
                    except (TypeError, ValueError):
                        value_str = "0"

                body_lines.append(f"{year_month} - {item_name} - {value_str}")

            body_lines.append("-" * 50)

        self._append_to_file(filename, body_lines, client_id)
        self.query1_items_received += len(rows)

    def _render_top_clients_birthdays(self, payload: Dict[str, Any], client_id: str) -> None:
        """Persist top clients birthdays into the results folder."""

        rows = payload.get("results") or []
        if not isinstance(rows, list):
            logger.warning(
                "Estructura de resultados inesperada para top clients: %s",
                type(rows),
            )
            rows = []

        body_lines: List[str] = [
            "TOP CLIENTES POR SUCURSAL (2024-2025)",
        ]

        if not rows:
            body_lines.append("Sin registros que cumplan las condiciones.")
            body_lines.append("-" * 50)
        else:
            body_lines.append("user_id - store_name - birthdate - purchases_qty")
            for row in rows:
                user_id = row.get("user_id", "")
                store_name = row.get("store_name", "")
                birthdate = row.get("birthdate", "")
                purchases_qty = row.get("purchases_qty", 0)
                try:
                    purchases_str = f"{int(purchases_qty)}"
                except (TypeError, ValueError):
                    purchases_str = "0"
                body_lines.append(
                    f"{user_id} - {store_name} - {birthdate} - {purchases_str}"
                )
            body_lines.append("-" * 50)

        self._append_to_file("top_clients_birthdays.txt", body_lines, client_id)
        self.query1_items_received += len(rows)

    def _render_tpv_summary(self, payload: Dict[str, Any], client_id: str) -> None:
        """Persist TPV summary results."""

        results = payload.get("results") or []
        if not isinstance(results, list):
            logger.warning("Estructura de resultados inesperada para TPV: %s", type(results))
            results = []

        body_lines: List[str] = []
        if not results:
            body_lines.append(
                "Sin registros que cumplan las condiciones para calcular TPV."
            )
            body_lines.append("-" * 50)
        else:
            sorted_results = sorted(
                results,
                key=lambda entry: (
                    entry.get("year", 0),
                    entry.get("semester", ""),
                    entry.get("store_name", ""),
                ),
            )
            for entry in sorted_results:
                store_name = entry.get("store_name", "unknown")
                store_id = entry.get("store_id", "unknown")
                semester = entry.get("year_half_created_at", "unknown")
                try:
                    tpv_value = float(entry.get("tpv", 0))
                except (TypeError, ValueError):
                    tpv_value = 0.0
                body_lines.append(
                    f"Sucursal {store_id} {semester} {store_name}: ${tpv_value:0.2f}"
                )
            body_lines.append("-" * 50)

        self._append_to_file("tpv_summary.txt", body_lines, client_id)
        self.query1_items_received += len(results)

    def _process_quantity_profit_bundle(self, bundle: Dict[str, Any], client_id: str) -> bool:
        """Process bundled quantity/profit results emitted without explicit types."""

        processed = False

        quantity_rows = bundle.get("quantity")
        if isinstance(quantity_rows, list):
            self._render_top_items_table(
                {"results": quantity_rows},
                "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
                "sellings_qty",
                "top_items_by_quantity.txt",
                client_id,
            )
            processed = True

        profit_rows = bundle.get("profit")
        if isinstance(profit_rows, list):
            self._render_top_items_table(
                {"results": profit_rows},
                "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
                "profit_sum",
                "top_items_by_profit.txt",
                client_id,
            )
            processed = True

        return processed

    def _process_data_list(self, rows: List[Any], client_id: str) -> bool:
        """Infer result type from a list payload when message type is missing."""

        if not rows:
            return False

        first_entry = rows[0]
        if isinstance(first_entry, dict) and {"quantity", "profit"}.issubset(first_entry.keys()):
            return self._process_quantity_profit_bundle(first_entry, client_id)

        if not all(isinstance(row, dict) for row in rows):
            return False

        sample_keys = {key for key in rows[0] if isinstance(key, str)}

        if "transaction_id" in sample_keys or "final_amount" in sample_keys:
            self._render_amount_transactions({"data": rows}, client_id)
            return True

        if {"year_month_created_at", "sellings_qty"}.issubset(sample_keys):
            self._render_top_items_table(
                {"results": rows},
                "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
                "sellings_qty",
                "top_items_by_quantity.txt",
                client_id,
            )
            return True

        if {"year_month_created_at", "profit_sum"}.issubset(sample_keys):
            self._render_top_items_table(
                {"results": rows},
                "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
                "profit_sum",
                "top_items_by_profit.txt",
                client_id,
            )
            return True

        if "tpv" in sample_keys or {
            "year",
            "semester",
            "store_name",
        }.intersection(sample_keys):
            self._render_tpv_summary({"results": rows}, client_id)
            return True

        if {"user_id", "purchases_qty"}.issubset(sample_keys) or "birthdate" in sample_keys:
            self._render_top_clients_birthdays({"results": rows}, client_id)
            return True

        return False

    def _process_data_dict(self, data: Dict[str, Any], client_id: str) -> bool:
        """Infer result type from a dict payload when message type is missing."""

        if {"quantity", "profit"}.issubset(data.keys()):
            return self._process_quantity_profit_bundle(data, client_id)

        results_section = data.get("results")
        if isinstance(results_section, list):
            nested_type = str(data.get("type") or "").upper()

            if nested_type == "TOP_ITEMS_BY_QUANTITY":
                self._render_top_items_table(
                    {"results": results_section},
                    "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
                    "sellings_qty",
                    "top_items_by_quantity.txt",
                    client_id,
                )
                return True

            if nested_type == "TOP_ITEMS_BY_PROFIT":
                self._render_top_items_table(
                    {"results": results_section},
                    "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
                    "profit_sum",
                    "top_items_by_profit.txt",
                    client_id,
                )
                return True

            if nested_type == "TPV_SUMMARY":
                self._render_tpv_summary({"results": results_section}, client_id)
                return True

            if nested_type in {"TOP_CLIENTS_BIRTHDAYS", "TOP_CLIENTS_PARTIAL"}:
                self._render_top_clients_birthdays({"results": results_section}, client_id)
                return True

            if nested_type in {"AMOUNT_FILTER_TRANSACTIONS", "AMOUNT_FILTER_SUMMARY"}:
                self._render_amount_transactions({"data": results_section}, client_id)
                return True

            return self._process_data_list(results_section, client_id)

        return False

    def _process_implicit_payload(self, result: Dict[str, Any], client_id: str) -> bool:
        """Handle messages without a recognized type by examining their payload."""

        data_section = result.get("data")

        if isinstance(data_section, dict):
            return self._process_data_dict(data_section, client_id)

        if isinstance(data_section, list):
            return self._process_data_list(data_section, client_id)

        return False

    def handle_single_result(self, result: Dict[str, Any]) -> bool:
        """Process a single result message.

        Returns False when an EOF control message is received to stop processing.
        """

        if not isinstance(result, dict):
            logger.warning("Ignorando payload de resultado inesperado: %s", result)
            return True

        message_client_id = result.get("client_id")
        if message_client_id:
            if message_client_id != self.current_client_id:
                logger.info("Procesando resultados para cliente %s", message_client_id)
            self.current_client_id = message_client_id
            self._initialize_client_files(message_client_id)
        elif not self.current_client_id:
            logger.warning("Resultado recibido sin client_id y sin cliente activo")
            return True

        client_id = self.current_client_id

        message_type = result.get("type")
        normalized_type = str(message_type).upper() if message_type else ""

        if normalized_type == "EOF":
            logger.info("EOF recibido en resultados del cliente")
            return False
        if normalized_type == "TPV_SUMMARY":
            self._render_tpv_summary(result, client_id)
            return True
        if normalized_type == "TOP_ITEMS_BY_QUANTITY":
            self._render_top_items_table(
                result,
                "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
                "sellings_qty",
                "top_items_by_quantity.txt",
                client_id,
            )
            return True
        if normalized_type == "TOP_ITEMS_BY_PROFIT":
            self._render_top_items_table(
                result,
                "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
                "profit_sum",
                "top_items_by_profit.txt",
                client_id,
            )
            return True
        if normalized_type == "TOP_CLIENTS_BIRTHDAYS":
            self._render_top_clients_birthdays(result, client_id)
            return True
        if normalized_type in {"AMOUNT_FILTER_TRANSACTIONS", "AMOUNT_FILTER_SUMMARY"}:
            self._render_amount_transactions(result, client_id)
            return True

        if self._process_implicit_payload(result, client_id):
            return True

        logger.debug("Resultado no reconocido, se omite: %s", result)
        return True

    def handle_results_message(self, message: Any) -> bool:
        """Handle stream messages that may contain individual or batched results."""

        try:
            if isinstance(message, list):
                for item in message:
                    if not self.handle_single_result(item):
                        return False
            else:
                if not self.handle_single_result(message):
                    return False
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Error processing results message: {exc}")
            return True

    def process_results_stream(self, socket_connection) -> None:
        """Process results stream from socket connection."""

        if not socket_connection:
            logger.info("No socket connection available; skipping results listener")
            return

        buffer = ""

        try:
            logger.info("Waiting for processed results from gateway")

            while self.queries_completed < self.queries_expected:
                chunk = socket_connection.recv(4096)
                if not chunk:
                    logger.info("Gateway closed the connection")
                    break

                buffer += chunk.decode("utf-8")

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning(f"Discarding malformed results payload: {exc}")
                        continue

                    if not self.handle_results_message(message):
                        logger.info("EOF received from gateway")
                        self.queries_completed += 1

        except KeyboardInterrupt:
            logger.info("Results listener interrupted by user")
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Error while listening for results: {exc}")
        finally:
            logger.info(f"Total results received: {self.query1_items_received}")
