"""Results handling module for processing and persisting client results."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from results_validator import ResultsValidator

logger = logging.getLogger(__name__)

class ResultsHandler:
    """Handles result processing and stores formatted output into files."""

    def __init__(self, data_dir: os.PathLike[str] | str) -> None:
        self.queries_expected = int(os.getenv("QUERIES_EXPECTED", "4")) # Default to 4 queries
        self.queries_completed = 0
        self.query1_items_received = 0
        
        self.results_dir = Path(".results")
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.validator = ResultsValidator(data_dir)
        self._amount_validation_pending = False
        self._amount_validation_finalized = False

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
        self._validation_results: List[Tuple[str, bool, str]] = []

    def reset_session_state(self):
        """Reset state for a new session."""
        self.queries_completed = 0
        self.query1_items_received = 0
        if self.validator.enabled:
            self.validator.reset()
        self._amount_validation_pending = False
        self._amount_validation_finalized = False
        self._validation_results.clear()
        logger.info("Results handler state reset for new session")

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

    def _validate_result(
        self,
        result_type: str,
        rows: Iterable[Dict[str, Any]],
        *,
        final: bool = True,
    ) -> None:
        """Send rows to the validator and log the outcome if enabled."""

        if not self.validator.enabled:
            return

        outcome = self.validator.validate(result_type, rows, final=final)

        if not final or not outcome:
            return

        success, detail = outcome
        label = self.validator.get_label(result_type)
        self._validation_results.append((label, success, detail))

        if result_type == "AMOUNT_FILTER_TRANSACTIONS":
            self._amount_validation_finalized = True
            self._amount_validation_pending = False

    def _finalize_amount_transactions_validation(self) -> None:
        """Ensure the accumulated amount-filter results are validated once."""

        if not self.validator.enabled:
            return

        if self._amount_validation_finalized or not self._amount_validation_pending:
            return

        self._validate_result("AMOUNT_FILTER_TRANSACTIONS", [], final=True)

    def print_validation_summary(self) -> None:
        """Print a summary of all validation results."""
        if not self.validator.enabled or not self._validation_results:
            return

        logger.info("=" * 60)
        logger.info("RESUMEN DE RESULTADOS")
        logger.info("=" * 60)
        
        for label, success, detail in self._validation_results:
            if success:
                logger.info("✓ Validación exitosa (%s): %s", label, detail)
            else:
                logger.error("✗ Validación fallida (%s): %s", label, detail)
        
        logger.info("=" * 60)

    def _render_amount_transactions(self, payload: Dict[str, Any], client_id: str) -> List[Dict[str, Any]]:
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
        return transactions

    def _render_top_items_table(
        self,
        payload: Dict[str, Any],
        title: str,
        metric_key: str,
        filename: str,
        client_id: str,
    ) -> List[Dict[str, Any]]:
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
        return rows

    def _render_top_clients_birthdays(self, payload: Dict[str, Any], client_id: str) -> List[Dict[str, Any]]:
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
            # Sort by store name and birthdate, mimicking expected display
            sorted_rows = sorted(
                rows,
                key=lambda row: (
                    row.get("store_name", ""),
                    str(row.get("birthdate", "")),
                ),
            )

            per_store_counters: Dict[str, int] = {}
            body_lines.append("store_name - birthdate")
            for row in sorted_rows:
                store_name = row.get("store_name", "Desconocido")
                birthdate = row.get("birthdate", "")

                store_counter = per_store_counters.get(store_name, 0)
                if store_counter >= 3:
                    continue

                per_store_counters[store_name] = store_counter + 1
                birthdate_str = str(birthdate) if birthdate is not None else ""
                body_lines.append(f"{store_name} - {birthdate_str}")

            body_lines.append("-" * 50)

        self._append_to_file("top_clients_birthdays.txt", body_lines, client_id)

        return rows

    def _render_tpv_summary(self, payload: Dict[str, Any], client_id: str) -> List[Dict[str, Any]]:
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
        else:
            for entry in results:
                store_name = entry.get("store_name", "unknown")
                store_id = entry.get("store_id", "unknown")
                period = entry.get("year_half_created_at") or ""
                if not period:
                    year = entry.get("year")
                    semester = entry.get("semester")
                    period = f"{year} {semester}".strip()

                try:
                    tpv_value = float(entry.get("tpv", 0))
                except (TypeError, ValueError):
                    tpv_value = 0.0

                body_lines.append(
                    f"Sucursal {store_id} {period} {store_name}: ${tpv_value:0.2f}"
                )

        body_lines.append("-" * 50)

        self._append_to_file("tpv_summary.txt", body_lines, client_id)

        return results

    def handle_single_result(self, result: Dict[str, Any]) -> bool:
        """Process a single result message.

        Returns False when an EOF control message is received to stop processing.
        """

        if not isinstance(result, dict):
            logger.warning("Ignorando payload de resultado inesperado: %s", result)
            return True

        logger.info(f"Client received result: {result}")

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
        
        logger.info(f"Processing result type: {normalized_type} for client {client_id}")

        if normalized_type == "EOF":
            self._finalize_amount_transactions_validation()
            return False
        if normalized_type == "TPV_SUMMARY":
            rendered = self._render_tpv_summary(result, client_id)
            self._validate_result(normalized_type, rendered or [])
            return True
        if normalized_type == "TOP_ITEMS_BY_QUANTITY":
            logger.info(f"Processing TOP_ITEMS_BY_QUANTITY for client {client_id}: {result}")
            rendered = self._render_top_items_table(
                result,
                "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
                "sellings_qty",
                "top_items_by_quantity.txt",
                client_id,
            )
            logger.info(f"Rendered {len(rendered)} quantity results for client {client_id}")
            self._validate_result(normalized_type, rendered or [])
            return True
        if normalized_type == "TOP_ITEMS_BY_PROFIT":
            logger.info(f"Processing TOP_ITEMS_BY_PROFIT for client {client_id}: {result}")
            rendered = self._render_top_items_table(
                result,
                "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
                "profit_sum",
                "top_items_by_profit.txt",
                client_id,
            )
            logger.info(f"Rendered {len(rendered)} profit results for client {client_id}")
            self._validate_result(normalized_type, rendered or [])
            return True
        if normalized_type == "TOP_CLIENTS_BIRTHDAYS":
            rendered = self._render_top_clients_birthdays(result, client_id)
            self._validate_result(normalized_type, rendered or [])
            return True
        if normalized_type == "AMOUNT_FILTER_TRANSACTIONS":
            rendered = self._render_amount_transactions(result, client_id)
            self._amount_validation_pending = True
            self._validate_result("AMOUNT_FILTER_TRANSACTIONS", rendered or [], final=False)
            return True
        if normalized_type == "AMOUNT_FILTER_SUMMARY":
            summary_rows = result.get("results")
            if not isinstance(summary_rows, list):
                summary_rows = []
            self._validate_result("AMOUNT_FILTER_TRANSACTIONS", summary_rows or [], final=True)
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
            self._finalize_amount_transactions_validation()
            logger.info(f"Total query 1 results received: {self.query1_items_received}")
            self.print_validation_summary()
