"""Results handling module for processing and displaying client results."""

import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class ResultsHandler:
    """Handles result processing and display formatting."""
    
    def __init__(self):
        """Initialize results handler."""
        self.results_received = 0
        self._tpv_header_printed = False
        self._quantity_header_printed = False
        self._profit_header_printed = False
        self._transactions_header_printed = False
        self._amount_summary_printed = False
        self._amount_results_total = 0
        self._top_clients_header_printed = False
        self._top_clients_section_printed = False
        self._top_clients_summary_printed = False
        self._top_clients_total = 0

    def _print_transactions_header(self) -> None:
        """Print header for transactions results."""
        if self._transactions_header_printed:
            return

        print("=" * 60)
        print("Transacciones (Id y monto) realizadas durante 2024 y 2025")
        print("entre las 06:00 AM y las 11:00 PM con monto total >= $75")
        print("=" * 60)
        self._transactions_header_printed = True

    def _render_amount_summary(self, payload: Dict[str, Any]) -> None:
        """Render amount filter summary results.
        
        Args:
            payload: Result payload containing results_count
        """
        count = int(payload.get('results_count', 0))
        self._amount_results_total = count
        self.results_received = count

        self._print_transactions_header()
        print(
            "Total de transacciones que cumplen las condiciones: "
            f"{self._amount_results_total}"
        )
        print("-" * 50)
        self._amount_summary_printed = True
        logger.info(
            "Reported total of %s transacciones filtradas por monto al usuario",
            self._amount_results_total,
        )

    def _print_top_clients_header(self) -> None:
        """Print header for top clients results."""
        if self._top_clients_header_printed:
            return

        print("=" * 60)
        print("Cumpleaños de los clientes con más compras por sucursal (2024-2025)")
        print("=" * 60)
        self._top_clients_header_printed = True

    def _render_top_items_table(
        self,
        title: str,
        rows: List[Dict[str, Any]],
        metric_key: str,
        header_flag_attr: str,
    ) -> None:
        """Render top items table with generic formatting.
        
        Args:
            title: Table title
            rows: List of result rows
            metric_key: Key for the metric column
            header_flag_attr: Attribute name for header printed flag
        """
        if not getattr(self, header_flag_attr):
            print("=" * 60)
            print(title)
            print("=" * 60)
            setattr(self, header_flag_attr, True)

        if not rows:
            print("Sin registros que cumplan las condiciones.")
            print("-" * 50)
            return

        metric_label = metric_key
        print(f"year_month_created_at - item_name - {metric_label}")

        for row in rows:
            year_month = row.get('year_month_created_at', '')
            item_name = row.get('item_name', 'Desconocido')
            value = row.get(metric_key, 0)

            if metric_key == 'profit_sum':
                try:
                    value_str = f"{float(value):.2f}"
                except (TypeError, ValueError):
                    value_str = "0.00"
            else:
                try:
                    value_str = f"{int(value)}"
                except (TypeError, ValueError):
                    value_str = "0"

            print(f"{year_month} - {item_name} - {value_str}")

        print("-" * 50)

    def _render_top_items_by_quantity(self, payload: Dict[str, Any]) -> None:
        """Render top items by quantity results.
        
        Args:
            payload: Result payload containing results list
        """
        rows = payload.get('results') or []
        self._render_top_items_table(
            "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
            rows,
            'sellings_qty',
            '_quantity_header_printed',
        )

    def _render_top_items_by_profit(self, payload: Dict[str, Any]) -> None:
        """Render top items by profit results.
        
        Args:
            payload: Result payload containing results list
        """
        rows = payload.get('results') or []
        self._render_top_items_table(
            "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
            rows,
            'profit_sum',
            '_profit_header_printed',
        )

    def _render_top_clients_birthdays(self, payload: Dict[str, Any]) -> None:
        """Render top clients birthdays results.
        
        Args:
            payload: Result payload containing results list
        """
        self._print_top_clients_header()

        if not getattr(self, '_top_clients_section_printed', False):
            print("=" * 60)
            print("TOP CLIENTES POR SUCURSAL (2024-2025)")
            print("=" * 60)
            self._top_clients_section_printed = True

        rows = payload.get('results') or []

        print("user_id - store_name - birthdate - purchases_qty")
        for row in rows:
            user_id = row.get('user_id', '')
            store_name = row.get('store_name', '')
            birthdate = row.get('birthdate', '')
            purchases_qty = row.get('purchases_qty', 0)
            print(f"{user_id} - {store_name} - {birthdate} - {purchases_qty}")

        print("-" * 50)
        self._top_clients_total += len(rows)
        self._top_clients_summary_printed = False

    def _print_tpv_header(self) -> None:
        """Print header for TPV results."""
        if self._tpv_header_printed:
            return

        print("=" * 60)
        print("RESUMEN TPV POR SEMESTRE Y SUCURSAL (2024-2025)")
        print("Transacciones entre las 06:00 y las 23:00")
        print("=" * 60)
        self._tpv_header_printed = True

    def _render_tpv_summary(self, payload: Dict[str, Any]) -> None:
        """Render TPV summary results.
        
        Args:
            payload: Result payload containing results list
        """
        try:
            results = payload.get('results') or []
            self._print_tpv_header()

            if not results:
                print("Sin registros que cumplan las condiciones para calcular TPV.")
                print("-" * 50)
                logger.info("TPV summary received without results")
                return

            # Sort results by year, semester, and store_name
            sorted_results = sorted(results, key=lambda x: (
                x.get('year', 0), 
                x.get('semester', ''), 
                x.get('store_name', '')
            ))

            for entry in sorted_results:
                store_name = entry.get('store_name', 'unknown')
                store_id = entry.get('store_id', 'unknown')
                year = entry.get('year', 'unknown')
                semester = entry.get('semester', 'unknown')
                try:
                    tpv_value = float(entry.get('tpv', 0))
                except (TypeError, ValueError):
                    tpv_value = 0.0
                print(f"Sucursal {store_name} - {year} {semester}: ${tpv_value:0.2f}")

            print("-" * 50)
            logger.info(
                "Processed TPV summary with %s entries", len(results)
            )
        except Exception as exc:
            logger.error(f"Failed to render TPV summary: {exc}")

    def handle_single_result(self, result: Dict[str, Any]) -> bool:
        """Print a single result message received from the results stream.

        Args:
            result: Result message dictionary
            
        Returns:
            bool: False if an EOF control message was received, True otherwise.
        """
        if not isinstance(result, dict):
            logger.warning(f"Ignoring unexpected result payload: {result}")
            return True

        # Allow special control messages to stop consumption
        message_type = result.get('type')
        if message_type:
            normalized_type = str(message_type).upper()
            if normalized_type == 'EOF':
                logger.info("Received EOF control message from results stream")
                if not self._amount_summary_printed and self._amount_results_total:
                    self._render_amount_summary({'results_count': self._amount_results_total})
                return False
            if normalized_type == 'TPV_SUMMARY':
                self._render_tpv_summary(result)
                return True
            if normalized_type == 'TOP_ITEMS_BY_QUANTITY':
                self._render_top_items_by_quantity(result)
                return True
            if normalized_type == 'TOP_ITEMS_BY_PROFIT':
                self._render_top_items_by_profit(result)
                return True
            if normalized_type == 'AMOUNT_FILTER_SUMMARY':
                self._render_amount_summary(result)
                return True
            if normalized_type == 'TOP_CLIENTS_BIRTHDAYS':
                self._render_top_clients_birthdays(result)
                return True

        self.results_received += 1
        logger.debug(
            "Resultado #%s contabilizado desde results stream", self.results_received
        )

        return True

    def handle_results_message(self, message: Any) -> bool:
        """Handle stream messages that may contain individual or batched results.

        Args:
            message: Message from results stream
            
        Returns:
            bool: False when an EOF control message is encountered.
        """
        logger.info(f"Received message from results stream: {type(message)}")
        try:
            if isinstance(message, list):
                for item in message:
                    if not self.handle_single_result(item):
                        return False
            else:
                if not self.handle_single_result(message):
                    return False
            return True
        except Exception as exc:
            logger.error(f"Error processing results message: {exc}")
            return True

    def process_results_stream(self, socket_connection) -> None:
        """Process results stream from socket connection.
        
        Args:
            socket_connection: Connected socket to receive results from
        """
        if not socket_connection:
            logger.info("No socket connection available; skipping results listener")
            return

        buffer = ""

        try:
            logger.info("Waiting for processed results from gateway")

            while True:
                chunk = socket_connection.recv(4096)
                if not chunk:
                    logger.info("Gateway closed the connection")
                    break

                buffer += chunk.decode('utf-8')

                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning(f"Discarding malformed results payload: {exc}")
                        continue

                    if not self.handle_results_message(message):
                        logger.info("EOF received from gateway; stopping listener")
                        return

        except KeyboardInterrupt:
            logger.info("Results listener interrupted by user")
        except Exception as exc:
            logger.error(f"Error while listening for results: {exc}")
        finally:
            logger.info(f"Total results received: {self.results_received}")