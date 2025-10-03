import os
import csv
import socket
import logging
import sys
import json
import signal
from typing import List, Dict, Any
from client.protocol import (DataType, send_batch, send_eof, receive_response)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Variable global para manejo de SIGTERM
shutdown_requested = False

def handle_sigterm(signum, frame):
    """Maneja la señal SIGTERM para terminar ordenadamente"""
    global shutdown_requested
    logger.info("SIGTERM recibido, iniciando shutdown ordenado...")
    shutdown_requested = True

def load_config_from_json(config_file: str = 'workers_config.json') -> Dict[str, Any]:
    """Load configuration from JSON file"""
    try:
        # Try to load from the current directory first
        if os.path.exists(config_file):
            config_path = config_file
        else:
            # Try to load from parent directory (common location)
            parent_config = os.path.join('..', '..', config_file)
            if os.path.exists(parent_config):
                config_path = parent_config
            else:
                logger.warning(f"Config file {config_file} not found, using defaults")
                return {}
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found, using defaults")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON config file {config_file}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error reading config file {config_file}: {e}")
        return {}

# Maximum serialized size per row type in bytes (calculated from CSV structure analysis)
MAX_ROW_SIZES = {
    DataType.TRANSACTIONS: 95,      # transaction_id(40) + store_id(4) + payment_method_id(4) + 
                                    # voucher_id(4) + user_id(4) + amounts(12) + created_at(23) + overhead(4)
    DataType.TRANSACTION_ITEMS: 83,  # transaction_id(40) + item_id(4) + quantity(4) + 
                                     # unit_price(4) + subtotal(4) + created_at(23) + overhead(4)
    DataType.USERS: 56,             # user_id(4) + gender(11) + birthdate(14) + 
                                    # registered_at(23) + overhead(4)
    DataType.STORES: 60,            # store_id(4) + store_name(50) + overhead(6)
    DataType.MENU_ITEMS: 88         # item metadata fields (id, name, category, price, flags)
}

def get_max_row_size(data_type: DataType) -> int:
    """Get the maximum serialized size for a row of the given data type"""
    return MAX_ROW_SIZES.get(data_type, 100)  # Default to 100 if unknown type

def estimate_row_size(data_type: DataType, sample_row: Dict[str, Any] | None = None) -> int:
    """Estimate the serialized size of a row in bytes using hardcoded maximums"""
    # Use hardcoded maximum sizes instead of actual serialization
    # This is more efficient and the sizes are predictable based on CSV structure
    return get_max_row_size(data_type)

class CoffeeShopClient:
    def __init__(self, config_file: str = 'workers_config.json'):
        # Load configuration from JSON file
        config = load_config_from_json(config_file)
        
        # Get common environment configuration
        common_env = config.get('common_environment', {})
        
        # Get client-specific configuration
        client_config = config.get('service_environment', {}).get('client', {})
        
        # Gateway configuration with environment variable fallback
        self.gateway_host = os.getenv('GATEWAY_HOST', client_config.get('GATEWAY_HOST', 'localhost'))
        self.gateway_port = int(os.getenv('GATEWAY_PORT', client_config.get('GATEWAY_PORT', '12345')))
        
        # Batch configuration
        self.max_batch_size_kb = int(os.getenv('BATCH_MAX_SIZE_KB', client_config.get('BATCH_MAX_SIZE_KB', '64')))
        
        # Logging configuration
        log_level = os.getenv('LOG_LEVEL', client_config.get('LOG_LEVEL', 'INFO')).upper()
        logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))
        
        # Data directory configuration
        self.data_dir = os.getenv('DATA_DIR', client_config.get('DATA_DIR', '.data'))
        
        self.socket: socket.socket | None = None
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

        logger.info(
            f"Client configured - Gateway: {self.gateway_host}:{self.gateway_port}, "
            f"Batch: {self.max_batch_size_kb}KB max, Data dir: {self.data_dir}"
        )
        
    def connect_to_gateway(self):
        """Establish connection to gateway"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.gateway_host, self.gateway_port))
            logger.info(f"Connected to gateway at {self.gateway_host}:{self.gateway_port}")
        except Exception as e:
            logger.error(f"Failed to connect to gateway: {e}")
            raise
            
    def disconnect(self):
        """Close connection to gateway"""
        if self.socket:
            self.socket.close()
            self.socket = None
            logger.info("Disconnected from gateway")


    def _print_transactions_header(self) -> None:
        if self._transactions_header_printed:
            return

        print("=" * 60)
        print("Transacciones (Id y monto) realizadas durante 2024 y 2025")
        print("entre las 06:00 AM y las 11:00 PM con monto total >= $75")
        print("=" * 60)
        self._transactions_header_printed = True

    def _render_amount_summary(self, payload: Dict[str, Any]) -> None:
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
        rows = payload.get('results') or []
        self._render_top_items_table(
            "TOP ÍTEMS POR CANTIDAD (2024-2025, 06:00-23:00)",
            rows,
            'sellings_qty',
            '_quantity_header_printed',
        )

    def _render_top_items_by_profit(self, payload: Dict[str, Any]) -> None:
        rows = payload.get('results') or []
        self._render_top_items_table(
            "TOP ÍTEMS POR GANANCIA (2024-2025, 06:00-23:00)",
            rows,
            'profit_sum',
            '_profit_header_printed',
        )

    def _render_top_clients_birthdays(self, payload: Dict[str, Any]) -> None:
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

    def _handle_single_result(self, result: Dict[str, Any]) -> bool:
        """Print a single result message received from the results stream.

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

    def _print_tpv_header(self) -> None:
        if self._tpv_header_printed:
            return

        print("=" * 60)
        print("RESUMEN TPV POR SEMESTRE Y SUCURSAL (2024-2025)")
        print("Transacciones entre las 06:00 y las 23:00")
        print("=" * 60)
        self._tpv_header_printed = True

    def _render_tpv_summary(self, payload: Dict[str, Any]) -> None:
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

    def _handle_results_message(self, message: Any) -> bool:
        """Handle stream messages that may contain individual or batched results.

        Returns:
            bool: False when an EOF control message is encountered.
        """
        try:
            if isinstance(message, list):
                for item in message:
                    if not self._handle_single_result(item):
                        return False
            else:
                if not self._handle_single_result(message):
                    return False
            return True
        except Exception as exc:
            logger.error(f"Error processing results message: {exc}")
            return True

    def listen_for_results(self):
        """Escucha resultados reenviados por el gateway mediante la misma conexión TCP."""
        if not self.socket:
            logger.info("Gateway connection not available; skipping results listener")
            return

        sock = self.socket
        buffer = ""

        try:
            logger.info("Waiting for processed results from gateway")

            while True:
                chunk = sock.recv(4096)
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

                    if not self._handle_results_message(message):
                        logger.info("EOF received from gateway; stopping listener")
                        return

        except KeyboardInterrupt:
            logger.info("Results listener interrupted by user")
        except Exception as exc:
            logger.error(f"Error while listening for results: {exc}")
        finally:
            logger.info(f"Total results received: {self.results_received}")
    
    def get_csv_files_by_type(self, data_type_str: str) -> List[str]:
        """Get all CSV files for a specific data type"""
        type_dir = os.path.join(self.data_dir, data_type_str)
        
        if not os.path.exists(type_dir):
            logger.warning(f"Directory {type_dir} does not exist")
            return []
            
        csv_files = []
        for file in os.listdir(type_dir):
            if file.endswith('.csv'):
                csv_files.append(os.path.join(type_dir, file))
        
        csv_files.sort()  # Process files in order
        logger.info(f"Found {len(csv_files)} CSV files in {type_dir}")
        return csv_files
    
    def process_csv_file_streaming(self, file_path: str, data_type: DataType, batch_size: int):
        """Stream process CSV file, sending batches as they're read"""
        total_rows = 0
        current_batch = []
        
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    # Clean up the row data
                    cleaned_row = {}
                    for key, value in row.items():
                        cleaned_key = key.strip()
                        cleaned_value = value.strip() if isinstance(value, str) else value
                        cleaned_row[cleaned_key] = cleaned_value
                    
                    current_batch.append(cleaned_row)
                    
                    # Send batch when it reaches the target size
                    if len(current_batch) >= batch_size:
                        if not self._send_batch_to_gateway(current_batch, data_type):
                            return total_rows  # Return on error
                        total_rows += len(current_batch)
                        current_batch = []
                
                # Send remaining rows in the last batch
                if current_batch:
                    if not self._send_batch_to_gateway(current_batch, data_type):
                        return total_rows  # Return on error
                    total_rows += len(current_batch)
            
            logger.info(f"Streamed {total_rows} rows from {file_path}")
            return total_rows
            
        except Exception as e:
            logger.error(f"Failed to stream {file_path}: {e}")
            return total_rows
    
    def _send_batch_to_gateway(self, batch, data_type: DataType) -> bool:
        """Send a batch to the gateway and handle response"""
        if not self.socket:
            logger.error("No socket connection available")
            return False
            
        try:
            send_batch(self.socket, data_type, batch)
            
            # Wait for response
            response_code = receive_response(self.socket)
            if response_code == 0:  # ResponseCode.OK
                logger.debug(f"Successfully sent batch of {len(batch)} rows for {data_type.name}")
                return True
            else:
                logger.error(f"Gateway rejected batch for {data_type.name} (response code: {response_code})")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send batch for {data_type.name}: {e}")
            return False
    
    def calculate_optimal_batch_size(self, data_type: DataType) -> int:
        """Calculate optimal batch size based on KB limit and hardcoded row size"""
        
        max_row_size = get_max_row_size(data_type)
        max_batch_size_bytes = self.max_batch_size_kb * 1024
        
        # Calculate how many rows fit in the KB limit
        optimal_batch_size = max(1, int(max_batch_size_bytes / max_row_size))
        
        # Special handling for stores (usually small files)
        if data_type == DataType.STORES:
            # For stores, use a smaller batch size since files are typically small
            optimal_batch_size = min(optimal_batch_size, 100)
        
        # Cap at reasonable maximum to avoid memory issues
        optimal_batch_size = min(optimal_batch_size, 10000)
        
        logger.info(f"Calculated batch size for {data_type.name}: {optimal_batch_size} rows "
                   f"(max row size: {max_row_size} bytes, target: {self.max_batch_size_kb}KB)")
        
        return optimal_batch_size
    
    def send_data_type_files(self, data_type: DataType, data_type_str: str):
        """Send all files for a specific data type using streaming approach"""
        csv_files = self.get_csv_files_by_type(data_type_str)
        
        if not csv_files:
            logger.info(f"No CSV files found for {data_type_str}")
            if self.socket:
                send_eof(self.socket, data_type)
            return
        
        total_rows = 0
        batch_size = self.calculate_optimal_batch_size(data_type)
        
        for csv_file in csv_files:
            logger.info(f"Streaming file: {csv_file}")
            
            # Stream process the file
            file_rows = self.process_csv_file_streaming(csv_file, data_type, batch_size)
            
            if file_rows == 0:
                logger.warning(f"No rows processed from {csv_file}")
                continue
            
            total_rows += file_rows
            logger.info(f"Completed streaming {csv_file}: {file_rows} rows sent")
        
        logger.info(f"Finished streaming {total_rows} total rows for {data_type_str} "
                   f"(batch size: {batch_size} rows)")
        
        # Send EOF for this data type
        if not self.socket:
            logger.error("No socket connection available for EOF")
            return
            
        try:
            send_eof(self.socket, data_type)
            logger.info(f"Sent EOF for {data_type_str}")

            try:
                response_code = receive_response(self.socket)
                if response_code != 0:
                    logger.warning(
                        f"Gateway reported error while acknowledging EOF for {data_type_str}"
                    )
            except Exception as exc:
                logger.error(
                    f"Failed to receive EOF acknowledgement for {data_type_str}: {exc}"
                )

        except Exception as e:
            logger.error(f"Failed to send EOF for {data_type_str}: {e}")
    
    def run(self):
        """Main client execution"""
        try:
            self.connect_to_gateway()
            
            # Send data for each type in order
            data_types = [
                (DataType.USERS, 'users'),
                (DataType.STORES, 'stores'),
                (DataType.MENU_ITEMS, 'menu_items'),
                (DataType.TRANSACTIONS, 'transactions'),
                (DataType.TRANSACTION_ITEMS, 'transaction_items'),
            ]
            
            for data_type, data_type_str in data_types:
                if shutdown_requested:
                    logger.info("Shutdown requested, stopping data transmission")
                    break
                logger.info(f"Starting to send {data_type_str} data")
                self.send_data_type_files(data_type, data_type_str)
            
            logger.info("All data sent successfully")

            # Esperar resultados del gateway sin cerrar la conexión
            self.listen_for_results()
            
        except Exception as e:
            logger.error(f"Error in client execution: {e}")
        finally:
            self.disconnect()

def main():
    """Entry point"""
    # Configurar manejo de SIGTERM
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    # Get config file from command line or use default
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'workers_config.json'
    
    logger.info(f"Starting Coffee Shop Client with config file: {config_file}")
    
    client = CoffeeShopClient(config_file)
    client.run()

if __name__ == "__main__":
    main()
