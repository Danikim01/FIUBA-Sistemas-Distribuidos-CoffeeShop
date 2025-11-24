"""Client handler module for managing client connections and communication."""

import logging
import socket
import threading
import json
from typing import Any

from protocol import DataType, MessageType, receive_message, send_response  # type: ignore
from message_handlers import MessageHandlers
from queue_manager import QueueManager
from client_session import ClientSessionManager
from result_normalizer import normalize_queue_message

logger = logging.getLogger(__name__)


class ClientHandler:
    """Handles individual client connections with multi-client support."""
    
    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        self.message_handlers = MessageHandlers(queue_manager)
        self.session_manager = ClientSessionManager()
        # Track seen transaction_ids per client for AMOUNT_FILTER_TRANSACTIONS deduplication
        self._seen_transaction_ids: dict[str, set[str]] = {}
    
    def handle_client(self, client_socket: socket.socket, address: tuple, shutdown_event: threading.Event) -> None:
        """Handle a client connection with unique identification and result routing."""
        # Create a new client session with unique ID
        client_id = self.session_manager.create_session(client_socket, address)
        
        try:
            logger.info(f"Starting to handle client {client_id} from {address}")
            
            while not shutdown_event.is_set():
                try:
                    # Receive message
                    message_type, message_data = receive_message(client_socket)
                    
                    if message_type == MessageType.BATCH:
                        self.message_handlers.handle_batch_message(client_socket, message_data, client_id)
                        
                    elif message_type == MessageType.SESSION_RESET:
                        # Create a completely new session for this connection
                        new_client_id = self.session_manager.create_session(client_socket, address)
                        
                        # Remove the old session
                        self.session_manager.remove_session(client_id)
                        
                        # Update to use the new client ID
                        client_id = new_client_id
                        
                        # Send success response
                        self._send_response_safe(client_socket, True)
                        logger.info(f"Created new session with client ID: {client_id}")
                        
                    elif message_type == MessageType.EOF:
                        session = self.session_manager.get_session(client_id)
                        if not session:
                            logger.error(f"Session not found for client {client_id}")
                            break
                            
                        # Convert string keys to DataType enum values
                        eof_state = {DataType[k]: v for k, v in session.eof_received.items()}
                        data_type, newly_marked = self.message_handlers.handle_eof_message(
                            client_socket,
                            message_data,
                            eof_state,
                            client_id
                        )

                        # Update session EOF status
                        if newly_marked and data_type:
                            all_eof_received = self.session_manager.update_eof_status(
                                client_id, 
                                data_type.name, 
                                True
                            )
                            
                            if all_eof_received:
                                logger.info(f"All EOFs received for client {client_id}, starting result forwarding")
                                self._forward_results_to_client(client_socket, client_id)
                                # Continue the loop to wait for session reset or disconnect
                                # Don't break here to allow for new sessions
                            else:
                                logger.debug(f"EOF received for {data_type.name} from client {client_id}")

                        else:
                            logger.warning(f"Unknown message type: {message_type} from client {client_id}")
                            self._send_response_safe(client_socket, False)
                        
                except ConnectionError:
                    logger.info(f"Client {client_id} from {address} disconnected")
                    break
                except Exception as e:
                    logger.error(f"Error handling message from client {client_id} at {address}: {e}")
                    self._send_response_safe(client_socket, False)
                    break
                    
        except Exception as e:
            logger.error(f"Error in client handler for {client_id} at {address}: {e}")
        # finally:
            # Clean up client session
            # self.session_manager.cleanup_session(client_id)
            # logger.info(f"Connection with client {client_id} from {address} closed")
    
    def _send_response_safe(self, client_socket: socket.socket, success: bool) -> None:
        """Safely send response to client."""
        try:
            send_response(client_socket, success)
        except Exception as e:
            logger.error(f"Failed to send response: {e}")
    
    def _send_json_line(self, client_socket: socket.socket, payload: Any) -> None:
        """Send JSON payload as a line to the client."""
        message = json.dumps(payload, ensure_ascii=False) + '\n'
        client_socket.sendall(message.encode('utf-8'))

    def _forward_results_to_client(self, client_socket: socket.socket, client_id: str) -> None:
        """Consume results from RabbitMQ and forward them to the specific client via TCP."""
        logger.info(f"Forwarding results to client {client_id}")

        # Initialize seen transaction_ids set for this client
        seen_transaction_ids = set()
        self._seen_transaction_ids[client_id] = seen_transaction_ids
        
        eof_sent = False

        def handle_payload(payload: Any) -> None:
            nonlocal eof_sent

            logger.debug(f"Handling payload for client {client_id}: {payload}")

            if isinstance(payload, list):
                for item in payload:
                    handle_payload(item)
                return

            if self.message_handlers.is_eof_message(payload):
                logger.info(f"Received EOF from results stream for client {client_id}; notifying client")
                try:
                    self._send_json_line(client_socket, {'type': 'EOF', 'client_id': client_id})
                    eof_sent = True
                except Exception as exc:
                    logger.error(f"Failed to send EOF to client {client_id}: {exc}")
                return

            if not isinstance(payload, dict):
                logger.debug(
                    "Ignoring results payload of type %s for client %s",
                    type(payload),
                    client_id,
                )
                return

            try:
                logger.info(f"Gateway payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'Not a dict'}")
                logger.info(f"Gateway payload type_metadata: {payload.get('type_metadata', 'NO_METADATA')}")
                normalized_messages = normalize_queue_message(payload)
                logger.info(f"Gateway normalized messages for client {client_id}: {normalized_messages}")
                
                if not normalized_messages:
                    logger.warning(
                        "Discarding unrecognized result payload for client %s: %s",
                        client_id,
                        payload,
                    )
                    return

                for result_payload in normalized_messages:
                    enriched_payload = dict(result_payload)
                    enriched_payload['client_id'] = client_id
                    
                    # Deduplicate AMOUNT_FILTER_TRANSACTIONS by transaction_id across all batches
                    result_type = enriched_payload.get('type', '').upper()
                    if result_type == 'AMOUNT_FILTER_TRANSACTIONS':
                        results = enriched_payload.get('results', [])
                        if isinstance(results, list) and results:
                            # Deduplicate by transaction_id, keeping first occurrence across all batches
                            deduplicated = []
                            duplicates_count = 0
                            for transaction in results:
                                if isinstance(transaction, dict):
                                    transaction_id = transaction.get('transaction_id')
                                    if transaction_id is not None:
                                        transaction_id_str = str(transaction_id)
                                        if transaction_id_str not in seen_transaction_ids:
                                            seen_transaction_ids.add(transaction_id_str)
                                            deduplicated.append(transaction)
                                        else:
                                            duplicates_count += 1
                                    else:
                                        # If no transaction_id, include it (shouldn't happen but handle gracefully)
                                        deduplicated.append(transaction)
                                else:
                                    deduplicated.append(transaction)
                            
                            enriched_payload['results'] = deduplicated
                            if duplicates_count > 0:
                                logger.info(
                                    "\033[1;91m" 
                                    "GATEWAY DEDUPLICATED %s DUPLICATE TRANSACTIONS for client %s (TYPE: %s). "
                                    "ORIGINAL COUNT: %s, DEDUPLICATED COUNT: %s"
                                    "\033[0m",
                                    duplicates_count,
                                    client_id,
                                    result_type,
                                    len(results),
                                    len(deduplicated)
                                )
                    
                    logger.info(
                        "Gateway sending normalized result to client %s: type=%s, results_count=%s",
                        client_id,
                        enriched_payload.get('type'),
                        len(enriched_payload.get('results', [])) if isinstance(enriched_payload.get('results'), list) else 'N/A'
                    )
                    self._send_json_line(client_socket, enriched_payload)
            except Exception as exc:
                logger.error(f"Failed to forward result to client {client_id}: {exc}")

        try:
            for message in self.queue_manager.iter_client_results(client_id):
                try:
                    handle_payload(message)
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"Unexpected error forwarding results for client {client_id}: {exc}")
        except KeyboardInterrupt:
            logger.info(f"Results forwarding interrupted for client {client_id}")
        except Exception as exc:
            logger.error(f"Error while consuming results stream for client {client_id}: {exc}")
        finally:
            # Clean up seen transaction_ids for this client
            self._seen_transaction_ids.pop(client_id, None)
            if not eof_sent:
                try:
                    self._send_json_line(client_socket, {'type': 'EOF', 'client_id': client_id})
                except Exception:
                    pass
