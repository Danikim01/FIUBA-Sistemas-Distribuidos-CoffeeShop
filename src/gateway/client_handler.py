"""Client handler module for managing client connections and communication."""

import logging
import socket
import threading
import json
from typing import Any

from protocol import DataType, MessageType, receive_message, send_response # type: ignore
from message_handlers import MessageHandlers
from queue_manager import QueueManager
from client_session import ClientSessionManager

logger = logging.getLogger(__name__)


class ClientHandler:
    """Handles individual client connections with multi-client support."""
    
    def __init__(self, queue_manager: QueueManager):
        self.queue_manager = queue_manager
        self.message_handlers = MessageHandlers(queue_manager)
        self.session_manager = ClientSessionManager()
    
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
                            
                            # Check if all EOFs have been received for this client
                            if all_eof_received:
                                logger.info(f"All EOFs received for client {client_id}, starting result forwarding")
                                self._forward_results_to_client(client_socket, client_id)
                                break

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
        finally:
            # Clean up client session
            self.session_manager.cleanup_session(client_id)
            logger.info(f"Connection with client {client_id} from {address} closed")
    
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

        results_queue = self.queue_manager.create_results_consumer()
        eof_sent = False

        def handle_payload(payload: Any) -> None:
            nonlocal eof_sent
            if isinstance(payload, list):
                for item in payload:
                    handle_payload(item)
                return

            # Check if this is an EOF message
            if self.message_handlers.is_eof_message(payload):
                # Check if this EOF is for our specific client
                if payload.get('client_id') == client_id:
                    logger.info(f"Received EOF from results queue for client {client_id}; notifying client")
                    try:
                        self._send_json_line(client_socket, {'type': 'EOF'})
                        eof_sent = True
                    except Exception as exc:
                        logger.error(f"Failed to send EOF to client {client_id}: {exc}")
                    finally:
                        results_queue.stop_consuming()
                return

            # Check if this result is for our specific client
            if isinstance(payload, dict) and payload.get('client_id') == client_id:
                try:
                    # Remove client_id from the payload before sending to client
                    result_payload = {k: v for k, v in payload.items() if k != 'client_id'}
                    logger.info(f"Gateway sending result to client {client_id}: {result_payload}")
                    self._send_json_line(client_socket, result_payload)
                except Exception as exc:
                    logger.error(f"Failed to forward result to client {client_id}: {exc}")
                    results_queue.stop_consuming()

        def on_message(message: Any) -> None:
            try:
                logger.debug(f"Gateway received message from results queue: {type(message)} - {message}")
                handle_payload(message)
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Unexpected error forwarding results for client {client_id}: {exc}")
                results_queue.stop_consuming()

        try:
            results_queue.start_consuming(on_message)
        except KeyboardInterrupt:
            logger.info(f"Results forwarding interrupted for client {client_id}")
        except Exception as exc:
            logger.error(f"Error while consuming results queue for client {client_id}: {exc}")
        finally:
            results_queue.close()
            if not eof_sent:
                try:
                    self._send_json_line(client_socket, {'type': 'EOF'})
                except Exception:
                    pass