import os
import socket
import logging
import threading
import signal
import sys

# Local imports
from config import GatewayConfig
from queue_manager import QueueManager
from client_handler import ClientHandler


from healthcheck.service import HealthcheckService


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CoffeeShopGateway:
    def __init__(self):
        # Initialize configuration
        self.config = GatewayConfig()

        logger.setLevel(self.config.log_level)
        
        # Initialize components
        self.queue_manager = QueueManager(self.config)
        self.client_handler = ClientHandler(self.queue_manager)
        
        # Server state
        self.socket = None
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Initialize healthcheck service
        self.healthcheck_service = None
        try:
            self.healthcheck_service = HealthcheckService()
            self.healthcheck_service.start()
            logger.info("Gateway started healthcheck service")
        except Exception as e:
            logger.error(f"Failed to start healthcheck service: {e}")
        
        # Configure signal handling
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        logger.info(f"Gateway initialized on port {self.config.port}")

    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown"""
        logger.info("SIGTERM received, initiating graceful shutdown...")
        self.shutdown_event.set()
        self.running = False
    
    def start_server(self):
        """Start the gateway server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(('0.0.0.0', self.config.port))  # Listen on all interfaces for Docker
            self.socket.listen(5)
            self.running = True
            
            logger.info(f"Gateway server started on port {self.config.port}")
            
            while self.running and not self.shutdown_event.is_set():
                try:
                    # Use timeout to check shutdown_event periodically
                    self.socket.settimeout(1.0)
                    client_socket, address = self.socket.accept()
                    logger.info(f"New client connected from {address}")
                    
                    # Handle client in a separate thread
                    client_thread = threading.Thread(
                        target=self.client_handler.handle_client,
                        args=(client_socket, address, self.shutdown_event)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except socket.timeout:
                    # Normal timeout, continue loop
                    continue
                except Exception as e:
                    if self.running and not self.shutdown_event.is_set():
                        logger.error(f"Error accepting connection: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
        finally:
            self.stop_server()
    
    def stop_server(self):
        """Stop the gateway server"""
        self.running = False
        self.shutdown_event.set()
        
        # Stop healthcheck service
        if self.healthcheck_service:
            try:
                self.healthcheck_service.stop()
            except Exception as e:
                logger.warning(f"Error stopping healthcheck service: {e}")
        
        try:
            self.queue_manager.close()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Error closing queue manager: %s", exc)
        if self.socket:
            self.socket.close()
            logger.info("Gateway server stopped")

def main():
    """Entry point"""
    gateway = CoffeeShopGateway()
    
    try:
        gateway.start_server()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        gateway.stop_server()

if __name__ == "__main__":
    main()
