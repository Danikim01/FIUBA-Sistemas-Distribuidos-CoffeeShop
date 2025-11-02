"""UDP healthcheck service that responds to health check requests."""

import logging
import os
import socket
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# Protocol constants
HEALTHCHECK_REQUEST = bytes([1])
HEALTHCHECK_ACK = bytes([2])
MSG_BYTES = 1
DEFAULT_PORT = 9290


class HealthcheckService:
    """UDP service that responds to health check requests.
    
    Listens on a configurable UDP port and responds with ACK when receiving
    health check messages from the healthchecker.
    """
    
    def __init__(self, port: Optional[int] = None):
        """Initialize the healthcheck service.
        
        Args:
            port: UDP port to listen on. Defaults to HEALTHCHECK_PORT env var or 9290.
        """
        self.port = port or int(os.getenv('HEALTHCHECK_PORT', DEFAULT_PORT))
        self.socket: Optional[socket.socket] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
    def start(self):
        """Start the healthcheck service in a background thread."""
        with self._lock:
            if self.running:
                logger.warning("Healthcheck service already running")
                return
                
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.bind(('0.0.0.0', self.port))
                self.socket.settimeout(1.0)  # Timeout for periodic checks
                self.running = True
                
                # Log the actual bound address
                bound_addr = self.socket.getsockname()
                logger.info(f"Healthcheck service socket bound to {bound_addr}")
                
                self.thread = threading.Thread(target=self._listen_loop, daemon=True)
                self.thread.start()
                
                logger.info(f"Healthcheck service started on port {self.port}")
            except OSError as e:
                if e.errno == 98:  # Address already in use
                    logger.error(f"Port {self.port} is already in use. Another healthcheck service may be running.")
                else:
                    logger.error(f"Failed to bind healthcheck service to port {self.port}: {e}")
                self.running = False
                raise
            except Exception as e:
                logger.error(f"Failed to start healthcheck service: {e}", exc_info=True)
                self.running = False
                raise
    
    def _listen_loop(self):
        """Main listening loop for health check requests."""
        logger.info(f"Healthcheck service listening loop started on port {self.port}")
        while self.running:
            try:
                if not self.socket:
                    logger.error("Healthcheck socket is None, stopping listen loop")
                    break
                    
                data, addr = self.socket.recvfrom(MSG_BYTES)
                logger.info(f"Received healthcheck request from {addr}: {data} (expected: {HEALTHCHECK_REQUEST})")
                
                if data == HEALTHCHECK_REQUEST:
                    # Send ACK response
                    try:
                        bytes_sent = self.socket.sendto(HEALTHCHECK_ACK, addr)
                        logger.info(f"Healthcheck ACK ({HEALTHCHECK_ACK}) sent to {addr} ({bytes_sent} bytes)")
                    except Exception as e:
                        logger.error(f"Error sending healthcheck ACK to {addr}: {e}", exc_info=True)
                else:
                    logger.warning(
                        f"Received unexpected message from {addr}: {data} "
                        f"(expected {HEALTHCHECK_REQUEST}, got {type(data).__name__})"
                    )
                    
            except socket.timeout:
                # Timeout is expected for periodic checks
                continue
            except OSError as e:
                if self.running:
                    logger.error(f"Socket error in healthcheck service: {e}")
                break
            except Exception as e:
                if self.running:
                    logger.error(f"Unexpected error in healthcheck service: {e}")
                    continue
        
        logger.info("Healthcheck service listening loop stopped")
    
    def stop(self):
        """Stop the healthcheck service."""
        with self._lock:
            if not self.running:
                return
                
            self.running = False
            
            if self.socket:
                try:
                    self.socket.close()
                except Exception as e:
                    logger.warning(f"Error closing healthcheck socket: {e}")
                self.socket = None
            
            if self.thread:
                self.thread.join(timeout=2.0)
                
            logger.info("Healthcheck service stopped")
    
    def close(self):
        """Alias for stop() for compatibility."""
        self.stop()

