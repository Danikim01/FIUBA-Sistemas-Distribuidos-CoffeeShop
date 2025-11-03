"""Healthchecker that monitors services and restarts them if they fail."""

import logging
import os
import socket
import subprocess
import threading
import time
from typing import List

logger = logging.getLogger(__name__)

# Protocol constants
HEALTHCHECK_MSG = bytes([1])
HEALTHCHECK_ACK = bytes([2])
MSG_BYTES = 1

# Default configuration
DEFAULT_PORT = 9290
DEFAULT_INTERVAL_MS = 1000
DEFAULT_TIMEOUT_MS = 1500
DEFAULT_MAX_ERRORS = 3


class HealthChecker:
    """Healthchecker that monitors services via UDP and restarts them if they fail."""
    
    def __init__(
        self,
        nodes: List[str],
        port: int = DEFAULT_PORT,
        interval_ms: int = DEFAULT_INTERVAL_MS,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        max_errors: int = DEFAULT_MAX_ERRORS
    ):
        """Initialize the healthchecker.
        
        Args:
            nodes: List of container names to monitor
            port: UDP port to connect to (default: 9290)
            interval_ms: Interval between checks in milliseconds (default: 1000)
            timeout_ms: Timeout for healthcheck response in milliseconds (default: 1500)
            max_errors: Number of consecutive errors before restarting (default: 3)
        """
        self.nodes = nodes
        self.port = port
        self.interval = interval_ms / 1000.0  # Convert to seconds
        self.timeout = timeout_ms / 1000.0  # Convert to seconds
        self.max_errors = max_errors
        self.finished = False
        self.finished_lock = threading.Lock()
        self.shutdown_requested = False  # Flag to prevent restarts during shutdown
        
        logger.info(
            f"HealthChecker initialized - Nodes: {nodes}, Port: {port}, "
            f"Interval: {interval_ms}ms, Timeout: {timeout_ms}ms, MaxErrors: {max_errors}"
        )
    
    def start(self, initial_delay_seconds: int = 3):
        """Start monitoring all nodes in separate threads.
        
        Args:
            initial_delay_seconds: Number of seconds to wait before starting monitoring.
                                  This allows services to start up.
        """
        logger.info(f"Waiting {initial_delay_seconds} seconds for services to start...")
        time.sleep(initial_delay_seconds)
        logger.info("Starting health monitoring...")
        
        threads = []
        for node in self.nodes:
            thread = threading.Thread(target=self._check_node, args=(node,), daemon=True)
            thread.start()
            threads.append(thread)
        
        # Wait for all threads
        for thread in threads:
            thread.join()
    
    def _check_node(self, container_name: str):
        """Continuously check a node's health.
        
        Args:
            container_name: Name of the container to monitor
        """
        conn_err_count = 0
        consecutive_failures = 0
        first_success = False
        
        while True:
            # Check if we should stop
            with self.finished_lock:
                if self.finished:
                    logger.info(f"Stopping healthcheck for {container_name}")
                    return
            
            # Try to connect and check health
            conn = self._connect(container_name)
            if conn is None:
                conn_err_count += 1
                consecutive_failures += 1
                
                # Log with appropriate level
                if conn_err_count == 1:
                    logger.warning(
                        f"Failed to connect to {container_name} "
                        f"(error count: {conn_err_count}/{self.max_errors}) - "
                        f"First failure, may still be starting up. "
                        f"Check worker/gateway logs to verify healthcheck service started."
                    )
                elif conn_err_count < self.max_errors:
                    logger.warning(
                        f"Failed to connect to {container_name} "
                        f"(error count: {conn_err_count}/{self.max_errors})"
                    )
                else:
                    logger.error(
                        f"Failed to connect to {container_name} "
                        f"(error count: {conn_err_count}/{self.max_errors}) - "
                        f"Check if healthcheck service is running on port {self.port}. "
                        f"Verify worker/gateway logs show 'Healthcheck service started'"
                    )
                
                if consecutive_failures >= self.max_errors:
                    logger.error(f"Trying to connect to {container_name}, but it is down, should restart...")
                    self._restart_node(container_name)
                    consecutive_failures = 0  # Reset after restart
                    conn_err_count = 0
                
                time.sleep(self.interval)
                continue
            
            # Reset consecutive failures on successful connection
            if consecutive_failures > 0:
                logger.info(f"Successfully connected to {container_name} after {consecutive_failures} failures")
                consecutive_failures = 0
                first_success = True
            
            # Send healthcheck message and wait for ACK
            logger.info(f"Sending healthcheck message to {container_name}")
            err_count = self._send_healthcheck(conn, container_name)
            
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection to {container_name}: {e}")
            
            # If we failed max_errors times, restart the node
            if err_count >= self.max_errors:
                consecutive_failures += err_count
                if consecutive_failures >= self.max_errors:
                    logger.error(f"Node {container_name} is down, should restart...")
                    self._restart_node(container_name)
                    consecutive_failures = 0
            else:
                # Reset consecutive failures on successful healthcheck
                if err_count == 0:
                    consecutive_failures = 0
                    if not first_success:
                        first_success = True
                        logger.info(f"Healthcheck service responding for {container_name}")
            
            time.sleep(self.interval)
    
    def _connect(self, container_name: str) -> socket.socket | None:
        """Connect to a node via UDP.
        
        Args:
            container_name: Name of the container to connect to
            
        Returns:
            UDP socket connection or None if connection fails
        """
        node_addr = f"{container_name}:{self.port}"
        
        try:
            # Resolve DNS name to IP address
            addr_info = socket.getaddrinfo(container_name, self.port, socket.AF_INET, socket.SOCK_DGRAM)
            if not addr_info:
                logger.warning(f"Could not resolve address for {node_addr}")
                return None
            
            udp_addr = addr_info[0][4]
            logger.debug(f"Resolved {node_addr} to {udp_addr}")
            
        except socket.gaierror as e:
            # DNS resolution error - might be transient
            logger.warning(f"DNS resolution error for {node_addr}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error resolving address {node_addr}: {e}")
            return None
        
        # Try to connect once (retries are handled at higher level)
        try:
            with self.finished_lock:
                if self.finished:
                    return None
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.timeout)
            # In UDP, connect() doesn't actually establish a connection,
            # it just sets the default destination for send/recv
            sock.connect(udp_addr)
            logger.debug(f"Created UDP socket for {node_addr} -> {udp_addr}")
            return sock
        except Exception as e:
            logger.warning(f"Error creating UDP socket for {node_addr}: {e}")
            return None
    
    def _send_healthcheck(self, conn: socket.socket, container_name: str) -> int:
        """Send healthcheck message and wait for ACK.
        
        Args:
            conn: UDP socket connection
            container_name: Name of the container (for logging)
            
        Returns:
            Number of consecutive errors encountered
        """
        err_count = 0
        
        for _ in range(self.max_errors):
            with self.finished_lock:
                if self.finished:
                    return err_count
            
            # Send healthcheck message
            try:
                bytes_sent = conn.send(HEALTHCHECK_MSG)
                logger.debug(
                    f"Sent healthcheck message ({HEALTHCHECK_MSG}) to {container_name} "
                    f"({bytes_sent} bytes)"
                )
            except Exception as e:
                err_count += 1
                logger.error(
                    f"Error sending healthcheck to {container_name}: {e} "
                    f"(error count: {err_count}/{self.max_errors})"
                )
                time.sleep(self.interval)
                continue
            
            # Wait for ACK response
            try:
                conn.settimeout(self.timeout)
                buffer = conn.recv(MSG_BYTES)
                logger.debug(
                    f"Received response from {container_name}: {buffer} "
                    f"(expected: {HEALTHCHECK_ACK})"
                )
                
                if buffer == HEALTHCHECK_ACK:
                    # Success, reset error count
                    err_count = 0
                    logger.info(f"Healthcheck successful for {container_name}")
                    break
                else:
                    err_count += 1
                    logger.warning(
                        f"Unexpected response from {container_name}: {buffer} "
                        f"(expected {HEALTHCHECK_ACK}, got {type(buffer).__name__}) "
                        f"(error count: {err_count}/{self.max_errors})"
                    )
                    
            except socket.timeout:
                err_count += 1
                logger.debug(
                    f"Timeout waiting for ACK from {container_name} "
                    f"(error count: {err_count}/{self.max_errors})"
                )
            except Exception as e:
                err_count += 1
                logger.debug(
                    f"Error receiving ACK from {container_name}: {e} "
                    f"(error count: {err_count}/{self.max_errors})"
                )
            
            time.sleep(self.interval)
        
        return err_count
    
    def _restart_node(self, container_name: str):
        """Restart a node using docker restart.
        
        Args:
            container_name: Name of the container to restart
        """
        # Do not restart nodes during shutdown - they're being stopped gracefully
        if self.shutdown_requested:
            logger.debug(f"Shutdown requested, skipping restart of {container_name}")
            return
        
        logger.error(f"Node {container_name} is down, restarting...")
        
        try:
            # Execute docker restart command
            result = subprocess.run(
                ['docker', 'restart', container_name],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"Node {container_name} restarted successfully")
                if result.stdout:
                    logger.debug(f"Restart output: {result.stdout}")
            else:
                logger.error(
                    f"Failed to restart node {container_name}: "
                    f"return code {result.returncode}, stderr: {result.stderr}"
                )
                
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout while restarting node {container_name}")
        except Exception as e:
            logger.error(f"Error restarting node {container_name}: {e}")
        
        # Wait a bit after restart before checking again
        time.sleep(self.interval)

