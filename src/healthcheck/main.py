"""Entry point for the healthchecker service."""

import logging
import os
import signal
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from healthcheck.healthchecker import HealthChecker, DEFAULT_PORT, DEFAULT_INTERVAL_MS, DEFAULT_TIMEOUT_MS, DEFAULT_MAX_ERRORS
from healthcheck.distribute_nodes import get_next_healthchecker, get_healthchecker_name
from healthcheck.service import HealthcheckService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global healthchecker instance and healthcheck service for signal handlers
healthchecker_instance = None
healthcheck_service = None


def handle_signal(signum, frame):
    """Handle SIGTERM/SIGINT signals for graceful shutdown."""
    logger.info("Received shutdown signal, shutting down...")
    global healthchecker_instance, healthcheck_service
    if healthchecker_instance:
        # Set shutdown_requested flag to prevent restarts during shutdown
        healthchecker_instance.shutdown_requested = True
        with healthchecker_instance.finished_lock:
            healthchecker_instance.finished = True
    if healthcheck_service:
        # Stop healthcheck service
        try:
            healthcheck_service.stop()
        except Exception as e:
            logger.warning(f"Error stopping healthcheck service: {e}")


def main():
    """Main entry point for the healthchecker."""
    global healthchecker_instance, healthcheck_service
    
    try:
        # Configure signal handlers (must be in main thread)
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
        
        # Load configuration from environment variables
        port = int(os.getenv('HEALTHCHECK_PORT', DEFAULT_PORT))
        interval_ms = int(os.getenv('HEALTHCHECK_INTERVAL_MS', DEFAULT_INTERVAL_MS))
        timeout_ms = int(os.getenv('HEALTHCHECK_TIMEOUT_MS', DEFAULT_TIMEOUT_MS))
        max_errors = int(os.getenv('HEALTHCHECK_MAX_ERRORS', DEFAULT_MAX_ERRORS))
        initial_delay = 3  # Fixed delay: wait 3 seconds for services to start before monitoring

        # Get healthchecker ID and total healthcheckers for Ring topology
        healthchecker_id = int(os.getenv('HEALTHCHECKER_ID', '1'))
        total_healthcheckers = int(os.getenv('TOTAL_HEALTHCHECKERS', '1'))
        
        # Get list of nodes to check from environment
        nodes_str = os.getenv('NODES_TO_CHECK', '')
        if not nodes_str:
            logger.error("NODES_TO_CHECK environment variable is required")
            sys.exit(1)
        
        nodes = [node.strip() for node in nodes_str.split() if node.strip()]
        if not nodes:
            logger.error("NODES_TO_CHECK must contain at least one node name")
            sys.exit(1)
        
        # Add next healthchecker in the ring to monitored nodes (if multiple healthcheckers)
        if total_healthcheckers > 1:
            next_hc_id = get_next_healthchecker(healthchecker_id, total_healthcheckers)
            next_hc_name = get_healthchecker_name(next_hc_id)
            nodes.append(next_hc_name)
            logger.info(f"Ring topology: monitoring next healthchecker {next_hc_name} in ring")
        
        logger.info(f"Starting healthchecker-{healthchecker_id} (total: {total_healthcheckers}) with nodes: {nodes}")
        logger.info(f"Configuration - Port: {port}, Interval: {interval_ms}ms, "
                   f"Timeout: {timeout_ms}ms, MaxErrors: {max_errors}, "
                   f"InitialDelay: {initial_delay}s")
        
        # Create healthchecker
        healthchecker_instance = HealthChecker(
            nodes=nodes,
            port=port,
            interval_ms=interval_ms,
            timeout_ms=timeout_ms,
            max_errors=max_errors
        )
        
        # Start healthcheck service so other healthcheckers can monitor this one
        # (Only needed when there are multiple healthcheckers)
        if total_healthcheckers > 1:
            try:
                global healthcheck_service
                healthcheck_service = HealthcheckService(port=port)
                healthcheck_service.start()
                logger.info(f"Healthcheck service started on port {port} for monitoring by other healthcheckers")
            except Exception as e:
                logger.warning(f"Failed to start healthcheck service for monitoring: {e}")
                healthcheck_service = None
        
        # Start monitoring with initial delay
        healthchecker_instance.start(initial_delay_seconds=initial_delay)
        
    except KeyboardInterrupt:
        logger.info("Healthchecker interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in healthchecker: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

