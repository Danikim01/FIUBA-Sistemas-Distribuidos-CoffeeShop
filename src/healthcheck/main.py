"""Entry point for the healthchecker service."""

import logging
import os
import signal
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from healthcheck.healthchecker import HealthChecker, DEFAULT_PORT, DEFAULT_INTERVAL_MS, DEFAULT_TIMEOUT_MS, DEFAULT_MAX_ERRORS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global healthchecker instance for signal handlers
healthchecker_instance = None


def handle_signal(signum, frame):
    """Handle SIGTERM/SIGINT signals for graceful shutdown."""
    logger.info("Received shutdown signal, shutting down...")
    global healthchecker_instance
    if healthchecker_instance:
        with healthchecker_instance.finished_lock:
            healthchecker_instance.finished = True


def main():
    """Main entry point for the healthchecker."""
    global healthchecker_instance
    
    try:
        # Configure signal handlers (must be in main thread)
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
        
        # Load configuration from environment variables
        port = int(os.getenv('HEALTHCHECK_PORT', DEFAULT_PORT))
        interval_ms = int(os.getenv('HEALTHCHECK_INTERVAL_MS', DEFAULT_INTERVAL_MS))
        timeout_ms = int(os.getenv('HEALTHCHECK_TIMEOUT_MS', DEFAULT_TIMEOUT_MS))
        max_errors = int(os.getenv('HEALTHCHECK_MAX_ERRORS', DEFAULT_MAX_ERRORS))
        initial_delay = int(os.getenv('HEALTHCHECK_INITIAL_DELAY_SECONDS', '10'))
        
        # Get list of nodes to check from environment
        nodes_str = os.getenv('NODES_TO_CHECK', '')
        if not nodes_str:
            logger.error("NODES_TO_CHECK environment variable is required")
            sys.exit(1)
        
        nodes = [node.strip() for node in nodes_str.split() if node.strip()]
        if not nodes:
            logger.error("NODES_TO_CHECK must contain at least one node name")
            sys.exit(1)
        
        logger.info(f"Starting healthchecker with nodes: {nodes}")
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

