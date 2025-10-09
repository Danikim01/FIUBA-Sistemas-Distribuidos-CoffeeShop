import os
import logging
import sys
import signal
from typing import Any

from config import ClientConfig
from data_processor import DataProcessor
from results_handler import ResultsHandler
from client_connection import ClientConnection, DataSender

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

class CoffeeShopClient:
    """Main client class that orchestrates the coffee shop data processing."""
    
    def __init__(self, config_file: str = 'workers_config.json'):
        """Initialize the coffee shop client.
        
        Args:
            config_file: Path to the configuration file
        """
        # Initialize configuration
        self.config = ClientConfig(config_file)
        
        # Initialize data processor
        self.data_processor = DataProcessor(
            self.config.data_dir,
            self.config.max_batch_size_kb
        )
        
        # Initialize results handler
        self.results_handler = ResultsHandler()
        
        # Initialize connection
        gateway_host, gateway_port = self.config.get_gateway_address()
        self.connection = ClientConnection(gateway_host, gateway_port)
        
        # Initialize data sender
        self.data_sender = DataSender(self.connection, self.data_processor)
        
        logger.info("Coffee Shop Client initialized successfully")
    
    def run(self):
        """Main client execution with continuous sessions."""
        global shutdown_requested
        
        try:
            # Connect to gateway
            if not self.connection.connect():
                logger.error("Failed to connect to gateway")
                return False
            
            session_count = 0
            max_sessions = int(os.getenv('MAX_SESSIONS', '10'))  # Default to 10 sessions
            
            logger.info(f"Starting continuous mode with {max_sessions} sessions")
            
            while session_count < max_sessions and not shutdown_requested:
                session_count += 1
                logger.info(f"=== Starting session {session_count}/{max_sessions} ===")
                
                # Check for shutdown before sending data
                if shutdown_requested:
                    logger.info("Shutdown requested, stopping session")
                    break
                
                # Send all data types
                if not self.data_sender.send_all_data_types():
                    logger.error("Failed to send all data")
                    return False
                
                logger.info("All data sent successfully")

                # Check for shutdown before processing results
                if shutdown_requested:
                    logger.info("Shutdown requested, skipping results processing")
                    break

                # Wait for and process results from gateway
                self.results_handler.process_results_stream(self.connection.get_socket())
                
                logger.info(f"Session {session_count} completed successfully")
                
                # Check for shutdown before starting next session
                if shutdown_requested:
                    logger.info("Shutdown requested, not starting next session")
                    break
                
                # If not the last session, reset for next session
                if session_count < max_sessions:
                    logger.info(f"Preparing for session {session_count + 1}")
                    
                    # Send session reset message to start a new session
                    if not self.connection.send_session_reset():
                        logger.error("Failed to reset session")
                        break
                    
                    # Reset results handler state for new session
                    self.results_handler.reset_session_state()
                    
                    logger.info("Session reset successful, starting next session")
                else:
                    logger.info("All sessions completed")
            
            if shutdown_requested:
                logger.info("Client shutdown due to SIGTERM")
                
            return True
            
        except KeyboardInterrupt:
            logger.info("Client interrupted by user")
            return True
        except Exception as e:
            logger.error(f"Error in client execution: {e}")
            return False
        finally:
            logger.info("Closing client connection")
            self.connection.disconnect()

def main():
    """Entry point for the Coffee Shop Client application."""
    # Configure SIGTERM handling
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    # Get config file from command line or use default
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'workers_config.json'
    
    logger.info(f"Starting Coffee Shop Client with config file: {config_file}")
    
    try:
        # Initialize and run client
        client = CoffeeShopClient(config_file)
        success = client.run()
        
        if success:
            logger.info("Client completed successfully")
            sys.exit(0)
        else:
            logger.error("Client execution failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Client interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
