#!/usr/bin/env python3
"""
Test script to demonstrate continuous client sessions without reconnecting.
This script simulates a client that can send data in multiple sessions.
"""

import sys
import os
import logging
import time

# Add the client source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'client'))

from config import ClientConfig
from data_processor import DataProcessor
from results_handler import ResultsHandler
from client_connection import ClientConnection, DataSender

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_continuous_sessions():
    """Test the continuous session functionality."""
    
    # Initialize configuration
    config = ClientConfig('workers_config.json')
    
    # Initialize data processor
    data_processor = DataProcessor(
        config.data_dir,
        config.max_batch_size_kb
    )
    
    # Initialize results handler
    results_handler = ResultsHandler()
    
    # Initialize connection
    gateway_host, gateway_port = config.get_gateway_address()
    connection = ClientConnection(gateway_host, gateway_port)
    
    # Initialize data sender
    data_sender = DataSender(connection, data_processor)
    
    try:
        # Connect to gateway
        logger.info("Connecting to gateway...")
        if not connection.connect():
            logger.error("Failed to connect to gateway")
            return False
        
        session_count = 0
        max_sessions = 3  # Limit for testing
        
        while session_count < max_sessions:
            session_count += 1
            logger.info(f"=== Starting session {session_count} ===")
            
            # Send all data types
            logger.info("Sending data...")
            if not data_sender.send_all_data_types():
                logger.error("Failed to send all data")
                return False
            
            logger.info("All data sent successfully, waiting for results...")

            # Wait for and process results from gateway
            results_handler.process_results_stream(connection.get_socket())
            
            logger.info(f"Session {session_count} completed successfully")
            
            # If not the last session, reset
            if session_count < max_sessions:
                logger.info("Resetting session for next round...")
                
                # Send session reset message to start a new session
                if not connection.send_session_reset():
                    logger.error("Failed to reset session")
                    break
                
                # Reset results handler state for new session
                results_handler.reset_session_state()
                
                logger.info("Session reset successful, will start new session in 2 seconds")
                time.sleep(2)  # Brief pause between sessions
        
        logger.info("All sessions completed successfully!")
        return True
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return True
    except Exception as e:
        logger.error(f"Error in test: {e}")
        return False
    finally:
        connection.disconnect()
        logger.info("Connection closed")

if __name__ == "__main__":
    logger.info("Starting continuous sessions test...")
    success = test_continuous_sessions()
    
    if success:
        logger.info("Test completed successfully!")
        sys.exit(0)
    else:
        logger.error("Test failed!")
        sys.exit(1)