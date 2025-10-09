"""Client connection module for gateway communication and protocol handling."""

import socket
import logging
from typing import List, Dict, Any
from protocol import DataType, send_batch, send_eof, send_session_reset, receive_response # type: ignore

logger = logging.getLogger(__name__)


class ClientConnection:
    """Handles socket communication with the gateway."""
    
    def __init__(self, gateway_host: str, gateway_port: int):
        """Initialize client connection.
        
        Args:
            gateway_host: Gateway server hostname
            gateway_port: Gateway server port
        """
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.socket: socket.socket | None = None
    
    def connect(self) -> bool:
        """Establish connection to gateway.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.gateway_host, self.gateway_port))
            logger.info(f"Connected to gateway at {self.gateway_host}:{self.gateway_port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to gateway: {e}")
            self.socket = None
            return False
    
    def disconnect(self) -> None:
        """Close connection to gateway."""
        if self.socket:
            self.socket.close()
            self.socket = None
            logger.info("Disconnected from gateway")
    
    def is_connected(self) -> bool:
        """Check if connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        return self.socket is not None
    
    def send_batch_to_gateway(self, batch: List[Dict[str, Any]], data_type: DataType) -> bool:
        """Send a batch to the gateway and handle response.
        
        Args:
            batch: List of data records to send
            data_type: Type of data being sent
            
        Returns:
            True if batch sent successfully, False otherwise
        """
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
    
    def send_eof_to_gateway(self, data_type: DataType) -> bool:
        """Send EOF message to gateway.
        
        Args:
            data_type: Type of data for which EOF is being sent
            
        Returns:
            True if EOF sent successfully, False otherwise
        """
        if not self.socket:
            logger.error("No socket connection available for EOF")
            return False
            
        try:
            send_eof(self.socket, data_type)
            logger.info(f"Sent EOF for {data_type.name}")

            try:
                response_code = receive_response(self.socket)
                if response_code != 0:
                    logger.warning(
                        f"Gateway reported error while acknowledging EOF for {data_type.name}"
                    )
                    return False
                return True
            except Exception as exc:
                logger.error(
                    f"Failed to receive EOF acknowledgement for {data_type.name}: {exc}"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to send EOF for {data_type.name}: {e}")
            return False
    
    def get_socket(self) -> socket.socket | None:
        """Get the underlying socket for direct access.
        
        Returns:
            Socket object if connected, None otherwise
        """
        return self.socket
    
    def send_session_reset(self) -> bool:
        """Send session reset message to start a new session.
        
        Returns:
            True if session reset sent successfully, False otherwise
        """
        if not self.socket:
            logger.error("No socket connection available for session reset")
            return False
            
        try:
            send_session_reset(self.socket)
            
            # Wait for response
            response_code = receive_response(self.socket)
            if response_code == 0:  # ResponseCode.OK
                logger.info("Session reset successfully")
                return True
            else:
                logger.error(f"Gateway rejected session reset (response code: {response_code})")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send session reset: {e}")
            return False


class DataSender:
    """Handles sending data files to the gateway through client connection."""
    
    def __init__(self, connection: ClientConnection, data_processor):
        """Initialize data sender.
        
        Args:
            connection: Client connection instance
            data_processor: Data processor instance for handling CSV files
        """
        self.connection = connection
        self.data_processor = data_processor
    
    def send_data_type_files(self, data_type: DataType, data_type_str: str) -> bool:
        """Send all files for a specific data type using streaming approach.
        
        Args:
            data_type: DataType enum value
            data_type_str: String representation of data type
            
        Returns:
            True if all files sent successfully, False otherwise
        """
        csv_files = self.data_processor.get_csv_files_by_type(data_type_str)
        
        if not csv_files:
            logger.info(f"No CSV files found for {data_type_str}")
            return self.connection.send_eof_to_gateway(data_type)
        
        total_rows = 0
        batch_size = self.data_processor.calculate_optimal_batch_size(data_type)
        
        for csv_file in csv_files:
            logger.debug(f"Processing file: {csv_file}")
            
            # Process the file into batches
            batches = self.data_processor.process_csv_file_streaming(csv_file, batch_size)
            
            if not batches:
                logger.warning(f"No batches generated from {csv_file}")
                continue
            
            # Send each batch
            file_rows = 0
            for batch in batches:
                if not self.connection.send_batch_to_gateway(batch, data_type):
                    logger.error(f"Failed to send batch from {csv_file}")
                    return False
                file_rows += len(batch)
            
            total_rows += file_rows
            logger.debug(f"Completed processing {csv_file}: {file_rows} rows sent")
        
        logger.info(f"Finished processing {total_rows} total rows for {data_type_str} "
                   f"(batch size: {batch_size} rows)")
        
        # Send EOF for this data type
        return self.connection.send_eof_to_gateway(data_type)
    
    def send_all_data_types(self) -> bool:
        """Send all data types in the correct order.
        
        Returns:
            True if all data sent successfully, False otherwise
        """
        data_types = self.data_processor.get_data_types_info()
        
        for data_type, data_type_str in data_types:
            logger.info(f"Starting to send {data_type_str} data")
            if not self.send_data_type_files(data_type, data_type_str):
                logger.error(f"Failed to send {data_type_str} data")
                return False
        
        logger.info("All data sent successfully")
        return True