"""Configuration module for the Coffee Shop Gateway."""

import os
from typing import List


class GatewayConfig:
    """Configuration class for gateway settings."""
    
    def __init__(self):
        # Server configuration
        self.port = int(os.getenv('GATEWAY_PORT', 12345))
        
        # RabbitMQ configuration
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        
        # Queue names
        self.transactions_queue_name = os.getenv('OUTPUT_QUEUE', 'transactions_raw')
        self.stores_queue_name = os.getenv('STORES_QUEUE', 'stores_raw')
        self.users_queue_name = os.getenv('USERS_QUEUE', 'users_raw')
        self.transaction_items_queue_name = os.getenv('TRANSACTION_ITEMS_QUEUE', 'transaction_items_raw')
        self.menu_items_queue_name = os.getenv('MENU_ITEMS_QUEUE', 'menu_items_raw')
        self.results_queue_name = os.getenv('RESULTS_QUEUE', 'gateway_results')
        
        # Multiple stores queues configuration
        self.stores_queue_names = self._parse_stores_queues()
        
        # Processing configuration
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 100))
    
    def _parse_stores_queues(self) -> List[str]:
        """Parse and validate stores queue names from environment variables."""
        raw_stores_queues = os.getenv('STORES_QUEUES', '')
        
        if raw_stores_queues:
            stores_queue_names = [name.strip() for name in raw_stores_queues.split(',') if name.strip()]
        else:
            stores_queue_names = []

        if not stores_queue_names:
            stores_queue_names = [self.stores_queue_name]
        elif self.stores_queue_name and self.stores_queue_name not in stores_queue_names:
            stores_queue_names.insert(0, self.stores_queue_name)

        return stores_queue_names
    
    def get_rabbitmq_connection_params(self) -> dict:
        """Get RabbitMQ connection parameters as a dictionary."""
        return {
            'host': self.rabbitmq_host,
            'port': self.rabbitmq_port
        }