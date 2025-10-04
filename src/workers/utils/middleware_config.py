"""Worker configuration management."""

import os
from typing import Union

from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue

class MiddlewareConfig:
    """Configuration class for worker settings."""
    
    def __init__( self ):
        """Initialize worker configuration.
        
        Args:
            input_queue: Input queue name (required)
            output_exchange: Output exchange name for fanout (optional)
            output_queue: Output queue name for direct sending (optional)
        """
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))

        self.input_queue = os.getenv('INPUT_QUEUE', '').strip()
        self.input_exchange = os.getenv('INPUT_EXCHANGE', '').strip()

        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', '').strip()
        self.output_queue = os.getenv('OUTPUT_QUEUE', '').strip()

        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', '10'))

        
    def get_input_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_input_exchange():
            return RabbitMQMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.input_exchange,
                route_keys=[self.input_exchange],
                exchange_type='direct',
                port=self.rabbitmq_port
            )
        
        return RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count
        )

    def get_output_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_output_exchange():
            return RabbitMQMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.output_exchange,
                route_keys=[self.output_exchange],
                exchange_type='direct',
                port=self.rabbitmq_port
            )
        
        return RabbitMQMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=self.output_queue,
                port=self.rabbitmq_port
            )
    
    def get_input_target(self) -> str:
        if self.has_input_exchange():
            return f"exchange:{self.input_exchange}"
        return f"queue:{self.input_queue}"
    
    def get_output_target(self) -> str:
        if self.has_output_exchange():
            return f"exchange:{self.output_exchange}"
        return f"queue:{self.output_queue}"

    def has_input_exchange(self) -> bool:
        return self.input_exchange != ''
    
    def has_output_exchange(self) -> bool:
        return self.output_exchange != ''
    