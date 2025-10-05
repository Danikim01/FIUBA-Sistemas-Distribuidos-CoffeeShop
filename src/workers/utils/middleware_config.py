"""Worker configuration management."""

import os
from typing import Optional, Union

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

        self.input_middleware = self._create_input_middleware()
        self.output_middleware = self._create_output_middleware()

    def _create_input_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_input_exchange():
            queue_override = self.input_queue if self.input_queue else None
            return self.create_exchange(
                self.input_exchange,
                queue_name=queue_override,
                prefetch_count=self.prefetch_count,
            )
        return self.create_queue(self.input_queue)

    def _create_output_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_output_exchange():
            return self.create_exchange(self.output_exchange)
        return self.create_queue(self.output_queue)

    def create_exchange(
        self,
        name: str,
        *, # Esta cosa rara hace que los siguientes argumentos sean solo por nombre
        queue_name: Optional[str] = None,
        prefetch_count: Optional[int] = None,
    ) -> RabbitMQMiddlewareExchange:
        return RabbitMQMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=name,
            route_keys=[name],
            exchange_type='direct',
            port=self.rabbitmq_port,
            queue_name=queue_name,
            prefetch_count=prefetch_count if prefetch_count is not None else self.prefetch_count,
        )

    def create_queue(self, name: str) -> RabbitMQMiddlewareQueue:
        return RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=name,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count
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
    
    def cleanup(self) -> None:
        """Clean up middleware connections."""
        if self.input_middleware:
            self.input_middleware.close()
        if self.output_middleware:
            self.output_middleware.close()
