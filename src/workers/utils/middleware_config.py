"""Worker configuration management."""

import logging
import os
from typing import Optional, Union

from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue

logger = logging.getLogger(__name__)

class MiddlewareConfig:
    """Configuration class for worker settings."""
    
    def __init__( self ):
        """Initialize worker configuration.
        
        Args:
            input_queue: Input queue name (required)
            output_exchange: Output exchange name for fanout (optional)
            output_queue: Output queue name for direct sending (optional)
        """
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))

        self.input_queue = os.getenv('INPUT_QUEUE', '').strip()
        self.input_exchange = os.getenv('INPUT_EXCHANGE', '').strip()

        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', '').strip()
        self.output_queue = os.getenv('OUTPUT_QUEUE', '').strip()

        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', '10'))

        self.is_sharded_worker = self._is_sharded_worker()

        self.input_middleware = self._create_input_middleware()
        self.output_middleware = self._create_output_middleware()

    def _create_input_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_input_exchange():
            queue_override = self.input_queue if self.input_queue else None
            
            # Check if this is a sharded worker that needs specific routing keys
            if self._is_sharded_worker():
                worker_id = int(os.getenv('WORKER_ID', '0'))
                route_keys = [f"shard_{worker_id}"]
                return self.create_exchange(
                    self.input_exchange,
                    queue_name=queue_override,
                    route_keys=route_keys
                )
            
            return self.create_exchange(
                self.input_exchange,
                queue_name=queue_override,
            )
        return self.create_queue(self.input_queue)

    def _create_output_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_output_exchange():
            return self.create_exchange(self.output_exchange)
        return self.create_queue(self.output_queue)

    def create_exchange(
        self,
        name: str,
        queue_name: Optional[str] = None,
        route_keys: Optional[list[str]] = None,
        exchange_type: str = 'direct'
    ) -> RabbitMQMiddlewareExchange:
        return RabbitMQMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=name,
            route_keys=route_keys or [name],
            exchange_type=exchange_type,
            port=self.rabbitmq_port,
            queue_name=queue_name,
            prefetch_count=self.prefetch_count
        )

    def create_queue(self, name: str, prefetch_count: int | None = None) -> RabbitMQMiddlewareQueue:
        return RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=name,
            port=self.rabbitmq_port,
            prefetch_count=prefetch_count or self.prefetch_count
        )
    
    # def create_eof_requeue(self) -> RabbitMQMiddlewareQueue:
    #     name = self.input_queue + '_eof_requeue'
    #     if self.is_sharded_worker:
    #         name = self.input_exchange + '_eof_requeue_sharded'
    #     logger.info(f"[DEBUG] Worker {os.getenv('WORKER_ID', '0')} creating EOF requeue queue: {name}")
    #     return self.create_queue(name, 1)

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
    
    def _is_sharded_worker(self) -> bool:
        """Check if this is a sharded worker that needs specific routing keys."""
        return os.getenv('IS_SHARDED_WORKER') == 'True'

    def cleanup(self) -> None:
        """Clean up middleware connections."""
        if self.input_middleware:
            self.input_middleware.close()
        if self.output_middleware:
            self.output_middleware.close()
