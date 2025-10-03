"""Multi-source worker implementation for consuming from multiple queues."""

import logging
from typing import Any, Callable, Dict

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from workers.utils.base_worker import BaseWorker
from message_utils import is_eof_message

logger = logging.getLogger(__name__)


class AggregatorWorker(BaseWorker):
    """Base class for workers that consume from multiple input sources."""
    
    def __init__(self, config: WorkerConfig, additional_queues: Dict[str, str]):
        """Initialize multi-source worker.
        
        Args:
            config: Worker configuration instance
            additional_queues: Dictionary mapping source names to queue names
        """
        super().__init__(config)
        
        # Setup additional input middlewares
        self.additional_middlewares = {}
        for source_name, queue_name in additional_queues.items():
            self.additional_middlewares[source_name] = RabbitMQMiddlewareQueue(
                host=config.rabbitmq_host,
                queue_name=queue_name,
                port=config.rabbitmq_port,
                prefetch_count=config.prefetch_count
            )
        
        self.sources_completed = set()
        self.expected_sources = {'main'} | set(additional_queues.keys())
        
        logger.info(
            "MultiSourceWorker initialized with sources: %s",
            ', '.join(self.expected_sources)
        )
    
    def consume_additional_source(self, source_name: str, message_handler: Callable[[Any], None]):
        """Consume messages from an additional source.
        
        Args:
            source_name: Name of the source to consume from
            message_handler: Function to handle received messages
        """
        if source_name not in self.additional_middlewares:
            logger.error(f"Unknown source: {source_name}")
            return
        
        middleware = self.additional_middlewares[source_name]
        
        def on_message(message):
            """Handle messages from additional source.
            
            Args:
                message: Received message
            """
            if self.shutdown_requested:
                middleware.stop_consuming()
                return
            
            if is_eof_message(message):
                logger.info(f"EOF received from source: {source_name}")
                self.sources_completed.add(source_name)
                middleware.stop_consuming()
                return
            
            message_handler(message)
        
        try:
            logger.info(f"Starting consumption from source: {source_name}")
            middleware.start_consuming(on_message)
        except Exception as e:
            logger.error(f"Error consuming from {source_name}: {e}")
        finally:
            logger.info(f"Finished consuming from {source_name}")
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF from main source.
        
        Args:
            message: EOF message dictionary
        """
        logger.info("EOF received from main source")
        self.sources_completed.add('main')
        
        if self.all_sources_completed():
            logger.info("All sources completed, forwarding EOF")
            super().handle_eof(message)
        else:
            logger.info("Waiting for other sources to complete")
            self.input_middleware.stop_consuming()
    
    def all_sources_completed(self) -> bool:
        """Check if all sources have completed.
        
        Returns:
            True if all sources have sent EOF, False otherwise
        """
        completed = self.sources_completed >= self.expected_sources
        if not completed:
            remaining = self.expected_sources - self.sources_completed
            logger.debug(f"Waiting for sources: {', '.join(remaining)}")
        return completed
    
    def cleanup(self):
        """Clean up all resources including additional middlewares."""
        super().cleanup()
        for source_name, middleware in self.additional_middlewares.items():
            try:
                middleware.close()
                logger.debug(f"Closed middleware for source: {source_name}")
            except Exception as e:
                logger.warning(f"Error cleaning up middleware for {source_name}: {e}")
    
    def get_source_status(self) -> Dict[str, bool]:
        """Get completion status of all sources.
        
        Returns:
            Dictionary mapping source names to completion status
        """
        return {source: source in self.sources_completed for source in self.expected_sources}