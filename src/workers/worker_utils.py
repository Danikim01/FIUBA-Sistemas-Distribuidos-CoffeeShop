#!/usr/bin/env python3
"""
Worker utilities module for common functionality across all workers.
Provides base classes and helper functions to reduce code duplication.
"""

import os
import sys
import logging
import signal
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue, RabbitMQMiddlewareExchange

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_eof_message(message: Any) -> bool:
    """Check if a message is an EOF (End Of File) control message."""
    return isinstance(message, dict) and str(message.get('type', '')).upper() == 'EOF'


class WorkerConfig:
    """Configuration class for worker settings."""
    
    def __init__(self, input_queue: str, output_exchange: Optional[str] = None, output_queue: Optional[str] = None):
        """
        Initialize worker configuration.
        
        Args:
            input_queue: Input queue name (required)
            output_exchange: Output exchange name for fanout (optional)
            output_queue: Output queue name for direct sending (optional)
        """
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
        self.input_queue = input_queue
        self.output_exchange = output_exchange
        self.output_queue = output_queue
        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', '10'))


class BaseWorker(ABC):
    """
    Base class for all workers providing common functionality.
    Handles middleware setup, signal handling, and message processing patterns.
    """
    
    def __init__(self, config: WorkerConfig):
        """Initialize base worker."""
        self.config = config
        self.shutdown_requested = False
        
        # Configure SIGTERM handling
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
        # Setup input middleware
        self.input_middleware = RabbitMQMiddlewareQueue(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
            port=config.rabbitmq_port,
            prefetch_count=config.prefetch_count
        )
        
        # Setup output middleware (fanout exchange or direct queue)
        if config.output_exchange:
            self.output_middleware = RabbitMQMiddlewareExchange(
                host=config.rabbitmq_host,
                exchange_name=config.output_exchange,
                route_keys=[],  # Not needed for fanout
                exchange_type='fanout',
                port=config.rabbitmq_port
            )
        elif config.output_queue:
            self.output_middleware = RabbitMQMiddlewareQueue(
                host=config.rabbitmq_host,
                queue_name=config.output_queue,
                port=config.rabbitmq_port
            )
        else:
            self.output_middleware = None
        
        self._initialize_worker()
        
        logger.info(
            "%s initialized - Input: %s, Output: %s",
            self.__class__.__name__,
            config.input_queue,
            config.output_exchange or config.output_queue or "None"
        )
    
    def _initialize_worker(self):
        """Hook for subclasses to perform additional initialization."""
        pass
    
    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown."""
        logger.info("SIGTERM received, initiating graceful shutdown...", signum, frame)
        self.input_middleware.stop_consuming()
        self.shutdown_requested = True
    
    def send_message(self, message: Any):
        """Send a message to the output."""
        if self.output_middleware:
            self.output_middleware.send(message)
    
    def send_eof(self, additional_data: Optional[Dict[str, Any]] = None):
        """Send EOF message to output."""
        eof_message = {'type': 'EOF'}
        if additional_data:
            eof_message.update(additional_data)
        
        self.send_message(eof_message)
    
    @abstractmethod
    def process_message(self, message: Any):
        """Process a single message. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def process_batch(self, batch: List[Any]):
        """Process a batch of messages. Must be implemented by subclasses."""
        pass
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF message. Can be overridden by subclasses."""
        self.send_eof()
        self.input_middleware.stop_consuming()
    
    def start_consuming(self):
        """Start consuming messages from the input queue."""
        try:
            def on_message(message):
                """Callback for processing received messages."""
                try:
                    if self.shutdown_requested:
                        logger.info("Shutdown requested, stopping message processing")
                        return
                    
                    if is_eof_message(message):
                        self.handle_eof(message)
                        return
                    
                    if isinstance(message, list):
                        self.process_batch(message)
                    else:
                        self.process_message(message)
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            self.input_middleware.start_consuming(on_message)
            
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Error starting consumption: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if self.input_middleware:
                self.input_middleware.close()
            if self.output_middleware:
                self.output_middleware.close()
            logger.info("Resources cleaned up")
        except Exception as e:
            logger.warning(f"Error cleaning up resources: {e}")


class FilterWorker(BaseWorker):
    """Base class for filter workers that apply filtering logic to messages."""
    
    @abstractmethod
    def apply_filter(self, item: Any) -> bool:
        """Apply filter logic to determine if an item should pass through."""
        pass
    
    def process_message(self, message: Any):
        """Process a single message by applying filter."""
        if self.apply_filter(message):
            self.send_message(message)
    
    def process_batch(self, batch: List[Any]):
        """Process a batch by filtering and sending filtered results."""
        filtered_items = [item for item in batch if self.apply_filter(item)]
        if filtered_items:
            self.send_message(filtered_items)


class AggregatorWorker(BaseWorker):
    """Base class for workers that aggregate data and emit results at EOF."""
    
    def __init__(self, config: WorkerConfig):
        super().__init__(config)
        self.results_emitted = False
    
    @abstractmethod
    def process_item(self, item: Any):
        """Process a single item for aggregation."""
        pass
    
    @abstractmethod
    def compute_results(self) -> Any:
        """Compute and return final aggregated results."""
        pass
    
    def process_message(self, message: Any):
        """Process a single message for aggregation."""
        self.process_item(message)
    
    def process_batch(self, batch: List[Any]):
        """Process a batch of messages for aggregation."""
        for item in batch:
            self.process_item(item)
    
    def emit_results(self):
        """Emit aggregated results."""
        if self.results_emitted:
            return
        
        results = self.compute_results()
        if results is not None:
            self.send_message(results)
        
        self.results_emitted = True
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF by emitting results."""
        self.emit_results()
        super().handle_eof(message)


class MultiSourceWorker(BaseWorker):
    """Base class for workers that consume from multiple input sources."""
    
    def __init__(self, config: WorkerConfig, additional_queues: Dict[str, str]):
        """Initialize multi-source worker."""
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
    
    def consume_additional_source(self, source_name: str, message_handler: Callable[[Any], None]):
        """Consume messages from an additional source."""
        middleware = self.additional_middlewares[source_name]
        
        def on_message(message):
            if self.shutdown_requested:
                middleware.stop_consuming()
                return
            
            if is_eof_message(message):
                self.sources_completed.add(source_name)
                middleware.stop_consuming()
                return
            
            message_handler(message)
        
        try:
            middleware.start_consuming(on_message)
        except Exception as e:
            logger.error(f"Error consuming from {source_name}: {e}")
        finally:
            logger.info(f"Finished consuming from {source_name}")
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF from main source."""
        self.sources_completed.add('main')
        if self.all_sources_completed():
            super().handle_eof(message)
        else:
            self.input_middleware.stop_consuming()
    
    def all_sources_completed(self) -> bool:
        """Check if all sources have completed."""
        return self.sources_completed >= self.expected_sources
    
    def cleanup(self):
        """Clean up all resources including additional middlewares."""
        super().cleanup()
        for middleware in self.additional_middlewares.values():
            try:
                middleware.close()
            except Exception as e:
                logger.warning(f"Error cleaning up middleware: {e}")


def create_worker_main(worker_class, *args, **kwargs):
    """Helper function to create a standard main function for workers."""
    def main():
        try:
            worker = worker_class(*args, **kwargs)
            worker.start_consuming()
        except Exception as e:
            logger.error(f"Error in main: {e}")
            sys.exit(1)
    
    return main


# Utility functions
def safe_float_conversion(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """Safely convert a value to int."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def parse_datetime_safe(datetime_str: str, format_str: str = '%Y-%m-%d %H:%M:%S') -> Optional[Any]:
    """Safely parse a datetime string."""
    try:
        from datetime import datetime
        return datetime.strptime(datetime_str, format_str)
    except (ValueError, TypeError):
        return None


def extract_year_safe(datetime_str: str) -> Optional[int]:
    """Safely extract year from datetime string."""
    dt = parse_datetime_safe(datetime_str)
    return dt.year if dt else None


def extract_time_safe(datetime_str: str) -> Optional[Any]:
    """Safely extract time from datetime string."""
    dt = parse_datetime_safe(datetime_str)
    return dt.time() if dt else None