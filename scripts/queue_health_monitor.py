#!/usr/bin/env python3

import pika # type: ignore
from pika.exceptions import ChannelClosedByBroker # type: ignore
import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class QueueStats:
    """Statistics for a RabbitMQ queue."""
    name: str
    messages: int
    consumers: int
    memory: int
    message_bytes: int
    
    def is_healthy(self) -> bool:
        """Check if queue appears healthy."""
        # Warning thresholds
        MAX_MESSAGES = 10000  # Adjust based on your workload
        MAX_MEMORY_MB = 100   # Adjust based on your available memory
        
        memory_mb = self.memory / (1024 * 1024)
        
        if self.messages > MAX_MESSAGES:
            logger.warning(f"Queue '{self.name}' has {self.messages} messages (>{MAX_MESSAGES})")
            return False
            
        if memory_mb > MAX_MEMORY_MB:
            logger.warning(f"Queue '{self.name}' using {memory_mb:.1f}MB memory (>{MAX_MEMORY_MB}MB)")
            return False
            
        if self.consumers == 0 and self.messages > 0:
            logger.warning(f"Queue '{self.name}' has {self.messages} messages but no consumers")
            return False
        
        return True

class RabbitMQHealthMonitor:
    """Monitor RabbitMQ queue health and connection status."""
    
    def __init__(self, host: str, port: int = 5672, management_port: int = 15672):
        self.host = host
        self.port = port
        self.management_port = management_port
        
    def get_queue_stats(self, queue_names: List[str]) -> Dict[str, QueueStats]:
        """Get statistics for specified queues."""
        stats = {}
        
        try:
            # Try to connect and get basic queue info
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, port=self.port)
            )
            channel = connection.channel()
            
            for queue_name in queue_names:
                try:
                    # Declare queue to ensure it exists
                    channel.queue_declare(queue=queue_name, passive=True)
                    
                    # Get queue method info - this gives us message count
                    method = channel.queue_declare(queue=queue_name, passive=True)
                    message_count = method.method.message_count
                    consumer_count = method.method.consumer_count
                    
                    stats[queue_name] = QueueStats(
                        name=queue_name,
                        messages=message_count,
                        consumers=consumer_count,
                        memory=0,  # Not available via AMQP
                        message_bytes=0  # Not available via AMQP
                    )
                    
                    logger.info(f"Queue '{queue_name}': {message_count} messages, {consumer_count} consumers")
                except ChannelClosedByBroker as e:
                    logger.error(f"Queue '{queue_name}' not found or accessible: {e}")
                    logger.error(f"Queue '{queue_name}' not found or accessible: {e}")
                    
            channel.close()
            connection.close()
            
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            
        return stats
    
    def check_connection_health(self) -> bool:
        """Test basic RabbitMQ connectivity."""
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host, 
                    port=self.port,
                    socket_timeout=5.0,
                    heartbeat=600
                )
            )
            channel = connection.channel()
            
            # Test basic operations
            test_queue = f"health_check_{int(time.time())}"
            channel.queue_declare(queue=test_queue, exclusive=True)
            channel.queue_delete(queue=test_queue)
            
            channel.close()
            connection.close()
            
            logger.info("RabbitMQ connection health check passed")
            return True
            
        except Exception as e:
            logger.error(f"RabbitMQ connection health check failed: {e}")
            return False
    
    def monitor_queues(self, queue_names: List[str], check_interval: int = 30):
        """Continuously monitor queue health."""
        logger.info(f"Starting queue health monitoring for: {queue_names}")
        
        while True:
            try:
                # Check connection health
                if not self.check_connection_health():
                    logger.error("RabbitMQ connection unhealthy!")
                    time.sleep(check_interval)
                    continue
                
                # Get queue stats
                stats = self.get_queue_stats(queue_names)
                
                # Check each queue health
                unhealthy_queues = []
                for queue_name, queue_stats in stats.items():
                    if not queue_stats.is_healthy():
                        unhealthy_queues.append(queue_name)
                
                if unhealthy_queues:
                    logger.warning(f"Unhealthy queues detected: {unhealthy_queues}")
                else:
                    logger.info("All monitored queues are healthy")
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("Health monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                time.sleep(check_interval)

def main():
    """Main function for queue health monitoring."""
    import sys
    import os
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get RabbitMQ host from environment or default
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    
    # Define queues to monitor - adjust these based on your system
    queues_to_monitor = [
        'gateway_results',
        'top_clients_partial',
        'users_raw',
        'transactions_raw',
        'transaction_items_raw',
        'stores_raw',
        'menu_items_raw'
    ]
    
    monitor = RabbitMQHealthMonitor(rabbitmq_host)
    
    if len(sys.argv) > 1 and sys.argv[1] == 'check':
        # Single health check
        stats = monitor.get_queue_stats(queues_to_monitor)
        connection_healthy = monitor.check_connection_health()
        
        print(f"Connection healthy: {connection_healthy}")
        print("Queue Statistics:")
        for queue_name, stats in stats.items():
            health = "HEALTHY" if stats.is_healthy() else "UNHEALTHY"
            print(f"  {queue_name}: {stats.messages} messages, {stats.consumers} consumers - {health}")
    else:
        # Continuous monitoring
        monitor.monitor_queues(queues_to_monitor)

if __name__ == '__main__':
    main()