#!/usr/bin/env python3

import pika # type: ignore
import pika.exceptions # type: ignore
import logging
import time
import threading
from typing import Optional, Callable, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class RobustRabbitMQConnection:
    """
    A robust RabbitMQ connection manager that handles:
    - Automatic reconnection with exponential backoff
    - Connection pooling
    - Heartbeat management
    - Thread safety
    - Graceful error handling
    """
    
    def __init__(
        self,
        host: str,
        port: int = 5672,
        max_retries: int = 10,
        initial_retry_delay: float = 1.0,
        max_retry_delay: float = 60.0,
        heartbeat: int = 600,  # 10 minutes
        blocked_connection_timeout: int = 300,  # 5 minutes
    ):
        self.host = host
        self.port = port
        self.max_retries = max_retries
        self.initial_retry_delay = initial_retry_delay
        self.max_retry_delay = max_retry_delay
        self.heartbeat = heartbeat
        self.blocked_connection_timeout = blocked_connection_timeout
        
        self._connection: Optional[pika.BlockingConnection] = None
        self._connection_lock = threading.RLock()
        self._is_closed = False
        self._retry_count = 0
        
        logger.info(f"Initializing robust RabbitMQ connection manager for {host}:{port}")
    
    @property
    def connection_parameters(self) -> pika.ConnectionParameters:
        """Get connection parameters with robust settings."""
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            heartbeat=self.heartbeat,
            blocked_connection_timeout=self.blocked_connection_timeout,
            # Enable TCP keep-alive
            socket_timeout=5.0,
            # Connection name for debugging
            client_properties={
                'connection_name': f'robust-worker-{threading.current_thread().name}'
            }
        )
    
    def _calculate_retry_delay(self) -> float:
        """Calculate exponential backoff delay."""
        delay = self.initial_retry_delay * (2 ** self._retry_count)
        return min(delay, self.max_retry_delay)
    
    def _connect(self) -> pika.BlockingConnection:
        """Establish connection with retry logic."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if self._is_closed:
                    raise ConnectionError("Connection manager is closed")
                
                logger.info(f"Attempting to connect to RabbitMQ {self.host}:{self.port} (attempt {attempt + 1})")
                connection = pika.BlockingConnection(self.connection_parameters)
                
                # Reset retry count on successful connection
                self._retry_count = 0
                logger.info(f"Successfully connected to RabbitMQ {self.host}:{self.port}")
                return connection
                
            except (pika.exceptions.AMQPConnectionError, ConnectionError) as e:
                last_exception = e
                self._retry_count = attempt
                
                if attempt < self.max_retries:
                    delay = self._calculate_retry_delay()
                    logger.warning(
                        f"Connection attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to connect after {self.max_retries + 1} attempts")
        
        raise ConnectionError(f"Unable to connect to RabbitMQ after {self.max_retries + 1} attempts: {last_exception}")
    
    def get_connection(self) -> pika.BlockingConnection:
        """Get a connection, creating one if necessary."""
        with self._connection_lock:
            if self._is_closed:
                raise ConnectionError("Connection manager is closed")
            
            # Check if current connection is still valid
            if self._connection and not self._connection.is_closed:
                try:
                    # Test connection with a simple operation
                    self._connection.process_data_events(time_limit=0)
                    return self._connection
                except Exception as e:
                    logger.warning(f"Existing connection failed health check: {e}")
                    self._connection = None
            
            # Create new connection
            self._connection = self._connect()
            return self._connection
    
    @contextmanager
    def get_channel(self):
        """Context manager for getting a channel with automatic cleanup."""
        channel = None
        try:
            connection = self.get_connection()
            channel = connection.channel()
            
            # Try to enable delivery confirmations, but don't fail if it doesn't work
            try:
                channel.confirm_delivery()
            except Exception as e:
                logger.warning(f"Could not enable delivery confirmations: {e}")
            
            yield channel
            
        except pika.exceptions.UnroutableError as e:
            logger.warning(f"Message could not be routed (this may be normal): {e}")
            # Don't re-raise unroutable errors as they may be expected
        except pika.exceptions.NackError as e:
            logger.error(f"Message was nacked by server: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in channel context manager: {e}")
            raise
        finally:
            if channel and not channel.is_closed:
                try:
                    channel.close()
                except Exception as e:
                    logger.warning(f"Error closing channel: {e}")
    
    def close(self):
        """Close the connection."""
        with self._connection_lock:
            self._is_closed = True
            if self._connection and not self._connection.is_closed:
                try:
                    self._connection.close()
                    logger.info(f"Closed RabbitMQ connection to {self.host}:{self.port}")
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
                finally:
                    self._connection = None


class ConnectionPool:
    """
    Thread-safe connection pool for RabbitMQ connections.
    """
    
    def __init__(self, host: str, port: int = 5672, max_connections: int = 10):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self._pool: list[RobustRabbitMQConnection] = []
        self._pool_lock = threading.Lock()
        self._closed = False
        
        logger.info(f"Initialized connection pool for {host}:{port} with max {max_connections} connections")
    
    def get_connection(self) -> RobustRabbitMQConnection:
        """Get a connection from the pool."""
        with self._pool_lock:
            if self._closed:
                raise ConnectionError("Connection pool is closed")
            
            # Try to reuse an existing connection
            while self._pool:
                conn = self._pool.pop(0)
                if not conn._is_closed:
                    return conn
            
            # Create new connection if pool is empty or all connections are closed
            if len(self._pool) < self.max_connections:
                return RobustRabbitMQConnection(self.host, self.port)
            
            # If pool is full, create a new connection anyway (this is acceptable for RabbitMQ)
            logger.warning("Connection pool exhausted, creating additional connection")
            return RobustRabbitMQConnection(self.host, self.port)
    
    def return_connection(self, connection: RobustRabbitMQConnection):
        """Return a connection to the pool."""
        with self._pool_lock:
            if self._closed or connection._is_closed:
                connection.close()
                return
            
            if len(self._pool) < self.max_connections:
                self._pool.append(connection)
            else:
                # Pool is full, close the connection
                connection.close()
    
    def close_all(self):
        """Close all connections in the pool."""
        with self._pool_lock:
            self._closed = True
            while self._pool:
                conn = self._pool.pop()
                conn.close()
            logger.info("Closed all connections in pool")


# Global connection pool instance
_connection_pools = {}
_pool_lock = threading.Lock()

def get_connection_pool(host: str, port: int = 5672) -> ConnectionPool:
    """Get or create a connection pool for the specified host."""
    key = f"{host}:{port}"
    
    with _pool_lock:
        if key not in _connection_pools:
            _connection_pools[key] = ConnectionPool(host, port)
        return _connection_pools[key]

def close_all_pools():
    """Close all connection pools."""
    with _pool_lock:
        for pool in _connection_pools.values():
            pool.close_all()
        _connection_pools.clear()