"""
Middleware package para comunicación con RabbitMQ.
"""

from .connection_manager import (  # noqa: F401
    RobustRabbitMQConnection,
    close_all_pools,
    get_connection_pool,
)
from .middleware_interface import (  # noqa: F401
    MessageMiddleware,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from .rabbitmq_middleware import (  # noqa: F401
    RabbitMQMiddlewareExchange,
    RabbitMQMiddlewareQueue,
)
from .thread_aware_publishers import (  # noqa: F401
    ThreadAwareExchangePublisher,
    ThreadAwareQueuePublisher,
)

__all__ = [
    "MessageMiddleware",
    "MessageMiddlewareMessageError",
    "MessageMiddlewareDisconnectedError",
    "MessageMiddlewareCloseError",
    "MessageMiddlewareDeleteError",
    "RabbitMQMiddlewareQueue",
    "RabbitMQMiddlewareExchange",
    "RobustRabbitMQConnection",
    "get_connection_pool",
    "close_all_pools",
    "ThreadAwareQueuePublisher",
    "ThreadAwareExchangePublisher",
]
