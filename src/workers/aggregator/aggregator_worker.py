"""Utilities for multi-source aggregators."""

import logging
from typing import Any, Callable, Dict

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from message_utils import is_eof_message
from workers.utils.base_worker import BaseWorker


logger = logging.getLogger(__name__)


class AggregatorWorker(BaseWorker):
    """Base worker that consumes a main queue plus optional auxiliary sources."""

    def __init__(self, additional_queues: Dict[str, str] | None = None) -> None:
        super().__init__()

        additional_queues = additional_queues or {}
        self._aux_middleware: Dict[str, RabbitMQMiddlewareQueue] = {}

        for source_name, queue_name in additional_queues.items():
            self._aux_middleware[source_name] = RabbitMQMiddlewareQueue(
                host=self.config.rabbitmq_host,
                queue_name=queue_name,
                port=self.config.rabbitmq_port,
                prefetch_count=self.config.prefetch_count,
            )

        self._sources_completed = set()
        self._expected_sources = {'main'} | set(additional_queues.keys())

        logger.info(
            "AggregatorWorker initialized with sources: %s",
            ', '.join(sorted(self._expected_sources)),
        )

    # ------------------------------------------------------------------
    # Auxiliary source consumption
    # ------------------------------------------------------------------

    def start_auxiliary_source(
        self,
        source_name: str,
        handler: Callable[[Any], None],
    ) -> None:
        middleware = self._aux_middleware.get(source_name)
        if middleware is None:
            logger.error("Unknown auxiliary source requested: %s", source_name)
            return

        def on_message(message: Any) -> None:
            if self.shutdown_requested:
                middleware.stop_consuming()
                return

            if is_eof_message(message):
                logger.info("EOF received from auxiliary source %s", source_name)
                self._sources_completed.add(source_name)
                middleware.stop_consuming()
                self.on_auxiliary_eof(source_name)
                return

            handler(message)

        try:
            middleware.start_consuming(on_message)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error consuming from %s: %s", source_name, exc)

    def on_auxiliary_eof(self, source_name: str) -> None:
        """Hook for subclasses to react when an auxiliary source finishes."""

        _ = source_name  # default no-op

    # ------------------------------------------------------------------
    # EOF coordination
    # ------------------------------------------------------------------

    def handle_eof(self, message: Dict[str, Any]):
        logger.info("EOF received from main source")
        self._sources_completed.add('main')

        if self._sources_completed >= self._expected_sources:
            logger.info("All sources completed; forwarding EOF")
            super().handle_eof(message)
        else:
            remaining = self._expected_sources - self._sources_completed
            logger.info("Waiting for auxiliary sources: %s", ', '.join(sorted(remaining)))
            self.input_middleware.stop_consuming()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self):
        super().cleanup()
        for source_name, middleware in self._aux_middleware.items():
            try:
                middleware.close()
                logger.debug("Closed middleware for source %s", source_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Error cleaning up middleware for %s: %s", source_name, exc)
