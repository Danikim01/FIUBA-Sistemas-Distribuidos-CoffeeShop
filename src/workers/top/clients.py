#!/usr/bin/env python3

import os
import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from worker_utils import MultiSourceWorker, WorkerConfig, create_worker_main, safe_int_conversion

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopClientsWorkerRefactored(MultiSourceWorker):
    """
    Calculates the most frequent clients by store for 2024/2025.
    Uses MultiSourceWorker to handle multiple input sources.
    """
    
    def _initialize_worker(self):
        """Initialize worker-specific configuration."""
        # Configuration
        self.top_levels = int(os.getenv('TOP_LEVELS', 3))
        
        # Lookup tables and accumulated counters
        self.store_lookup: Dict[int, str] = {}
        self.user_lookup: Dict[int, Dict[str, Any]] = {}
        self.counts_by_store: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        
        # Completion tracking
        self.stores_loaded = False
        self.users_loaded = False
        self.results_emitted = False
        
        logger.info(f"TopClientsWorker configured with top_levels: {self.top_levels}")
    
    def process_message(self, message: Any):
        """Process transactions (main source)."""
        self._update_counts(message)
    
    def process_batch(self, batch: List[Any]):
        """Process transaction batches (main source)."""
        for transaction in batch:
            self._update_counts(transaction)
    
    def _update_counts(self, transaction: Dict[str, Any]) -> None:
        """Update client frequency counts."""
        try:
            store_id = safe_int_conversion(transaction.get('store_id'))
            user_id = safe_int_conversion(transaction.get('user_id'))
        except Exception:
            logger.debug(f"Invalid compact transaction: {transaction}")
            return

        if user_id <= 0:
            return

        self.counts_by_store[store_id][user_id] += 1
    
    def process_stores_message(self, message: Any) -> None:
        """Process store data messages."""
        if isinstance(message, list):
            self._process_stores_batch(message)
        else:
            self._process_stores_batch([message])
    
    def _process_stores_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process batch of store data."""
        for store in batch:
            try:
                store_id = safe_int_conversion(store.get('store_id'))
                store_name = store.get('store_name', '')
                self.store_lookup[store_id] = store_name
            except Exception:
                logger.debug(f"Invalid store omitted: {store}")
    
    def process_users_message(self, message: Any) -> None:
        """Process user data messages."""
        if isinstance(message, list):
            self._process_users_batch(message)
        else:
            self._process_users_batch([message])
    
    def _process_users_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process batch of user data."""
        for user in batch:
            try:
                user_id = safe_int_conversion(user.get('user_id'))
            except Exception:
                logger.debug(f"Invalid user omitted: {user}")
                continue

            birthdate = user.get('birthdate', '')
            self.user_lookup[user_id] = {
                'birthdate': birthdate,
            }
    
    def _compute_results(self) -> List[Dict[str, Any]]:
        """Compute final results."""
        results: List[Dict[str, Any]] = []

        for store_id, user_counts in self.counts_by_store.items():
            if not user_counts:
                continue

            sorted_counts: List[Tuple[int, int]] = sorted(
                user_counts.items(),
                key=lambda item: (item[1], -item[0]),
                reverse=True,
            )

            unique_levels: List[int] = []
            for _, count in sorted_counts:
                if count not in unique_levels:
                    unique_levels.append(count)
                if len(unique_levels) >= self.top_levels:
                    break

            if not unique_levels:
                continue

            threshold = unique_levels[-1]
            store_name = self.store_lookup.get(store_id)
            if not store_name:
                continue

            limited_results: List[Dict[str, Any]] = []

            for user_id, count in sorted_counts:
                if count < threshold:
                    break

                birthdate = (self.user_lookup.get(user_id, {}) or {}).get('birthdate', '')
                if not birthdate or not str(birthdate).strip():
                    continue

                limited_results.append(
                    {
                        'store_id': store_id,
                        'store_name': store_name,
                        'user_id': user_id,
                        'birthdate': birthdate,
                        'purchases_qty': count,
                    }
                )

                if len(limited_results) >= 35:
                    break

            results.extend(limited_results)

        results.sort(
            key=lambda item: (
                item['store_name'],
                -item['purchases_qty'],
                item['birthdate'],
                item['user_id'],
            )
        )

        return results
    
    def _emit_results(self) -> None:
        """Emit final results."""
        if self.results_emitted:
            return

        results = self._compute_results()
        payload = {
            'type': 'top_clients_birthdays',
            'results': results,
        }

        self.send_to_outputs(payload)
        self.results_emitted = True
        logger.info(f"TopClientsWorker sent {len(results)} results")
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF from main source by emitting results."""
        self._emit_results()
        eof_with_source = {'type': 'EOF', 'source': 'top_clients_birthdays'}
        self.send_to_outputs(eof_with_source)
        self.input_middleware.stop_consuming()
    
    def start_consuming(self):
        """Start consuming from all sources."""
        try:
            # Load stores first
            logger.info("TopClientsWorker loading store metadata")
            self.consume_additional_source('stores', self.process_stores_message)
            
            # Load users second
            logger.info("TopClientsWorker loading user metadata")
            self.consume_additional_source('users', self.process_users_message)
            
            # Process transactions last
            logger.info("TopClientsWorker processing compact transactions")
            super().start_consuming()
            
        finally:
            self.cleanup()


def main():
    """Main entry point."""
    # Get queue names from environment
    users_queue = os.getenv('USERS_QUEUE', 'users_raw')
    stores_queue = os.getenv('STORES_QUEUE', 'stores_for_top_clients')
    
    config = WorkerConfig(
        input_queue_default='transactions_compact',  # Main transactions queue
        output_queue_default='transactions_final_results',
        prefetch_count_default=10
    )
    
    additional_queues = {
        'users': users_queue,
        'stores': stores_queue
    }
    
    worker = TopClientsWorkerRefactored(config, additional_queues)
    worker.start_consuming()


if __name__ == "__main__":
    # Use the helper to create a robust main function
    def create_top_clients_worker():
        users_queue = os.getenv('USERS_QUEUE', 'users_raw')
        stores_queue = os.getenv('STORES_QUEUE', 'stores_for_top_clients')
        
        config = WorkerConfig(
            input_queue_default='transactions_compact',
            output_queue_default='transactions_final_results',
            prefetch_count_default=10
        )
        
        additional_queues = {
            'users': users_queue,
            'stores': stores_queue
        }
        
        return TopClientsWorkerRefactored(config, additional_queues)
    
    main_func = create_worker_main(create_top_clients_worker)
    main_func()