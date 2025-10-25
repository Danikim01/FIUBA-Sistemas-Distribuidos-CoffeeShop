#!/usr/bin/env python3

"""Items Sharding router that distributes transaction items to Items workers based on item_id."""

import logging
import os
from typing import Any
from workers.utils.worker_utils import run_main
from workers.sharding.sharding_router import ShardingRouter
from workers.utils.sharding_utils import get_routing_key_for_semester, extract_semester_from_payload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ItemsShardingRouter(ShardingRouter):
    """
    Items-specific sharding router that inherits from the base ShardingRouter.
    
    This router distributes transaction items to Items sharded workers based on item_id.
    It reuses all the batching and routing logic from the base ShardingRouter.
    """
    
    def __init__(self):
        super().__init__()
        logger.info("ItemsShardingRouter initialized - inheriting all functionality from ShardingRouter")

    def process_message(self, message: Any, client_id: str) -> None:
        """
        Process incoming transaction item and add it to appropriate shard batch.
        
        Args:
            message: Transaction item data
            client_id: Client identifier
        """
        # Extract semester from the transaction item
        semester = extract_semester_from_payload(message)
        if semester is None:
            logger.warning(f"Transaction item without created_at, skipping. Message keys: {list(message.keys()) if isinstance(message, dict) else 'Not a dict'}")
            return
            
        # Calculate routing key for this semester
        routing_key = get_routing_key_for_semester(message.get('created_at'), self.num_shards)
        
        # Add message to batch
        self._add_to_batch(client_id, routing_key, message)


if __name__ == '__main__':
    run_main(ItemsShardingRouter)
