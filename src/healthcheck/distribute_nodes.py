"""Function to distribute nodes across multiple healthcheckers using consistent hashing."""

import hashlib
from typing import List, Tuple


def distribute_nodes(
    all_nodes: List[str],
    healthchecker_id: int,
    total_healthcheckers: int
) -> List[str]:
    """Distribute nodes to a specific healthchecker using consistent hashing.
    
    Each node is assigned to a healthchecker based on: hash(node_name) % total_healthcheckers
    
    Args:
        all_nodes: List of all node names to distribute
        healthchecker_id: ID of this healthchecker (1-based: 1, 2, 3, ...)
        total_healthcheckers: Total number of healthcheckers
        
    Returns:
        List of node names assigned to this healthchecker
    """
    if total_healthcheckers <= 0:
        raise ValueError("total_healthcheckers must be > 0")
    if healthchecker_id < 1 or healthchecker_id > total_healthcheckers:
        raise ValueError(f"healthchecker_id must be between 1 and {total_healthcheckers}")
    
    assigned_nodes = []
    # Convert to 0-based index for modulo operation
    healthchecker_index = healthchecker_id - 1
    
    for node in all_nodes:
        # Use MD5 hash of node name, then modulo to assign to healthchecker
        node_hash = int(hashlib.md5(node.encode()).hexdigest(), 16)
        assigned_index = node_hash % total_healthcheckers
        
        if assigned_index == healthchecker_index:
            assigned_nodes.append(node)
    
    return assigned_nodes


def get_next_healthchecker(healthchecker_id: int, total_healthcheckers: int) -> int:
    """Get the ID of the next healthchecker in the ring.
    
    Args:
        healthchecker_id: Current healthchecker ID (1-based)
        total_healthcheckers: Total number of healthcheckers
        
    Returns:
        Next healthchecker ID in the ring (wraps around)
    """
    if healthchecker_id == total_healthcheckers:
        return 1  # Wrap around to first healthchecker
    return healthchecker_id + 1


def get_healthchecker_name(healthchecker_id: int, base_name: str = "healthchecker") -> str:
    """Get the container name for a healthchecker.
    
    Args:
        healthchecker_id: Healthchecker ID (1-based)
        base_name: Base name for healthchecker containers
        
    Returns:
        Container name (e.g., "healthchecker-1")
    """
    return f"{base_name}-{healthchecker_id}"

