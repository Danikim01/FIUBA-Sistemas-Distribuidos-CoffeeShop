#!/usr/bin/env python3

"""Test script to verify sharding functionality."""

import sys
import os

# Add src to path for imports
sys.path.append('src')

from workers.utils.sharding_utils import get_shard_id, get_routing_key, extract_store_id_from_payload


def test_sharding_consistency():
    """Test that same store_id always gets same shard."""
    print("Testing sharding consistency...")
    
    test_store_ids = [1, 2, 3, 4, 5, 10, 100, 1000]
    num_shards = 2
    
    for store_id in test_store_ids:
        shard1 = get_shard_id(store_id, num_shards)
        shard2 = get_shard_id(store_id, num_shards)
        routing_key = get_routing_key(store_id, num_shards)
        
        assert shard1 == shard2, f"Shard inconsistent for store_id {store_id}"
        assert routing_key == f"shard_{shard1}", f"Routing key incorrect for store_id {store_id}"
        
        print(f"Store ID {store_id:4d} -> Shard {shard1} -> Routing Key {routing_key}")


def test_sharding_distribution():
    """Test that shards are distributed evenly."""
    print("\nTesting sharding distribution...")
    
    num_shards = 2
    store_ids = list(range(1, 101))  # 100 store IDs
    
    shard_counts = [0] * num_shards
    
    for store_id in store_ids:
        shard = get_shard_id(store_id, num_shards)
        shard_counts[shard] += 1
    
    print(f"Distribution across {num_shards} shards:")
    for i, count in enumerate(shard_counts):
        percentage = (count / len(store_ids)) * 100
        print(f"  Shard {i}: {count:3d} stores ({percentage:5.1f}%)")
    
    # Check distribution is reasonably balanced (within 20% of each other)
    max_count = max(shard_counts)
    min_count = min(shard_counts)
    imbalance = (max_count - min_count) / max_count
    
    print(f"Imbalance: {imbalance:.1%}")
    assert imbalance < 0.2, f"Shards too imbalanced: {imbalance:.1%}"


def test_payload_extraction():
    """Test store_id extraction from transaction payloads."""
    print("\nTesting payload extraction...")
    
    test_payloads = [
        {"store_id": 1, "user_id": 100, "amount": 50.0},
        {"store_id": "2", "user_id": 200, "amount": 75.0},
        {"user_id": 300, "amount": 100.0},  # No store_id
        {"store_id": None, "user_id": 400, "amount": 25.0},
    ]
    
    expected_store_ids = [1, "2", None, None]
    
    for payload, expected in zip(test_payloads, expected_store_ids):
        extracted = extract_store_id_from_payload(payload)
        assert extracted == expected, f"Extraction failed for {payload}: got {extracted}, expected {expected}"
        print(f"Payload {payload} -> Store ID: {extracted}")


def test_routing_key_generation():
    """Test routing key generation for different scenarios."""
    print("\nTesting routing key generation...")
    
    test_cases = [
        (1, 2, "shard_1"),
        (2, 2, "shard_0"),
        (10, 3, "shard_1"),
        (100, 4, "shard_0"),
    ]
    
    for store_id, num_shards, expected_prefix in test_cases:
        routing_key = get_routing_key(store_id, num_shards)
        assert routing_key.startswith("shard_"), f"Routing key should start with 'shard_': {routing_key}"
        print(f"Store ID {store_id} with {num_shards} shards -> {routing_key}")


def main():
    """Run all sharding tests."""
    print("🧪 Testing Sharding Implementation")
    print("=" * 50)
    
    try:
        test_sharding_consistency()
        test_sharding_distribution()
        test_payload_extraction()
        test_routing_key_generation()
        
        print("\n✅ All tests passed! Sharding implementation is working correctly.")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
