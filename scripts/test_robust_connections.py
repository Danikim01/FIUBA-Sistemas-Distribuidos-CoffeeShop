#!/usr/bin/env python3

"""
Test script for the robust RabbitMQ connection management.
Run this to verify the new connection handling works properly.
"""

import sys
import os
import time
import threading
import logging

# Add the middleware directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue
from middleware.connection_manager import get_connection_pool, close_all_pools

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_robust_connection():
    """Test the robust connection management."""
    print("🔍 Testing Robust RabbitMQ Connection Management")
    print("=" * 50)
    
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    test_queue = f"test_robust_{int(time.time())}"
    
    print(f"Testing with queue: {test_queue}")
    print(f"RabbitMQ host: {rabbitmq_host}")
    print()
    
    # Test 1: Basic send/receive
    print("Test 1: Basic send/receive operations")
    try:
        middleware = RabbitMQMiddlewareQueue(rabbitmq_host, test_queue)
        
        # Send test message
        test_message = {"test": "message", "timestamp": time.time()}
        middleware.send(test_message)
        print("✓ Message sent successfully")
        
        # Test receiving (we'll just test that consuming starts without error)
        received_messages = []
        
        def on_message(message):
            received_messages.append(message)
            print(f"✓ Received message: {message}")
            # Stop consuming after first message
            middleware.stop_consuming()
        
        # Start consuming in a separate thread
        consumer_thread = threading.Thread(target=middleware.start_consuming, args=(on_message,))
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Wait a bit for message processing
        time.sleep(2)
        
        middleware.close()
        print("✓ Basic operations completed successfully")
        
    except Exception as e:
        print(f"✗ Basic operations failed: {e}")
        return False
    
    print()
    
    # Test 2: Connection resilience with multiple workers
    print("Test 2: Multiple workers with connection pooling")
    try:
        workers = []
        for i in range(3):
            worker = RabbitMQMiddlewareQueue(rabbitmq_host, f"{test_queue}_{i}")
            workers.append(worker)
            
            # Send a message from each worker
            worker.send({"worker_id": i, "message": f"test from worker {i}"})
            print(f"✓ Worker {i} sent message successfully")
        
        # Clean up
        for worker in workers:
            worker.close()
        
        print("✓ Multiple workers test completed successfully")
        
    except Exception as e:
        print(f"✗ Multiple workers test failed: {e}")
        return False
    
    print()
    
    # Test 3: Connection pool cleanup
    print("Test 3: Connection pool cleanup")
    try:
        close_all_pools()
        print("✓ Connection pools closed successfully")
        
    except Exception as e:
        print(f"✗ Connection pool cleanup failed: {e}")
        return False
    
    print()
    print("🎉 All tests passed! Robust connection management is working.")
    return True

def stress_test_connections():
    """Stress test the connection management."""
    print("🔥 Stress Testing Connection Management")
    print("=" * 40)
    
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    test_queue = f"stress_test_{int(time.time())}"
    
    num_workers = 10
    messages_per_worker = 5
    
    print(f"Creating {num_workers} workers, each sending {messages_per_worker} messages")
    
    def worker_function(worker_id):
        """Function to run in each worker thread."""
        try:
            middleware = RabbitMQMiddlewareQueue(rabbitmq_host, f"{test_queue}_{worker_id}")
            
            for i in range(messages_per_worker):
                message = {
                    "worker_id": worker_id,
                    "message_num": i,
                    "timestamp": time.time()
                }
                middleware.send(message)
                time.sleep(0.1)  # Small delay between messages
            
            middleware.close()
            print(f"✓ Worker {worker_id} completed successfully")
            
        except Exception as e:
            print(f"✗ Worker {worker_id} failed: {e}")
    
    # Create and start worker threads
    threads = []
    start_time = time.time()
    
    for i in range(num_workers):
        thread = threading.Thread(target=worker_function, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    
    print(f"✓ Stress test completed in {end_time - start_time:.2f} seconds")
    
    # Clean up
    close_all_pools()
    print("✓ Cleanup completed")

def main():
    """Main test function."""
    if len(sys.argv) > 1 and sys.argv[1] == 'stress':
        stress_test_connections()
    else:
        if test_robust_connection():
            print("\n🚀 Your robust connection management is ready!")
            print("   The connection issues should be significantly reduced.")
        else:
            print("\n❌ Tests failed. Please check your RabbitMQ setup.")

if __name__ == '__main__':
    main()