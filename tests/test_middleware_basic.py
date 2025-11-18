#!/usr/bin/env python3
"""Basic tests for RabbitMQ middleware: 1 client, 2 clients, and 1-to-N scenarios.

Tests included:
1. Queue - Single Client (1:1)
2. Exchange - Single Client (1:1)
3. Queue - Two Clients with ThreadAware (N:1)
4. Exchange - Two Clients with ThreadAware (N:1)
5. Queue - One Producer to Many Consumers (1:N, load balancing)
6. Exchange - One Producer to Many Consumers (1:N, fanout broadcast)
"""

import logging
import sys
import threading
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue, RabbitMQMiddlewareExchange
from middleware.thread_aware_publishers import ThreadAwareQueuePublisher, ThreadAwareExchangePublisher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_queue_single_client():
    """Test 1: Single client sending and receiving messages in order."""
    logger.info("="*60)
    logger.info("TEST 1: Queue - Single Client")
    logger.info("="*60)
    
    host = 'localhost'
    queue_name = 'test_queue_single'
    
    received_messages = []
    received_lock = threading.Lock()
    consumer_ready = threading.Event()
    
    def consumer_callback(msg):
        with received_lock:
            received_messages.append(msg)
        logger.debug(f"Received: {msg}")
    
    # Create queue middleware
    queue = RabbitMQMiddlewareQueue(host=host, queue_name=queue_name)
    
    try:
        # Start consumer in background
        def run_consumer():
            consumer_ready.set()
            queue.start_consuming(consumer_callback)
        
        cons_thread = threading.Thread(target=run_consumer, daemon=True)
        cons_thread.start()
        
        # Wait for consumer to be ready
        consumer_ready.wait(timeout=2)
        time.sleep(0.5)  # Give it a moment to start
        
        # Send messages in order
        num_messages = 50
        logger.info(f"Sending {num_messages} messages...")
        for i in range(num_messages):
            message = {'seq': i, 'data': f'message_{i}'}
            queue.send(message)
            if i % 10 == 0:
                logger.debug(f"Sent message {i}")
        
        logger.info("All messages sent. Waiting for consumption...")
        
        # Wait for messages to be consumed
        max_wait = 10
        waited = 0
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            with received_lock:
                if len(received_messages) >= num_messages:
                    break
            if waited % 2 == 0:
                logger.info(f"Progress: {len(received_messages)}/{num_messages} messages received")
        
        # Stop consumer
        queue.stop_consuming()
        time.sleep(0.5)
        
        # Validate
        with received_lock:
            received = len(received_messages)
            sequences = [msg.get('seq') for msg in received_messages if 'seq' in msg]
        
        logger.info(f"\nResults: {received}/{num_messages} messages received")
        
        # Check ordering
        is_ordered = all(sequences[i] <= sequences[i+1] for i in range(len(sequences)-1))
        all_received = received == num_messages
        
        if is_ordered and all_received:
            logger.info("✓ TEST PASSED: Messages received in order and all messages received")
            return True
        else:
            logger.error(f"✗ TEST FAILED: Ordered={is_ordered}, AllReceived={all_received}")
            if not is_ordered:
                logger.error(f"First out-of-order: {sequences}")
            return False
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        return False
    finally:
        try:
            queue.close()
        except Exception:
            pass


def test_exchange_single_client():
    """Test 2: Single client using exchange, sending and receiving in order."""
    logger.info("="*60)
    logger.info("TEST 2: Exchange - Single Client")
    logger.info("="*60)
    
    host = 'localhost'
    exchange_name = 'test_exchange_single'
    queue_name = 'test_exchange_single_queue'
    
    received_messages = []
    received_lock = threading.Lock()
    consumer_ready = threading.Event()
    
    def consumer_callback(msg):
        with received_lock:
            received_messages.append(msg)
        logger.debug(f"Received: {msg}")
    
    # Create exchange middleware
    exchange = RabbitMQMiddlewareExchange(
        host=host,
        exchange_name=exchange_name,
        route_keys=['test_key'],
        exchange_type='direct',
        queue_name=queue_name
    )
    
    try:
        # Start consumer
        def run_consumer():
            consumer_ready.set()
            exchange.start_consuming(consumer_callback)
        
        cons_thread = threading.Thread(target=run_consumer, daemon=True)
        cons_thread.start()
        
        consumer_ready.wait(timeout=2)
        time.sleep(0.5)
        
        # Send messages
        num_messages = 50
        logger.info(f"Sending {num_messages} messages to exchange...")
        for i in range(num_messages):
            message = {'seq': i, 'data': f'message_{i}'}
            exchange.send(message, routing_key='test_key')
            if i % 10 == 0:
                logger.debug(f"Sent message {i}")
        
        logger.info("All messages sent. Waiting for consumption...")
        
        # Wait for messages
        max_wait = 10
        waited = 0
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            with received_lock:
                if len(received_messages) >= num_messages:
                    break
        
        exchange.stop_consuming()
        time.sleep(0.5)
        
        # Validate
        with received_lock:
            received = len(received_messages)
            sequences = [msg.get('seq') for msg in received_messages if 'seq' in msg]
        
        logger.info(f"\nResults: {received}/{num_messages} messages received")
        
        is_ordered = all(sequences[i] <= sequences[i+1] for i in range(len(sequences)-1))
        all_received = received == num_messages
        
        if is_ordered and all_received:
            logger.info("✓ TEST PASSED: Exchange messages received in order")
            return True
        else:
            logger.error(f"✗ TEST FAILED: Ordered={is_ordered}, AllReceived={all_received}")
            return False
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        try:
            exchange.close()
        except Exception:
            pass


def test_queue_two_clients_thread_aware():
    """Test 3: Two clients using ThreadAwareQueuePublisher."""
    logger.info("="*60)
    logger.info("TEST 3: Queue - Two Clients with ThreadAwarePublisher")
    logger.info("="*60)
    
    host = 'localhost'
    queue_name = 'test_queue_two_clients'
    
    received_messages = []
    received_lock = threading.Lock()
    consumer_ready = threading.Event()
    
    def consumer_callback(msg):
        with received_lock:
            received_messages.append(msg)
        logger.debug(f"Received: {msg}")
    
    # Create queue and thread-aware publisher
    def queue_factory():
        return RabbitMQMiddlewareQueue(host=host, queue_name=queue_name)
    
    thread_aware_publisher = ThreadAwareQueuePublisher(queue_factory)
    queue = queue_factory()  # For consumer
    
    try:
        # Start consumer
        def run_consumer():
            consumer_ready.set()
            queue.start_consuming(consumer_callback)
        
        cons_thread = threading.Thread(target=run_consumer, daemon=True)
        cons_thread.start()
        
        consumer_ready.wait(timeout=2)
        time.sleep(0.5)
        
        # Two client threads sending messages
        def client_producer(client_id: int, num_messages: int):
            logger.info(f"Client {client_id} starting to send {num_messages} messages...")
            for i in range(num_messages):
                message = {'client': client_id, 'seq': i, 'data': f'client_{client_id}_msg_{i}'}
                thread_aware_publisher.send(message)
                if i % 10 == 0:
                    logger.debug(f"Client {client_id} sent message {i}")
            logger.info(f"Client {client_id} finished sending")
        
        num_messages_per_client = 30
        client1_thread = threading.Thread(
            target=client_producer,
            args=(1, num_messages_per_client),
            name='client-1'
        )
        client2_thread = threading.Thread(
            target=client_producer,
            args=(2, num_messages_per_client),
            name='client-2'
        )
        
        logger.info("Starting two client threads...")
        client1_thread.start()
        client2_thread.start()
        
        client1_thread.join()
        client2_thread.join()
        
        logger.info("All clients finished. Waiting for consumption...")
        
        # Wait for messages
        max_wait = 15
        waited = 0
        total_expected = 2 * num_messages_per_client
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            with received_lock:
                if len(received_messages) >= total_expected:
                    break
            if waited % 2 == 0:
                logger.info(f"Progress: {len(received_messages)}/{total_expected} messages received")
        
        queue.stop_consuming()
        time.sleep(0.5)
        
        # Validate per client
        with received_lock:
            received = len(received_messages)
            client1_msgs = [msg for msg in received_messages if msg.get('client') == 1]
            client2_msgs = [msg for msg in received_messages if msg.get('client') == 2]
            
            client1_seqs = [msg.get('seq') for msg in client1_msgs]
            client2_seqs = [msg.get('seq') for msg in client2_msgs]
        
        logger.info(f"\nResults:")
        logger.info(f"  Total received: {received}/{total_expected}")
        logger.info(f"  Client 1: {len(client1_msgs)}/{num_messages_per_client}")
        logger.info(f"  Client 2: {len(client2_msgs)}/{num_messages_per_client}")
        
        client1_ordered = all(client1_seqs[i] <= client1_seqs[i+1] for i in range(len(client1_seqs)-1)) if len(client1_seqs) > 1 else True
        client2_ordered = all(client2_seqs[i] <= client2_seqs[i+1] for i in range(len(client2_seqs)-1)) if len(client2_seqs) > 1 else True
        all_received = received == total_expected
        
        if client1_ordered and client2_ordered and all_received:
            logger.info("✓ TEST PASSED: Both clients' messages received in order")
            return True
        else:
            logger.error(f"✗ TEST FAILED: Client1Ordered={client1_ordered}, Client2Ordered={client2_ordered}, AllReceived={all_received}")
            return False
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        try:
            thread_aware_publisher.close_all()
            queue.close()
        except Exception:
            pass


def test_exchange_two_clients_thread_aware():
    """Test 4: Two clients using ThreadAwareExchangePublisher."""
    logger.info("="*60)
    logger.info("TEST 4: Exchange - Two Clients with ThreadAwarePublisher")
    logger.info("="*60)
    
    host = 'localhost'
    exchange_name = 'test_exchange_two_clients'
    queue_name = 'test_exchange_two_clients_queue'
    
    received_messages = []
    received_lock = threading.Lock()
    consumer_ready = threading.Event()
    
    def consumer_callback(msg):
        with received_lock:
            received_messages.append(msg)
        logger.debug(f"Received: {msg}")
    
    # Create exchange and thread-aware publisher
    def exchange_factory():
        return RabbitMQMiddlewareExchange(
            host=host,
            exchange_name=exchange_name,
            route_keys=['test_key'],
            exchange_type='direct',
            queue_name=queue_name
        )
    
    thread_aware_publisher = ThreadAwareExchangePublisher(exchange_factory)
    exchange = exchange_factory()  # For consumer
    
    try:
        # Start consumer
        def run_consumer():
            consumer_ready.set()
            exchange.start_consuming(consumer_callback)
        
        cons_thread = threading.Thread(target=run_consumer, daemon=True)
        cons_thread.start()
        
        consumer_ready.wait(timeout=2)
        time.sleep(0.5)
        
        # Two client threads
        def client_producer(client_id: int, num_messages: int):
            logger.info(f"Client {client_id} starting to send {num_messages} messages...")
            for i in range(num_messages):
                message = {'client': client_id, 'seq': i, 'data': f'client_{client_id}_msg_{i}'}
                thread_aware_publisher.send(message, routing_key='test_key')
                if i % 10 == 0:
                    logger.debug(f"Client {client_id} sent message {i}")
            logger.info(f"Client {client_id} finished sending")
        
        num_messages_per_client = 30
        client1_thread = threading.Thread(
            target=client_producer,
            args=(1, num_messages_per_client),
            name='client-1'
        )
        client2_thread = threading.Thread(
            target=client_producer,
            args=(2, num_messages_per_client),
            name='client-2'
        )
        
        logger.info("Starting two client threads...")
        client1_thread.start()
        client2_thread.start()
        
        client1_thread.join()
        client2_thread.join()
        
        logger.info("All clients finished. Waiting for consumption...")
        
        # Wait for messages
        max_wait = 15
        waited = 0
        total_expected = 2 * num_messages_per_client
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            with received_lock:
                if len(received_messages) >= total_expected:
                    break
        
        exchange.stop_consuming()
        time.sleep(0.5)
        
        # Validate
        with received_lock:
            received = len(received_messages)
            client1_msgs = [msg for msg in received_messages if msg.get('client') == 1]
            client2_msgs = [msg for msg in received_messages if msg.get('client') == 2]
            
            client1_seqs = [msg.get('seq') for msg in client1_msgs]
            client2_seqs = [msg.get('seq') for msg in client2_msgs]
        
        logger.info(f"\nResults:")
        logger.info(f"  Total received: {received}/{total_expected}")
        logger.info(f"  Client 1: {len(client1_msgs)}/{num_messages_per_client}")
        logger.info(f"  Client 2: {len(client2_msgs)}/{num_messages_per_client}")
        
        client1_ordered = all(client1_seqs[i] <= client1_seqs[i+1] for i in range(len(client1_seqs)-1)) if len(client1_seqs) > 1 else True
        client2_ordered = all(client2_seqs[i] <= client2_seqs[i+1] for i in range(len(client2_seqs)-1)) if len(client2_seqs) > 1 else True
        all_received = received == total_expected
        
        if client1_ordered and client2_ordered and all_received:
            logger.info("✓ TEST PASSED: Exchange - Both clients' messages received in order")
            return True
        else:
            logger.error(f"✗ TEST FAILED: Client1Ordered={client1_ordered}, Client2Ordered={client2_ordered}, AllReceived={all_received}")
            return False
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        try:
            thread_aware_publisher.close_all()
            exchange.close()
        except Exception:
            pass


def test_queue_one_to_many():
    """Test 5: Queue - 1 producer, N consumers (load balancing)."""
    logger.info("="*60)
    logger.info("TEST 5: Queue - One Producer to Many Consumers (1:N)")
    logger.info("="*60)
    
    host = 'localhost'
    queue_name = 'test_queue_one_to_many'
    num_consumers = 3
    
    # Track messages received by each consumer
    consumers_messages = [list() for _ in range(num_consumers)]
    consumers_locks = [threading.Lock() for _ in range(num_consumers)]
    consumers_ready = [threading.Event() for _ in range(num_consumers)]
    
    def consumer_callback(consumer_id: int, msg):
        with consumers_locks[consumer_id]:
            consumers_messages[consumer_id].append(msg)
        logger.debug(f"Consumer {consumer_id} received: {msg}")
    
    # Create queue middleware for producer
    producer_queue = RabbitMQMiddlewareQueue(host=host, queue_name=queue_name)
    
    # Create consumers
    consumer_queues = []
    consumer_threads = []
    
    try:
        # Start all consumers
        for i in range(num_consumers):
            queue = RabbitMQMiddlewareQueue(host=host, queue_name=queue_name)
            consumer_queues.append(queue)
            
            def run_consumer(cons_id):
                consumers_ready[cons_id].set()
                queue.start_consuming(lambda msg: consumer_callback(cons_id, msg))
            
            thread = threading.Thread(target=run_consumer, args=(i,), daemon=True, name=f'consumer-{i}')
            consumer_threads.append(thread)
            thread.start()
        
        # Wait for all consumers to be ready
        for ready in consumers_ready:
            ready.wait(timeout=2)
        time.sleep(0.5)
        
        # Send messages from single producer
        num_messages = 30
        logger.info(f"Producer sending {num_messages} messages to {num_consumers} consumers...")
        for i in range(num_messages):
            message = {'seq': i, 'data': f'message_{i}'}
            producer_queue.send(message)
            if i % 10 == 0:
                logger.debug(f"Producer sent message {i}")
        
        logger.info("All messages sent. Waiting for consumption...")
        
        # Wait for messages to be consumed
        max_wait = 15
        waited = 0
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            total_received = sum(len(msgs) for msgs in consumers_messages)
            if total_received >= num_messages:
                break
            if waited % 2 == 0:
                logger.info(f"Progress: {total_received}/{num_messages} messages received across all consumers")
        
        # Stop all consumers
        for queue in consumer_queues:
            queue.stop_consuming()
        time.sleep(0.5)
        
        # Validate
        total_received = sum(len(msgs) for msgs in consumers_messages)
        all_sequences = []
        for i, msgs in enumerate(consumers_messages):
            sequences = [msg.get('seq') for msg in msgs if 'seq' in msg]
            all_sequences.extend(sequences)
            logger.info(f"Consumer {i} received {len(msgs)} messages: {sequences[:10]}..." if len(sequences) > 10 else f"Consumer {i} received {len(msgs)} messages: {sequences}")
        
        logger.info(f"\nResults:")
        logger.info(f"  Total messages sent: {num_messages}")
        logger.info(f"  Total messages received: {total_received}")
        logger.info(f"  Distribution: {[len(msgs) for msgs in consumers_messages]}")
        
        # Check: all messages received exactly once (no duplicates, no missing)
        all_received = total_received == num_messages
        no_duplicates = len(all_sequences) == len(set(all_sequences))
        all_present = set(all_sequences) == set(range(num_messages))
        
        if all_received and no_duplicates and all_present:
            logger.info("✓ TEST PASSED: All messages received exactly once, distributed across consumers")
            return True
        else:
            logger.error(f"✗ TEST FAILED: AllReceived={all_received}, NoDuplicates={no_duplicates}, AllPresent={all_present}")
            return False
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        try:
            producer_queue.close()
            for queue in consumer_queues:
                queue.close()
        except Exception:
            pass


def test_exchange_one_to_many():
    """Test 6: Exchange - 1 producer, N consumers (broadcast/fanout)."""
    logger.info("="*60)
    logger.info("TEST 6: Exchange - One Producer to Many Consumers (1:N)")
    logger.info("="*60)
    
    host = 'localhost'
    exchange_name = 'test_exchange_one_to_many'
    num_consumers = 3
    
    # Track messages received by each consumer
    consumers_messages = [list() for _ in range(num_consumers)]
    consumers_locks = [threading.Lock() for _ in range(num_consumers)]
    consumers_ready = [threading.Event() for _ in range(num_consumers)]
    
    def consumer_callback(consumer_id: int, msg):
        with consumers_locks[consumer_id]:
            consumers_messages[consumer_id].append(msg)
        logger.debug(f"Consumer {consumer_id} received: {msg}")
    
    # Create exchange middleware for producer
    producer_exchange = RabbitMQMiddlewareExchange(
        host=host,
        exchange_name=exchange_name,
        route_keys=['test_key'],
        exchange_type='fanout',  # Fanout broadcasts to all bound queues
        queue_name=None  # Producer doesn't need a queue
    )
    
    # Create consumers (each with its own queue bound to the exchange)
    consumer_exchanges = []
    consumer_threads = []
    
    try:
        # Start all consumers
        for i in range(num_consumers):
            queue_name = f'test_exchange_one_to_many_queue_{i}'
            exchange = RabbitMQMiddlewareExchange(
                host=host,
                exchange_name=exchange_name,
                route_keys=['test_key'],
                exchange_type='fanout',
                queue_name=queue_name
            )
            consumer_exchanges.append(exchange)
            
            def run_consumer(cons_id):
                consumers_ready[cons_id].set()
                exchange.start_consuming(lambda msg: consumer_callback(cons_id, msg))
            
            thread = threading.Thread(target=run_consumer, args=(i,), daemon=True, name=f'consumer-{i}')
            consumer_threads.append(thread)
            thread.start()
        
        # Wait for all consumers to be ready
        for ready in consumers_ready:
            ready.wait(timeout=2)
        time.sleep(0.5)
        
        # Send messages from single producer
        num_messages = 20
        logger.info(f"Producer sending {num_messages} messages to {num_consumers} consumers via fanout exchange...")
        for i in range(num_messages):
            message = {'seq': i, 'data': f'message_{i}'}
            producer_exchange.send(message, routing_key='test_key')
            if i % 5 == 0:
                logger.debug(f"Producer sent message {i}")
        
        logger.info("All messages sent. Waiting for consumption...")
        
        # Wait for messages to be consumed
        max_wait = 15
        waited = 0
        expected_total = num_messages * num_consumers  # Each message goes to all consumers
        while waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
            total_received = sum(len(msgs) for msgs in consumers_messages)
            if total_received >= expected_total:
                break
            if waited % 2 == 0:
                logger.info(f"Progress: {total_received}/{expected_total} messages received across all consumers")
        
        # Stop all consumers
        for exchange in consumer_exchanges:
            exchange.stop_consuming()
        time.sleep(0.5)
        
        # Validate
        total_received = sum(len(msgs) for msgs in consumers_messages)
        for i, msgs in enumerate(consumers_messages):
            sequences = [msg.get('seq') for msg in msgs if 'seq' in msg]
            logger.info(f"Consumer {i} received {len(msgs)} messages: {sequences[:10]}..." if len(sequences) > 10 else f"Consumer {i} received {len(msgs)} messages: {sequences}")
        
        logger.info(f"\nResults:")
        logger.info(f"  Total messages sent: {num_messages}")
        logger.info(f"  Expected total received (fanout): {expected_total}")
        logger.info(f"  Actual total received: {total_received}")
        logger.info(f"  Distribution: {[len(msgs) for msgs in consumers_messages]}")
        
        # Check: each consumer received all messages (fanout broadcasts to all)
        all_received = total_received == expected_total
        each_consumer_got_all = all(len(msgs) == num_messages for msgs in consumers_messages)
        
        if all_received and each_consumer_got_all:
            logger.info("✓ TEST PASSED: All consumers received all messages (fanout broadcast)")
            return True
        else:
            logger.error(f"✗ TEST FAILED: AllReceived={all_received}, EachConsumerGotAll={each_consumer_got_all}")
            return False
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return False
    finally:
        try:
            producer_exchange.close()
            for exchange in consumer_exchanges:
                exchange.close()
        except Exception:
            pass


def main():
    """Run all basic tests."""
    logger.info("="*60)
    logger.info("RABBITMQ MIDDLEWARE BASIC TESTS")
    logger.info("="*60)
    logger.info("")
    logger.info("Make sure RabbitMQ is running on localhost:5672")
    logger.info("")
    
    results = []
    
    # Test 1: Queue single client
    results.append(("Queue Single Client", test_queue_single_client()))
    time.sleep(1)
    
    # Test 2: Exchange single client
    results.append(("Exchange Single Client", test_exchange_single_client()))
    time.sleep(1)
    
    # Test 3: Queue two clients with ThreadAware
    results.append(("Queue Two Clients (ThreadAware)", test_queue_two_clients_thread_aware()))
    time.sleep(1)
    
    # Test 4: Exchange two clients with ThreadAware
    results.append(("Exchange Two Clients (ThreadAware)", test_exchange_two_clients_thread_aware()))
    time.sleep(1)
    
    # Test 5: Queue 1 to N (load balancing)
    results.append(("Queue One to Many (1:N)", test_queue_one_to_many()))
    time.sleep(1)
    
    # Test 6: Exchange 1 to N (broadcast/fanout)
    results.append(("Exchange One to Many (1:N)", test_exchange_one_to_many()))
    
    # Summary
    logger.info("")
    logger.info("="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{test_name}: {status}")
    
    all_passed = all(result[1] for result in results)
    if all_passed:
        logger.info("\n✓ ALL TESTS PASSED")
    else:
        logger.error("\n✗ SOME TESTS FAILED")
    
    return all_passed


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

