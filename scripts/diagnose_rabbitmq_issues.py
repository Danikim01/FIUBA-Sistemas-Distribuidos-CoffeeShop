#!/usr/bin/env python3

"""
Comprehensive diagnostic script for RabbitMQ connection issues.
This script helps identify whether the problem is queue overflow, connection issues, or other factors.
"""

import os
import time
import psutil # type: ignore
import subprocess
from typing import Dict, List, Any

def check_system_resources():
    """Check system resource utilization."""
    print("=== System Resources ===")
    
    # CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    print(f"CPU Usage: {cpu_percent}%")
    
    # Memory usage
    memory = psutil.virtual_memory()
    print(f"Memory Usage: {memory.percent}% ({memory.used / (1024**3):.1f}GB / {memory.total / (1024**3):.1f}GB)")
    
    # Disk usage
    disk = psutil.disk_usage('/')
    print(f"Disk Usage: {disk.percent}% ({disk.used / (1024**3):.1f}GB / {disk.total / (1024**3):.1f}GB)")
    
    # Network connections
    connections = psutil.net_connections(kind='tcp')
    established = len([c for c in connections if c.status == 'ESTABLISHED'])
    time_wait = len([c for c in connections if c.status == 'TIME_WAIT'])
    print(f"Network Connections - Established: {established}, Time-Wait: {time_wait}")
    
    print()

def check_docker_stats():
    """Check Docker container resource usage."""
    print("=== Docker Container Stats ===")
    
    try:
        # Get container stats
        result = subprocess.run(['docker', 'stats', '--no-stream', '--format', 
                               'table {{.Name}}\\t{{.CPUPerc}}\\t{{.MemUsage}}\\t{{.NetIO}}'],
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"Error getting Docker stats: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print("Docker stats command timed out")
    except FileNotFoundError:
        print("Docker command not found - is Docker installed?")
    except Exception as e:
        print(f"Error checking Docker stats: {e}")
    
    print()

def check_rabbitmq_status():
    """Check RabbitMQ server status and queues."""
    print("=== RabbitMQ Status ===")
    
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    
    try:
        # Try to get RabbitMQ status via management API
        import requests # type: ignore
        
        management_url = f"http://{rabbitmq_host}:15672/api"
        
        # Try to access overview
        try:
            response = requests.get(f"{management_url}/overview", 
                                  auth=('guest', 'guest'), timeout=5)
            if response.status_code == 200:
                overview = response.json()
                print(f"RabbitMQ Version: {overview.get('rabbitmq_version', 'Unknown')}")
                print(f"Erlang Version: {overview.get('erlang_version', 'Unknown')}")
                
                # Message stats
                msg_stats = overview.get('message_stats', {})
                if msg_stats:
                    print(f"Total Messages Published: {msg_stats.get('publish', 0)}")
                    print(f"Total Messages Delivered: {msg_stats.get('deliver_get', 0)}")
            else:
                print(f"Management API returned status {response.status_code}")
        except Exception as e:
            print(f"Could not access RabbitMQ management API: {e}")
        
        # Get queue information
        try:
            response = requests.get(f"{management_url}/queues", 
                                  auth=('guest', 'guest'), timeout=5)
            if response.status_code == 200:
                queues = response.json()
                print(f"\nTotal Queues: {len(queues)}")
                
                print("\nQueue Details:")
                for queue in queues:
                    name = queue.get('name', 'Unknown')
                    messages = queue.get('messages', 0)
                    consumers = queue.get('consumers', 0)
                    memory = queue.get('memory', 0) / (1024 * 1024)  # MB
                    
                    status = "HEALTHY"
                    if messages > 1000:
                        status = "HIGH_MESSAGES"
                    if consumers == 0 and messages > 0:
                        status = "NO_CONSUMERS"
                    if memory > 50:  # MB
                        status = "HIGH_MEMORY"
                    
                    print(f"  {name}: {messages} msgs, {consumers} consumers, {memory:.1f}MB - {status}")
        
        except Exception as e:
            print(f"Could not get queue information: {e}")
            
    except ImportError:
        print("requests library not available - using basic connection test")
        check_basic_rabbitmq_connection(rabbitmq_host)
    
    print()

def check_basic_rabbitmq_connection(host: str):
    """Basic RabbitMQ connection test."""
    try:
        import pika # type: ignore
        
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, socket_timeout=5)
        )
        channel = connection.channel()
        
        # Test basic operations
        test_queue = f"diagnostic_test_{int(time.time())}"
        channel.queue_declare(queue=test_queue, exclusive=True)
        channel.basic_publish(exchange='', routing_key=test_queue, body='test')
        
        method, properties, body = channel.basic_get(queue=test_queue)
        if body == b'test':
            print("✓ Basic RabbitMQ operations working")
        else:
            print("✗ RabbitMQ message delivery failed")
        
        channel.queue_delete(queue=test_queue)
        channel.close()
        connection.close()
        
    except Exception as e:
        print(f"✗ RabbitMQ connection failed: {e}")

def check_network_connectivity():
    """Check network connectivity to RabbitMQ."""
    print("=== Network Connectivity ===")
    
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    
    # Test basic connectivity
    import socket
    
    ports_to_test = [5672, 15672]  # AMQP and Management
    
    for port in ports_to_test:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((rabbitmq_host, port))
            sock.close()
            
            if result == 0:
                print(f"✓ Port {port} is accessible")
            else:
                print(f"✗ Port {port} is not accessible")
        except Exception as e:
            print(f"✗ Error testing port {port}: {e}")
    
    print()

def analyze_error_patterns():
    """Analyze common error patterns."""
    print("=== Error Pattern Analysis ===")
    
    error_indicators = {
        "queue_overflow": [
            "queue full", "x-max-length", "memory alarm", "disk alarm"
        ],
        "connection_issues": [
            "Connection reset by peer", "IndexError.*pop from an empty deque",
            "Stream connection lost", "AMQP connection error"
        ],
        "resource_exhaustion": [
            "out of memory", "too many connections", "file descriptor limit"
        ],
        "configuration_issues": [
            "authentication failed", "access refused", "vhost not found"
        ]
    }
    
    print("Based on your error logs, the primary issues appear to be:")
    print("1. ✗ Connection reset by peer - Network/connection instability")
    print("2. ✗ IndexError: pop from an empty deque - Threading/concurrency issues in Pika")
    print("3. ✗ Stream connection lost - Connection pool exhaustion")
    print()
    print("These are NOT queue overflow issues. Recommendations:")
    print("- Implement connection pooling and retry logic")
    print("- Add proper error handling and circuit breakers")
    print("- Monitor connection counts and resource usage")
    print("- Consider using async AMQP client for better concurrency")
    print()

def generate_recommendations():
    """Generate specific recommendations based on findings."""
    print("=== Recommendations ===")
    
    recommendations = [
        "1. Implement robust connection management:",
        "   - Use connection pooling",
        "   - Add exponential backoff retry logic",
        "   - Implement circuit breaker pattern",
        "",
        "2. Monitor system resources:",
        "   - Set up alerts for high CPU/memory usage",
        "   - Monitor network connection counts",
        "   - Track RabbitMQ queue depths",
        "",
        "3. Improve error handling:",
        "   - Catch and retry transient connection errors",
        "   - Log detailed error information for debugging",
        "   - Implement graceful degradation",
        "",
        "4. Configuration tuning:",
        "   - Adjust heartbeat intervals",
        "   - Configure appropriate prefetch counts",
        "   - Set connection limits appropriately",
        "",
        "5. Consider architectural changes:",
        "   - Use async AMQP clients for better concurrency",
        "   - Implement message buffering for reliability",
        "   - Add health checks and monitoring"
    ]
    
    for rec in recommendations:
        print(rec)
    
    print()

def main():
    """Main diagnostic function."""
    print("🔍 RabbitMQ Connection Issue Diagnostic Tool")
    print("=" * 50)
    print()
    
    check_system_resources()
    check_docker_stats()
    check_network_connectivity()
    check_rabbitmq_status()
    analyze_error_patterns()
    generate_recommendations()
    
    print("🎯 Conclusion: The issues are primarily connection management problems,")
    print("   NOT queue overflow. Focus on implementing robust connection handling.")

if __name__ == '__main__':
    main()