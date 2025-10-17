import contextlib
import os
import socket
import sys
import threading
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTILS_PATH = PROJECT_ROOT / "src" / "workers" / "utils"
if str(UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(UTILS_PATH))

from middleware import RabbitMQMiddlewareQueue
from src.workers.utils.handle_eof import EOFHandler
from src.workers.utils.message_utils import create_message_with_metadata
from src.workers.utils.middleware_config import MiddlewareConfig


def _cleanup_queue(host: str, port: int, queue_name: str) -> None:
    queue = RabbitMQMiddlewareQueue(host, queue_name, port=port)
    try:
        queue.delete()
    except Exception:  # noqa: BLE001
        pass
    finally:
        with contextlib.suppress(Exception):
            queue.close()


def _consume_messages(host: str, port: int, queue_name: str, expected_count: int, timeout: float = 5.0):
    queue = RabbitMQMiddlewareQueue(host, queue_name, port=port)
    messages = []
    done = threading.Event()

    def _callback(message):
        messages.append(message)
        if len(messages) >= expected_count:
            done.set()
            queue.stop_consuming()

    consumer_thread = threading.Thread(
        target=lambda: queue.start_consuming(_callback),
        daemon=True,
    )

    consumer_thread.start()
    done.wait(timeout=timeout)
    queue.stop_consuming()
    consumer_thread.join(timeout=timeout)
    queue.close()
    return messages


def _ensure_rabbitmq_available(host: str, port: int) -> None:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return
    except OSError as exc:
        pytest.skip(f"RabbitMQ no disponible en {host}:{port} ({exc})")


@pytest.fixture(scope="module")
def rabbitmq_endpoint():
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    return host, port


@pytest.fixture
def eof_test_env(monkeypatch, rabbitmq_endpoint):
    host, port = rabbitmq_endpoint
    _ensure_rabbitmq_available(host, port)

    input_queue = f"eof_test_input_{uuid.uuid4().hex}"
    output_queue = f"eof_test_output_{uuid.uuid4().hex}"

    _cleanup_queue(host, port, input_queue)
    _cleanup_queue(host, port, output_queue)

    monkeypatch.setenv("RABBITMQ_HOST", host)
    monkeypatch.setenv("RABBITMQ_PORT", str(port))
    monkeypatch.setenv("INPUT_QUEUE", input_queue)
    monkeypatch.delenv("INPUT_EXCHANGE", raising=False)
    monkeypatch.setenv("OUTPUT_QUEUE", output_queue)
    monkeypatch.delenv("OUTPUT_EXCHANGE", raising=False)
    monkeypatch.setenv("PREFETCH_COUNT", "2")
    monkeypatch.setenv("REPLICA_COUNT", "2")

    try:
        yield input_queue, output_queue, port, host
    finally:
        _cleanup_queue(host, port, input_queue)
        _cleanup_queue(host, port, output_queue)


def test_eof_protocol_supports_multiple_clients(monkeypatch, eof_test_env):
    input_queue, output_queue, port, host = eof_test_env

    monkeypatch.setenv("WORKER_ID", "0")
    leader_config = MiddlewareConfig()
    leader_handler = EOFHandler(leader_config)

    clients = ["client_A", "client_B"]
    for client in clients:
        message = create_message_with_metadata(client, data=None, message_type="EOF")
        leader_handler.handle_eof(message)

    leader_handler.cleanup()
    leader_config.cleanup()

    requeued_messages = _consume_messages(host, port, input_queue, expected_count=len(clients))
    assert len(requeued_messages) == len(clients)
    for message in requeued_messages:
        assert message["type"] == "EOF"
        assert message["data"] is None
        assert message.get("counter") == {"0": 1}

    monkeypatch.setenv("WORKER_ID", "1")
    replica_config = MiddlewareConfig()
    replica_handler = EOFHandler(replica_config)

    for message in requeued_messages:
        replica_handler.handle_eof(message)

    replica_handler.cleanup()
    replica_config.cleanup()

    output_messages = _consume_messages(host, port, output_queue, expected_count=len(clients))
    assert len(output_messages) == len(clients)
    assert sorted(msg["client_id"] for msg in output_messages) == sorted(clients)

    for message in output_messages:
        assert message["type"] == "EOF"
        assert message["data"] is None
        assert "counter" not in message
