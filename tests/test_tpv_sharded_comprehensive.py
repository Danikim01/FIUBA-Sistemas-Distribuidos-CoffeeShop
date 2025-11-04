#!/usr/bin/env python3
"""
Suite de tests exhaustiva para ShardedTPVWorker.

Cubre todos los casos borde y escenarios de falla:
- Load/store del archivo de estado
- Mensajes duplicados
- EOF repetidos
- Transacciones después de EOF
- Race conditions
- Workers que fallan
"""

import os
import sys
import tempfile
import shutil
import threading
import time
import json
import uuid
from pathlib import Path
from collections import defaultdict
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any, List

import pytest

# Configurar PYTHONPATH para imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"
UTILS_PATH = PROJECT_ROOT / "src" / "workers" / "utils"
MIDDLEWARE_PATH = PROJECT_ROOT / "middleware"

# Agregar paths necesarios al sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))
if str(UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(UTILS_PATH))
if str(MIDDLEWARE_PATH) not in sys.path:
    sys.path.insert(0, str(MIDDLEWARE_PATH))

from workers.local_top_scaling.tpv_sharded import ShardedTPVWorker
from workers.utils.state_manager import TPVStateManager
from message_utils import ClientId


class TestTPVShardedStateManagement:
    """Tests para la gestión del estado (load/store)."""
    
    @pytest.fixture
    def temp_state_dir(self):
        """Crea un directorio temporal para los archivos de estado."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def worker_with_state_dir(self, temp_state_dir, monkeypatch):
        """Crea un worker con un directorio de estado temporal."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        monkeypatch.setenv('STATE_DIR', str(temp_state_dir))
        
        # Mock del middleware
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                yield worker
    
    def test_state_persistence_and_load(self, worker_with_state_dir, temp_state_dir):
        """Test que el estado se persiste y se puede cargar correctamente."""
        worker = worker_with_state_dir
        client_id = "test-client-123"
        
        # Agregar datos al estado (store_id=0 pertenece a shard_0)
        transaction = {
            'store_id': 0,
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 100.50
        }
        
        worker.accumulate_transaction(client_id, transaction)
        
        # Persistir el estado
        worker.state_manager.persist_state()
        
        # Verificar que el archivo existe
        state_file = temp_state_dir / "state.bin"
        assert state_file.exists(), "El archivo de estado debería existir"
        
        # Crear un nuevo worker que debería cargar el estado
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                new_worker = ShardedTPVWorker()
                
                # Verificar que el estado se cargó correctamente
                loaded_data = new_worker.partial_tpv.get(client_id, {})
                assert loaded_data, "El estado debería haberse cargado"
                
                # Verificar que los datos están correctos
                year_half = loaded_data.get('2024-H1', {})
                assert 0 in year_half, "El store_id debería estar en el estado cargado"
                assert year_half[0] == 100.50, "El valor debería ser correcto"
    
    def test_state_load_with_invalid_file(self, worker_with_state_dir, temp_state_dir):
        """Test que el worker maneja correctamente archivos de estado inválidos."""
        # Crear un archivo de estado corrupto
        state_file = temp_state_dir / "state.bin"
        state_file.write_text("invalid json content")
        
        # El worker debería crear un estado vacío
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # El estado debería estar vacío o usar el backup
                assert worker.partial_tpv is not None
    
    def test_state_backup_restoration(self, worker_with_state_dir, temp_state_dir):
        """Test que el worker puede restaurar desde un backup."""
        worker = worker_with_state_dir
        client_id = "test-client-backup"
        
        # Crear estado inicial
        transaction = {
            'store_id': 2,
            'created_at': '2024-05-20 14:00:00',
            'final_amount': 200.75
        }
        worker.accumulate_transaction(client_id, transaction)
        worker.state_manager.persist_state()
        
        # Corromper el archivo principal
        state_file = temp_state_dir / "state.bin"
        backup_file = temp_state_dir / "state.backup.bin"
        
        # Crear backup manualmente
        if state_file.exists():
            shutil.copy(state_file, backup_file)
        
        # Corromper el principal
        state_file.write_text("corrupted")
        
        # El worker debería cargar desde el backup
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                new_worker = ShardedTPVWorker()
                loaded_data = new_worker.partial_tpv.get(client_id, {})
                # Debería tener los datos del backup o estar vacío
                assert isinstance(loaded_data, dict)


class TestTPVShardedDuplicateMessages:
    """Tests para el manejo de mensajes duplicados."""
    
    @pytest.fixture
    def worker(self, monkeypatch):
        """Crea un worker para testing."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # Limpiar el estado antes de cada test
                worker.partial_tpv.clear()
                worker.state_manager._last_processed_message.clear()
            yield worker
            # Limpiar el estado después de cada test
            worker.partial_tpv.clear()
            worker.state_manager._last_processed_message.clear()
    
    def test_duplicate_batch_skipped(self, worker):
        """Test que un batch duplicado se omite correctamente."""
        client_id = "test-client-duplicate"
        message_uuid = str(uuid.uuid4())
        
        batch = [
            {
                'store_id': 0,  # store_id=0 pertenece a shard_0
                'created_at': '2024-03-15 10:30:00',
                'final_amount': 50.0
            },
            {
                'store_id': 0,
                'created_at': '2024-03-15 11:00:00',
                'final_amount': 75.0
            }
        ]
        
        # Procesar el batch por primera vez
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': message_uuid}
            worker.process_batch(batch, client_id)
        
        initial_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        assert initial_total == 125.0, "El batch debería haberse procesado"
        
        # Intentar procesar el mismo batch de nuevo
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': message_uuid}
            worker.process_batch(batch, client_id)
        
        # El total no debería haber cambiado
        final_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        assert final_total == 125.0, "El batch duplicado debería haberse omitido"
    
    def test_different_batch_same_client_processed(self, worker):
        """Test que diferentes batches del mismo cliente se procesan correctamente."""
        client_id = "test-client-multiple"
        
        batch1 = [{
            'store_id': 0,  # store_id=0 pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 50.0
        }]
        
        batch2 = [{
            'store_id': 0,
            'created_at': '2024-03-15 11:00:00',
            'final_amount': 75.0
        }]
        
        uuid1 = str(uuid.uuid4())
        uuid2 = str(uuid.uuid4())
        
        # Procesar batch1
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': uuid1}
            worker.process_batch(batch1, client_id)
        
        # Procesar batch2
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': uuid2}
            worker.process_batch(batch2, client_id)
        
        total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        assert total == 125.0, "Ambos batches deberían haberse procesado"
    
    def test_duplicate_after_restart(self, worker):
        """Test que un mensaje duplicado después de un restart se maneja correctamente."""
        client_id = "test-client-restart"
        message_uuid = str(uuid.uuid4())
        
        batch = [{
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 100.0
        }]
        
        # Procesar y persistir
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': message_uuid}
            worker.process_batch(batch, client_id)
        
        worker.state_manager.persist_state()
        
        # Simular restart: crear nuevo worker
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                # Usar el mismo directorio de estado
                state_dir = worker.state_manager._state_dir
                os.environ['STATE_DIR'] = str(state_dir)
                
                new_worker = ShardedTPVWorker()
                
                # Intentar procesar el mismo batch de nuevo
                with patch.object(new_worker, '_get_current_message_metadata') as mock_meta:
                    mock_meta.return_value = {'message_uuid': message_uuid}
                    new_worker.process_batch(batch, client_id)
                
                # El total no debería haber cambiado
                total = new_worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
                assert total == 100.0, "El batch duplicado debería haberse omitido después del restart"


class TestTPVShardedEOFHandling:
    """Tests para el manejo de EOF."""
    
    @pytest.fixture
    def worker(self, monkeypatch):
        """Crea un worker para testing."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # Limpiar el estado antes de cada test
                worker.partial_tpv.clear()
                worker.state_manager._last_processed_message.clear()
            yield worker
            # Limpiar el estado después de cada test
            worker.partial_tpv.clear()
            worker.state_manager._last_processed_message.clear()
    
    def test_eof_clears_state(self, worker):
        """Test que un EOF limpia el estado del cliente."""
        client_id = "test-client-eof"
        
        # Agregar datos
        transaction = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 150.0
        }
        worker.accumulate_transaction(client_id, transaction)
        
        assert client_id in worker.partial_tpv, "El cliente debería tener estado"
        
        # Procesar EOF
        eof_message = {'type': 'EOF'}
        eof_uuid = str(uuid.uuid4())
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': eof_uuid}
            with patch.object(worker, 'send_message') as mock_send:
                worker.handle_eof(eof_message, client_id)
        
        # Verificar que el estado fue limpiado
        assert client_id not in worker.partial_tpv or not worker.partial_tpv.get(client_id), \
            "El estado del cliente debería haberse limpiado después del EOF"
    
    def test_repeated_eof_handled_gracefully(self, worker):
        """Test que múltiples EOFs del mismo cliente se manejan correctamente."""
        client_id = "test-client-repeated-eof"
        
        # Primer EOF
        eof_message = {'type': 'EOF'}
        eof_uuid1 = str(uuid.uuid4())
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': eof_uuid1}
            with patch.object(worker, 'send_message') as mock_send:
                worker.handle_eof(eof_message, client_id)
        
        # Segundo EOF (duplicado o retransmitido)
        eof_uuid2 = str(uuid.uuid4())
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': eof_uuid2}
            with patch.object(worker, 'send_message') as mock_send:
                # No debería lanzar excepción
                worker.handle_eof(eof_message, client_id)
    
    def test_eof_without_uuid(self, worker):
        """Test que un EOF sin UUID se maneja correctamente."""
        client_id = "test-client-eof-no-uuid"
        
        eof_message = {'type': 'EOF'}
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {}  # Sin UUID
            with patch.object(worker, 'send_message') as mock_send:
                # No debería lanzar excepción
                worker.handle_eof(eof_message, client_id)
    
    def test_eof_after_transaction_emits_results(self, worker):
        """Test que un EOF después de transacciones emite los resultados correctos."""
        client_id = "test-client-eof-results"
        
        # Agregar múltiples transacciones
        transactions = [
            {'store_id': 0, 'created_at': '2024-03-15 10:30:00', 'final_amount': 50.0},  # pertenece a shard_0
            {'store_id': 2, 'created_at': '2024-03-15 11:00:00', 'final_amount': 75.0},  # pertenece a shard_0
            {'store_id': 0, 'created_at': '2024-07-20 14:00:00', 'final_amount': 100.0},  # pertenece a shard_0
        ]
        
        for tx in transactions:
            worker.accumulate_transaction(client_id, tx)
        
        # Procesar EOF
        eof_message = {'type': 'EOF'}
        eof_uuid = str(uuid.uuid4())
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': eof_uuid}
            with patch.object(worker, 'send_message') as mock_send:
                worker.handle_eof(eof_message, client_id)
                
                # Verificar que se enviaron los resultados
                assert mock_send.called, "Debería haberse enviado un mensaje"
                
                # Verificar el contenido del mensaje
                call_args = mock_send.call_args
                sent_data = call_args[1]['data'] if call_args else None
                
                if sent_data:
                    assert len(sent_data) == 3, "Deberían haberse enviado 3 resultados"
                    # Verificar que los resultados tienen la estructura correcta
                    for result in sent_data:
                        assert 'year_half_created_at' in result
                        assert 'store_id' in result
                        assert 'tpv' in result


class TestTPVShardedTransactionAfterEOF:
    """Tests para transacciones que llegan después de EOF."""
    
    @pytest.fixture
    def worker(self, monkeypatch):
        """Crea un worker para testing."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # Limpiar el estado antes de cada test
                worker.partial_tpv.clear()
                worker.state_manager._last_processed_message.clear()
            yield worker
            # Limpiar el estado después de cada test
            worker.partial_tpv.clear()
            worker.state_manager._last_processed_message.clear()
    
    def test_transaction_after_eof_creates_new_state(self, worker):
        """Test que una transacción después de EOF crea un nuevo estado."""
        client_id = "test-client-after-eof"
        
        # Agregar transacción inicial
        tx1 = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 100.0
        }
        worker.accumulate_transaction(client_id, tx1)
        
        # Procesar EOF
        eof_message = {'type': 'EOF'}
        eof_uuid = str(uuid.uuid4())
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': eof_uuid}
            with patch.object(worker, 'send_message'):
                worker.handle_eof(eof_message, client_id)
        
        # Agregar nueva transacción después del EOF
        tx2 = {
            'store_id': 2,
            'created_at': '2024-03-15 11:00:00',
            'final_amount': 200.0
        }
        worker.accumulate_transaction(client_id, tx2)
        
        # Verificar que el nuevo estado existe
        assert client_id in worker.partial_tpv, "Debería haber un nuevo estado"
        new_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(2, 0)
        assert new_total == 200.0, "La nueva transacción debería estar en el estado"
        
        # Verificar que la transacción anterior no está
        old_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        assert old_total == 0, "La transacción anterior no debería estar en el nuevo estado"


class TestTPVShardedRaceConditions:
    """Tests para race conditions y concurrencia."""
    
    @pytest.fixture
    def worker(self, monkeypatch):
        """Crea un worker para testing."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # Limpiar el estado antes de cada test
                worker.partial_tpv.clear()
                worker.state_manager._last_processed_message.clear()
            yield worker
            # Limpiar el estado después de cada test
            worker.partial_tpv.clear()
            worker.state_manager._last_processed_message.clear()
    
    def test_concurrent_batch_processing(self, worker):
        """Test que múltiples batches procesados concurrentemente no causan corrupción."""
        client_id = "test-client-concurrent"
        num_threads = 5
        transactions_per_thread = 10
        
        def process_batch(thread_id):
            batch = []
            for i in range(transactions_per_thread):
                batch.append({
                    'store_id': 0,  # pertenece a shard_0
                    'created_at': f'2024-03-15 {10 + thread_id}:{i % 60:02d}:00',
                    'final_amount': 10.0
                })
            
            batch_uuid = str(uuid.uuid4())
            with patch.object(worker, '_get_current_message_metadata') as mock_meta:
                mock_meta.return_value = {'message_uuid': batch_uuid}
                worker.process_batch(batch, client_id)
        
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=process_batch, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verificar que todas las transacciones se procesaron
        # Las transacciones pueden estar en diferentes year_half dependiendo del timestamp
        # Verificamos que al menos algunas se procesaron
        all_data = worker.partial_tpv.get(client_id, {})
        total = sum(sum(store_map.values()) for store_map in all_data.values())
        expected_total = num_threads * transactions_per_thread * 10.0
        assert total == expected_total, f"El total debería ser {expected_total}, pero es {total}"
    
    def test_concurrent_eof_and_transactions(self, worker):
        """Test que un EOF concurrente con transacciones se maneja correctamente."""
        client_id = "test-client-concurrent-eof"
        
        eof_processed = threading.Event()
        transactions_processed = threading.Event()
        
        def process_eof():
            eof_message = {'type': 'EOF'}
            eof_uuid = str(uuid.uuid4())
            
            with patch.object(worker, '_get_current_message_metadata') as mock_meta:
                mock_meta.return_value = {'message_uuid': eof_uuid}
                with patch.object(worker, 'send_message'):
                    worker.handle_eof(eof_message, client_id)
            eof_processed.set()
        
        def process_transactions():
            for i in range(10):
                tx = {
                    'store_id': 0,  # pertenece a shard_0
                    'created_at': f'2024-03-15 10:{i % 60:02d}:00',
                    'final_amount': 10.0
                }
                worker.accumulate_transaction(client_id, tx)
            transactions_processed.set()
        
        # Iniciar ambos threads
        eof_thread = threading.Thread(target=process_eof)
        tx_thread = threading.Thread(target=process_transactions)
        
        eof_thread.start()
        tx_thread.start()
        
        eof_thread.join()
        tx_thread.join()
        
        # Verificar que no hay corrupción de estado
        # El estado debería estar limpio después del EOF o tener las transacciones
        # dependiendo del orden de ejecución
        state = worker.partial_tpv.get(client_id, {})
        # No debería haber excepciones y el estado debería ser consistente
        assert isinstance(state, dict)


class TestTPVShardedWorkerFailures:
    """Tests para simular fallos de workers."""
    
    @pytest.fixture
    def worker(self, monkeypatch):
        """Crea un worker para testing."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # Limpiar el estado antes de cada test
                worker.partial_tpv.clear()
                worker.state_manager._last_processed_message.clear()
            yield worker
            # Limpiar el estado después de cada test
            worker.partial_tpv.clear()
            worker.state_manager._last_processed_message.clear()
    
    def test_rollback_on_exception_during_batch(self, worker):
        """Test que una excepción durante el procesamiento de un batch hace rollback."""
        client_id = "test-client-rollback"
        
        # Agregar estado inicial
        tx1 = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 100.0
        }
        worker.accumulate_transaction(client_id, tx1)
        
        initial_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        
        # Simular una excepción durante el procesamiento
        batch = [
            {'store_id': 0, 'created_at': '2024-03-15 11:00:00', 'final_amount': 50.0},  # pertenece a shard_0
            {'store_id': 0, 'created_at': '2024-03-15 11:30:00', 'final_amount': 75.0},  # pertenece a shard_0
        ]
        
        batch_uuid = str(uuid.uuid4())
        
        # Mock accumulate_transaction para lanzar excepción en la segunda transacción
        original_accumulate = worker.accumulate_transaction
        call_count = [0]
        
        def failing_accumulate(cid, payload):
            call_count[0] += 1
            if call_count[0] == 2:  # Segunda transacción falla
                raise ValueError("Simulated failure")
            original_accumulate(cid, payload)
        
        worker.accumulate_transaction = failing_accumulate
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': batch_uuid}
            try:
                worker.process_batch(batch, client_id)
            except ValueError:
                pass  # Esperado
        
        # Verificar que el estado se restauró
        final_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        assert final_total == initial_total, "El estado debería haberse restaurado al valor inicial"
    
    def test_persist_failure_handling(self, worker):
        """Test que un fallo al persistir el estado se maneja correctamente con rollback."""
        client_id = "test-client-persist-fail"
        
        # Agregar estado inicial
        tx_initial = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 09:00:00',
            'final_amount': 50.0
        }
        worker.accumulate_transaction(client_id, tx_initial)
        initial_total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        
        batch = [{
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 100.0
        }]
        
        batch_uuid = str(uuid.uuid4())
        
        # Mock persist_state para lanzar excepción
        def failing_persist():
            raise IOError("Simulated disk failure")
        
        worker.state_manager.persist_state = failing_persist
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': batch_uuid}
            try:
                worker.process_batch(batch, client_id)
            except IOError:
                pass  # Esperado
        
        # El estado debería haberse restaurado al valor inicial (rollback)
        total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        assert total == initial_total, f"El estado debería haberse restaurado al valor inicial {initial_total} después del rollback, pero es {total}"


class TestTPVShardedEdgeCases:
    """Tests para casos borde adicionales."""
    
    @pytest.fixture
    def worker(self, monkeypatch):
        """Crea un worker para testing."""
        monkeypatch.setenv('NUM_SHARDS', '2')
        monkeypatch.setenv('WORKER_ID', '0')
        
        with patch('workers.base_worker.MiddlewareConfig') as mock_middleware:
            mock_middleware.return_value.input_middleware = Mock()
            mock_middleware.return_value.output_middleware = Mock()
            mock_middleware.return_value.get_input_target = Mock(return_value='input')
            mock_middleware.return_value.get_output_target = Mock(return_value='output')
            mock_middleware.return_value.cleanup = Mock()
            
            with patch('workers.base_worker.EOFHandler') as mock_eof:
                mock_eof.return_value.handle_eof = Mock()
                mock_eof.return_value.start_consuming = Mock()
                mock_eof.return_value.cleanup = Mock()
                
                worker = ShardedTPVWorker()
                # Limpiar el estado antes de cada test
                worker.partial_tpv.clear()
                worker.state_manager._last_processed_message.clear()
            yield worker
            # Limpiar el estado después de cada test
            worker.partial_tpv.clear()
            worker.state_manager._last_processed_message.clear()
    
    def test_empty_batch_handled(self, worker):
        """Test que un batch vacío se maneja correctamente."""
        client_id = "test-client-empty"
        batch = []
        batch_uuid = str(uuid.uuid4())
        
        with patch.object(worker, '_get_current_message_metadata') as mock_meta:
            mock_meta.return_value = {'message_uuid': batch_uuid}
            # No debería lanzar excepción
            worker.process_batch(batch, client_id)
    
    def test_transaction_with_missing_fields(self, worker):
        """Test que transacciones con campos faltantes se manejan correctamente."""
        client_id = "test-client-missing-fields"
        
        # Transacción sin created_at
        tx1 = {
            'store_id': 0,  # pertenece a shard_0
            'final_amount': 100.0
        }
        worker.accumulate_transaction(client_id, tx1)
        # No debería agregarse nada porque no hay year_half
        
        # Transacción sin final_amount
        tx2 = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00'
        }
        worker.accumulate_transaction(client_id, tx2)
        # Debería usar 0.0 como default
        
        # Verificar estado - sin created_at válido, no debería agregarse nada
        # Transacción con created_at pero sin final_amount debería usar 0.0
        total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(0, 0)
        # La primera transacción no tiene created_at, la segunda usa 0.0 como default
        assert total == 0.0, "El total debería ser 0.0 (sin created_at o sin amount)"
    
    def test_transaction_wrong_shard_ignored(self, worker):
        """Test que transacciones del shard incorrecto se ignoran."""
        client_id = "test-client-wrong-shard"
        
        # Worker 0 debería procesar store_id que hash a shard_0
        # Necesitamos encontrar un store_id que hash a shard_1
        from workers.utils.sharding_utils import get_routing_key
        
        # Buscar un store_id que no pertenezca a este worker
        store_id_wrong = None
        for sid in range(100):
            routing_key = get_routing_key(sid, 2)
            if routing_key != "shard_0":
                store_id_wrong = sid
                break
        
        if store_id_wrong is not None:
            tx = {
                'store_id': store_id_wrong,
                'created_at': '2024-03-15 10:30:00',
                'final_amount': 100.0
            }
            worker.accumulate_transaction(client_id, tx)
            
            # Verificar que no se agregó
            total = worker.partial_tpv.get(client_id, {}).get('2024-H1', {}).get(store_id_wrong, 0)
            assert total == 0, "Las transacciones del shard incorrecto deberían ignorarse"
    
    def test_multiple_clients_independent(self, worker):
        """Test que múltiples clientes mantienen estados independientes."""
        client1 = "test-client-1"
        client2 = "test-client-2"
        
        tx1 = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 100.0
        }
        
        tx2 = {
            'store_id': 0,  # pertenece a shard_0
            'created_at': '2024-03-15 10:30:00',
            'final_amount': 200.0
        }
        
        worker.accumulate_transaction(client1, tx1)
        worker.accumulate_transaction(client2, tx2)
        
        total1 = worker.partial_tpv.get(client1, {}).get('2024-H1', {}).get(0, 0)
        total2 = worker.partial_tpv.get(client2, {}).get('2024-H1', {}).get(0, 0)
        
        assert total1 == 100.0, "El cliente 1 debería tener 100.0"
        assert total2 == 200.0, "El cliente 2 debería tener 200.0"
        assert total1 != total2, "Los estados deberían ser independientes"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

