"""Client session management for multi-client support."""

import uuid
import logging
import threading
import socket
from typing import Dict, Set, Optional
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ClientSession:
    """Represents an active client session."""
    client_id: str
    socket: socket.socket
    address: tuple
    created_at: datetime
    eof_received: Dict[str, bool]  # Track EOF status for each data type
    
    def __post_init__(self):
        """Initialize EOF tracking for all data types."""
        if not self.eof_received:
            self.eof_received = {
                'USERS': False,
                'TRANSACTIONS': False,
                'TRANSACTION_ITEMS': False,
                'STORES': False,
                'MENU_ITEMS': False
            }


class ClientSessionManager:
    """Manages multiple client sessions with thread-safe operations."""
    
    def __init__(self):
        self._sessions: Dict[str, ClientSession] = {}
        self._lock = threading.RLock()
        logger.info("Client session manager initialized")
    
    def create_session(self, client_socket: socket.socket, address: tuple) -> str:
        """Create a new client session and return the client ID."""
        client_id = str(uuid.uuid4())
        
        session = ClientSession(
            client_id=client_id,
            socket=client_socket,
            address=address,
            created_at=datetime.now(),
            eof_received={}
        )
        
        with self._lock:
            self._sessions[client_id] = session
        
        logger.info(f"Created new client session: {client_id} for {address}")
        return client_id
    
    def get_session(self, client_id: str) -> Optional[ClientSession]:
        """Get a client session by ID."""
        with self._lock:
            return self._sessions.get(client_id)
    
    def remove_session(self, client_id: str) -> Optional[ClientSession]:
        """Remove and return a client session."""
        with self._lock:
            session = self._sessions.pop(client_id, None)
            if session:
                logger.info(f"Removed client session: {client_id}")
            return session
    
    def get_all_sessions(self) -> Dict[str, ClientSession]:
        """Get a copy of all active sessions."""
        with self._lock:
            return self._sessions.copy()
    
    def update_eof_status(self, client_id: str, data_type: str, status: bool) -> bool:
        """Update EOF status for a data type. Returns True if all EOFs received."""
        with self._lock:
            session = self._sessions.get(client_id)
            if not session:
                logger.warning(f"Attempted to update EOF for non-existent session: {client_id}")
                return False
            
            session.eof_received[data_type] = status
            all_eof_received = all(session.eof_received.values())
            
            if all_eof_received:
                logger.info(f"All EOFs received for client {client_id}")
            
            return all_eof_received
    
    def get_active_client_count(self) -> int:
        """Get the number of active client sessions."""
        with self._lock:
            return len(self._sessions)
    
    def cleanup_session(self, client_id: str) -> None:
        """Clean up resources for a session."""
        session = self.remove_session(client_id)
        if session:
            try:
                session.socket.close()
            except Exception as e:
                logger.error(f"Error closing socket for client {client_id}: {e}")