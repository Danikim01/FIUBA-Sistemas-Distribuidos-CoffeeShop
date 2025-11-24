import os
from typing import Dict

from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.metadata_store.metadata_store import MetadataStore
from workers.utils.metadata_persistence_store import MetadataPersistenceStore

UserId = str
Birthday = str

id_column = "user_id"
birthday_column = "birthdate"

class UsersMetadataStore(MetadataStore):
    """Stores {user_id: birthdate} per client, backed by JSON Lines persistence on disk."""

    def __init__(self, middleware_config: MiddlewareConfig):
        clients_queue = os.getenv("CLIENTS_QUEUE", "users_raw").strip()
        middleware = middleware_config.create_queue(clients_queue)
        super().__init__(clients_queue, middleware)

        # Cache en memoria para acceso rápido
        self.data: dict[ClientId, Dict[UserId, Birthday]] = {}
        
        self._persistence = MetadataPersistenceStore(
            store_type='users',
            id_key='user_id',
            value_key='birthday'
        )
        

    def reset_state(self, client_id: ClientId):
        """Reset the internal state of the metadata store."""
        self.data.pop(client_id, None)
        self._persistence.clear_client(client_id)

    def save_data(self, data: dict):
        """Save the message to disk or process it as needed."""
        user_id = UserId(data.get(id_column, ""))
        birthday = Birthday(data.get(birthday_column, ""))
        
        if not user_id or not birthday:
            return
        
        # Actualizar cache en memoria
        self.data.setdefault(self.current_client_id, {})[user_id] = birthday
        
        # Persistir en disco (append-only)
        self._persistence.save_item(self.current_client_id, user_id, birthday)

    def save_batch(self, data: list):
        """Save a batch of messages to disk or process them as needed."""
        items: Dict[str, str] = {}
        
        for item in data:
            user_id = UserId(item.get(id_column, ""))
            birthday = Birthday(item.get(birthday_column, ""))
            if user_id and birthday:
                items[user_id] = birthday
        
        if not items:
            return
        
        # Actualizar cache
        self.data.setdefault(self.current_client_id, {}).update(items)
        
        # Persistir batch en disco
        self._persistence.save_batch(self.current_client_id, items)

    def _get_item(self, client_id: ClientId, item_id: str) -> Birthday:
        """Get item from cache or persistence."""
        # Primero buscar en cache en memoria
        users = self.data.get(client_id, {})
        if item_id in users:
            return users[item_id]
        
        # Si no está en cache, buscar en persistencia
        birthday = self._persistence.get_item(client_id, item_id)
        
        # Si encontramos el item en persistencia, asegurarnos de que esté en cache
        if birthday != "Unknown Birthday":
            # Si el cliente no está en cache, cargar todos los datos desde persistencia
            if client_id not in self.data:
                persisted_data = self._persistence.get_all_items(client_id)
                if persisted_data:
                    self.data[client_id] = persisted_data
                    # Retornar el valor del cache ahora
                    return self.data[client_id].get(item_id, "Unknown Birthday")
            else:
                # El cliente está en cache pero sin este item, actualizar cache con este item
                self.data[client_id][item_id] = birthday
        
        return birthday