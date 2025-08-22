"""
Optimized client management for scalable data pipelines.
Implements singleton pattern and connection pooling to prevent resource bloat.
"""

import logging
from typing import Dict, Optional, Any
from threading import Lock
import time
from contextlib import contextmanager


class ClientManager:
    """
    Singleton client manager for Google Cloud services.
    Prevents resource bloat by reusing clients and implementing proper lifecycle management.
    """
    
    _instance = None
    _lock = Lock()
    _clients: Dict[str, Any] = {}
    _last_used: Dict[str, float] = {}
    _client_locks: Dict[str, Lock] = {}
    
    # Client timeout settings (in seconds)
    CLIENT_TIMEOUT = 3600  # 1 hour
    CLEANUP_INTERVAL = 1800  # 30 minutes
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_bigquery_client(self, project_id: Optional[str] = None):
        """Get BigQuery client with connection reuse"""
        client_key = f"bigquery_{project_id or 'default'}"
        return self._get_or_create_client(
            client_key,
            lambda: self._create_bigquery_client(project_id)
        )
    
    def get_storage_client(self, project_id: Optional[str] = None):
        """Get Cloud Storage client with connection reuse"""
        client_key = f"storage_{project_id or 'default'}"
        return self._get_or_create_client(
            client_key,
            lambda: self._create_storage_client(project_id)
        )
    
    def get_secret_manager_client(self, project_id: Optional[str] = None):
        """Get Secret Manager client with connection reuse"""
        client_key = f"secretmanager_{project_id or 'default'}"
        return self._get_or_create_client(
            client_key,
            lambda: self._create_secret_manager_client()
        )
    
    def _get_or_create_client(self, client_key: str, factory_func):
        """Generic client getter with caching and thread safety"""
        current_time = time.time()
        
        # Check if client exists and is not expired
        if (client_key in self._clients and 
            current_time - self._last_used.get(client_key, 0) < self.CLIENT_TIMEOUT):
            self._last_used[client_key] = current_time
            return self._clients[client_key]
        
        # Create client lock if not exists
        if client_key not in self._client_locks:
            self._client_locks[client_key] = Lock()
        
        # Thread-safe client creation
        with self._client_locks[client_key]:
            # Double-check in case another thread created it
            if (client_key in self._clients and 
                current_time - self._last_used.get(client_key, 0) < self.CLIENT_TIMEOUT):
                self._last_used[client_key] = current_time
                return self._clients[client_key]
            
            # Create new client
            try:
                client = factory_func()
                self._clients[client_key] = client
                self._last_used[client_key] = current_time
                
                logging.info(f"Created new client: {client_key}")
                return client
                
            except Exception as e:
                logging.error(f"Failed to create client {client_key}: {e}")
                raise
    
    def _create_bigquery_client(self, project_id: Optional[str] = None):
        """Create BigQuery client with optimized settings"""
        from google.cloud import bigquery
        
        # Optimize client settings for better performance
        client_options = {
            'project': project_id,
            'default_query_job_config': bigquery.QueryJobConfig(
                use_query_cache=True,
                use_legacy_sql=False,
                maximum_bytes_billed=10**12,  # 1TB limit
                job_timeout_ms=300000,  # 5 minutes
            ),
            'default_load_job_config': bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
        }
        
        return bigquery.Client(**{k: v for k, v in client_options.items() if v is not None})
    
    def _create_storage_client(self, project_id: Optional[str] = None):
        """Create Cloud Storage client"""
        from google.cloud import storage
        return storage.Client(project=project_id)
    
    def _create_secret_manager_client(self):
        """Create Secret Manager client"""
        from google.cloud import secretmanager
        return secretmanager.SecretManagerServiceClient()
    
    @contextmanager
    def batch_bigquery_client(self, project_id: Optional[str] = None):
        """Context manager for BigQuery operations that automatically handles batching"""
        client = self.get_bigquery_client(project_id)
        
        # Store original settings
        original_config = client._default_query_job_config
        
        # Set batch-optimized settings
        from google.cloud import bigquery
        batch_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            use_legacy_sql=False,
            priority=bigquery.QueryPriority.BATCH,  # Use batch priority for better resource allocation
            maximum_bytes_billed=10**13,  # Higher limit for batch jobs
        )
        client._default_query_job_config = batch_config
        
        try:
            yield client
        finally:
            # Restore original settings
            client._default_query_job_config = original_config
    
    def cleanup_expired_clients(self):
        """Remove expired clients to prevent memory leaks"""
        current_time = time.time()
        expired_keys = []
        
        for client_key, last_used in self._last_used.items():
            if current_time - last_used > self.CLIENT_TIMEOUT:
                expired_keys.append(client_key)
        
        for key in expired_keys:
            with self._client_locks.get(key, Lock()):
                if key in self._clients:
                    try:
                        # Properly close client if it has a close method
                        client = self._clients[key]
                        if hasattr(client, 'close'):
                            client.close()
                    except Exception as e:
                        logging.warning(f"Error closing client {key}: {e}")
                    
                    del self._clients[key]
                    del self._last_used[key]
                    logging.info(f"Cleaned up expired client: {key}")
    
    def get_client_stats(self) -> Dict[str, Any]:
        """Get current client statistics for monitoring"""
        current_time = time.time()
        return {
            'active_clients': len(self._clients),
            'client_types': {
                key.split('_')[0]: sum(1 for k in self._clients.keys() if k.startswith(key.split('_')[0]))
                for key in self._clients.keys()
            },
            'oldest_client_age': min([current_time - last_used for last_used in self._last_used.values()]) if self._last_used else 0,
            'newest_client_age': max([current_time - last_used for last_used in self._last_used.values()]) if self._last_used else 0
        }


# Global client manager instance
client_manager = ClientManager()


# Convenience functions for backward compatibility and ease of use
def get_bigquery_client(project_id: Optional[str] = None):
    """Get shared BigQuery client"""
    return client_manager.get_bigquery_client(project_id)


def get_storage_client(project_id: Optional[str] = None):
    """Get shared Cloud Storage client"""
    return client_manager.get_storage_client(project_id)


def get_secret_manager_client(project_id: Optional[str] = None):
    """Get shared Secret Manager client"""
    return client_manager.get_secret_manager_client(project_id)


@contextmanager
def batch_bigquery_operations(project_id: Optional[str] = None):
    """Context manager for batch BigQuery operations"""
    with client_manager.batch_bigquery_client(project_id) as client:
        yield client


class DataflowClientMixin:
    """
    Mixin for Dataflow jobs to use optimized clients.
    Prevents creating new clients in each DoFn worker.
    """
    
    def __init__(self):
        # Don't initialize clients in __init__ - let workers create them lazily
        self._project_id = None
        self._initialized = False
    
    def setup(self):
        """Setup method called once per worker - initialize clients here"""
        if not self._initialized:
            # Get project ID from runtime environment if not set
            if not self._project_id:
                import os
                self._project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
            
            self._initialized = True
            logging.info("Initialized client connections for Dataflow worker")
    
    def get_bq_client(self):
        """Get BigQuery client for this worker"""
        if not self._initialized:
            self.setup()
        return get_bigquery_client(self._project_id)
    
    def get_storage_client(self):
        """Get Storage client for this worker"""
        if not self._initialized:
            self.setup()
        return get_storage_client(self._project_id)


# Usage examples and best practices
"""
USAGE EXAMPLES:

# For Airflow DAGs:
def my_airflow_task(**context):
    bq_client = get_bigquery_client()  # Reuses existing client
    # Use client...

# For Dataflow DoFns:
class MyDoFn(beam.DoFn, DataflowClientMixin):
    def __init__(self, project_id):
        super().__init__()
        self._project_id = project_id
    
    def process(self, element):
        bq_client = self.get_bq_client()  # Worker-local client
        # Process element...

# For batch operations:
with batch_bigquery_operations() as bq_client:
    # Run multiple queries with batch settings
    result1 = bq_client.query("SELECT ...")
    result2 = bq_client.query("SELECT ...")

BEST PRACTICES:
1. Use the global functions (get_bigquery_client, etc.) in Airflow tasks
2. Use DataflowClientMixin in Dataflow DoFns
3. Use batch_bigquery_operations for large batch jobs
4. Let the ClientManager handle cleanup automatically
5. Monitor client usage with client_manager.get_client_stats()
"""
