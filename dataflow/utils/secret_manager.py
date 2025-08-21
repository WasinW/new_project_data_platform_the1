"""
Secret Manager utility for securely retrieving credentials and configuration.
"""

import logging
from typing import Dict, Any, Optional
from google.cloud import secretmanager
import json


class SecretManagerClient:
    """Client for accessing Google Cloud Secret Manager"""
    
    def __init__(self, project_id: Optional[str] = None):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id
        self._cache = {}
    
    def get_secret(self, secret_name: str, version: str = "latest") -> str:
        """
        Retrieve a secret from Secret Manager
        
        Args:
            secret_name: Name of the secret (can be full path or just name)
            version: Version of the secret to retrieve
            
        Returns:
            Secret value as string
        """
        # Handle both full paths and secret names
        if secret_name.startswith("projects/"):
            name = secret_name
        else:
            if not self.project_id:
                raise ValueError("project_id must be provided for secret name without full path")
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/{version}"
        
        # Check cache first
        if name in self._cache:
            return self._cache[name]
        
        try:
            response = self.client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8")
            
            # Cache the secret
            self._cache[name] = secret_value
            
            logging.info(f"Successfully retrieved secret: {secret_name}")
            return secret_value
            
        except Exception as e:
            logging.error(f"Failed to retrieve secret {secret_name}: {e}")
            raise
    
    def get_secret_json(self, secret_name: str, version: str = "latest") -> Dict[str, Any]:
        """
        Retrieve a secret that contains JSON data
        
        Args:
            secret_name: Name of the secret
            version: Version of the secret to retrieve
            
        Returns:
            Parsed JSON as dictionary
        """
        secret_value = self.get_secret(secret_name, version)
        try:
            return json.loads(secret_value)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse secret {secret_name} as JSON: {e}")
            raise
    
    def get_aws_credentials(self, config: Dict[str, str]) -> Dict[str, str]:
        """
        Retrieve AWS credentials from Secret Manager
        
        Args:
            config: Dictionary containing secret paths for AWS credentials
            
        Returns:
            Dictionary with AWS credentials
        """
        credentials = {}
        
        try:
            credentials['aws_access_key_id'] = self.get_secret(
                config['aws_access_key_id']
            )
            credentials['aws_secret_access_key'] = self.get_secret(
                config['aws_secret_access_key']
            )
            credentials['s3_bucket_name'] = self.get_secret(
                config['s3_bucket_name']
            )
            
            logging.info("Successfully retrieved AWS credentials from Secret Manager")
            return credentials
            
        except Exception as e:
            logging.error(f"Failed to retrieve AWS credentials: {e}")
            raise
    
    def get_bigquery_credentials(self, config: Dict[str, str]) -> Dict[str, Any]:
        """
        Retrieve BigQuery service account credentials from Secret Manager
        
        Args:
            config: Dictionary containing secret paths for BigQuery credentials
            
        Returns:
            Dictionary with BigQuery service account credentials
        """
        try:
            service_account_json = self.get_secret_json(
                config['service_account_key']
            )
            
            logging.info("Successfully retrieved BigQuery credentials from Secret Manager")
            return service_account_json
            
        except Exception as e:
            logging.error(f"Failed to retrieve BigQuery credentials: {e}")
            raise
    
    def clear_cache(self):
        """Clear the secret cache"""
        self._cache.clear()
        logging.info("Secret cache cleared")


def get_secrets_from_config(config: Dict[str, Any], project_id: str) -> Dict[str, Any]:
    """
    Retrieve all secrets defined in configuration
    
    Args:
        config: Pipeline configuration containing secret paths
        project_id: GCP project ID
        
    Returns:
        Dictionary containing all retrieved secrets
    """
    secret_client = SecretManagerClient(project_id)
    secrets = {}
    
    if 'secrets' in config:
        secrets_config = config['secrets']
        
        # Get AWS/S3 credentials
        if 's3_credentials' in secrets_config:
            secrets['s3_credentials'] = secret_client.get_aws_credentials(
                secrets_config['s3_credentials']
            )
        
        # Get BigQuery credentials
        if 'bigquery_credentials' in secrets_config:
            secrets['bigquery_credentials'] = secret_client.get_bigquery_credentials(
                secrets_config['bigquery_credentials']
            )
    
    return secrets


def setup_aws_credentials_for_sts(secrets: Dict[str, Any]) -> Dict[str, str]:
    """
    Setup AWS credentials for Storage Transfer Service
    
    Args:
        secrets: Retrieved secrets dictionary
        
    Returns:
        Dictionary formatted for STS configuration
    """
    if 's3_credentials' not in secrets:
        raise ValueError("S3 credentials not found in secrets")
    
    s3_creds = secrets['s3_credentials']
    
    return {
        'accessKeyId': s3_creds['aws_access_key_id'],
        'secretAccessKey': s3_creds['aws_secret_access_key']
    }


def setup_bigquery_credentials(secrets: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Setup BigQuery credentials for external table access
    
    Args:
        secrets: Retrieved secrets dictionary
        
    Returns:
        Service account credentials or None if using default
    """
    if 'bigquery_credentials' in secrets:
        return secrets['bigquery_credentials']
    
    # Use default service account if no specific credentials provided
    logging.info("Using default service account for BigQuery access")
    return None
