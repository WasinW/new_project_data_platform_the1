# dataflow/utils/config_loader.py
import yaml
import json
import logging
from typing import Dict, Any, Optional
from google.cloud import storage
import os

class ConfigLoader:
    """
    Utility class for loading configuration from various sources.
    Supports local files, GCS, and environment variables.
    """
    
    def __init__(self, default_config_path: Optional[str] = None):
        self.default_config_path = default_config_path
        self.config_cache = {}
        
    def load_config(self, config_path: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Load configuration from specified path.
        
        Args:
            config_path: Path to configuration file (local or GCS)
            use_cache: Whether to use cached configuration
            
        Returns:
            Configuration dictionary
        """
        if use_cache and config_path in self.config_cache:
            logging.info(f"Using cached config for {config_path}")
            return self.config_cache[config_path]
        
        try:
            if config_path.startswith('gs://'):
                config = self._load_from_gcs(config_path)
            else:
                config = self._load_from_local(config_path)
            
            # Process config (handle includes, environment variables, etc.)
            processed_config = self._process_config(config)
            
            # Cache the config
            if use_cache:
                self.config_cache[config_path] = processed_config
            
            logging.info(f"Successfully loaded config from {config_path}")
            return processed_config
            
        except Exception as e:
            logging.error(f"Failed to load config from {config_path}: {e}")
            if self.default_config_path:
                logging.info(f"Falling back to default config: {self.default_config_path}")
                return self.load_config(self.default_config_path, use_cache=False)
            raise
    
    def _load_from_gcs(self, gcs_path: str) -> Dict[str, Any]:
        """Load configuration from Google Cloud Storage"""
        # Parse GCS path
        path_parts = gcs_path[5:].split('/', 1)  # Remove 'gs://'
        bucket_name = path_parts[0]
        blob_name = path_parts[1]
        
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Download content
        content = blob.download_as_text()
        
        # Parse based on file extension
        if blob_name.endswith('.yaml') or blob_name.endswith('.yml'):
            return yaml.safe_load(content)
        elif blob_name.endswith('.json'):
            return json.loads(content)
        else:
            raise ValueError(f"Unsupported config file format: {blob_name}")
    
    def _load_from_local(self, local_path: str) -> Dict[str, Any]:
        """Load configuration from local file"""
        with open(local_path, 'r', encoding='utf-8') as f:
            if local_path.endswith('.yaml') or local_path.endswith('.yml'):
                return yaml.safe_load(f)
            elif local_path.endswith('.json'):
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {local_path}")
    
    def _process_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process configuration to handle includes, environment variables, etc.
        """
        processed = self._substitute_env_vars(config)
        processed = self._handle_includes(processed)
        processed = self._validate_config(processed)
        return processed
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.
        Variables are specified as ${VAR_NAME} or ${VAR_NAME:default_value}
        """
        if isinstance(config, dict):
            return {key: self._substitute_env_vars(value) for key, value in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            import re
            
            # Pattern to match ${VAR_NAME} or ${VAR_NAME:default}
            pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
            
            def replace_var(match):
                var_name = match.group(1)
                default_value = match.group(2) if match.group(2) is not None else ''
                return os.environ.get(var_name, default_value)
            
            return re.sub(pattern, replace_var, config)
        else:
            return config
    
    def _handle_includes(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle include directives in configuration.
        Supports including other config files.
        """
        if 'includes' in config:
            for include_path in config['includes']:
                try:
                    included_config = self.load_config(include_path, use_cache=False)
                    # Merge included config (included takes precedence)
                    config = self._merge_configs(included_config, config)
                except Exception as e:
                    logging.warning(f"Failed to include config {include_path}: {e}")
            
            # Remove includes directive
            del config['includes']
        
        return config
    
    def _merge_configs(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two configuration dictionaries"""
        result = base.copy()
        
        for key, value in overlay.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate configuration structure and required fields.
        """
        required_fields = ['project', 'domain']
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Required configuration field missing: {field}")
        
        # Validate specific sections
        if 'source' in config:
            source_required = ['project', 'dataset', 'table']
            for field in source_required:
                if field not in config['source']:
                    raise ValueError(f"Required source field missing: {field}")
        
        if 'pubsub' in config and config.get('mode') == 'realtime':
            if 'subscription' not in config['pubsub']:
                raise ValueError("Pub/Sub subscription required for realtime mode")
        
        return config
    
    def get_domain_config(self, domain: str, base_config_path: str) -> Dict[str, Any]:
        """
        Get domain-specific configuration by merging base config with domain overrides.
        """
        # Load base config
        base_config = self.load_config(base_config_path)
        
        # Try to load domain-specific overrides
        domain_config_path = base_config_path.replace('config.yaml', f'{domain}_config.yaml')
        
        try:
            domain_overrides = self.load_config(domain_config_path)
            return self._merge_configs(base_config, domain_overrides)
        except Exception:
            logging.info(f"No domain-specific config found for {domain}, using base config")
            return base_config

class EnvironmentConfigLoader(ConfigLoader):
    """
    Enhanced config loader that adapts configuration based on environment.
    Supports dev, staging, prod environments with different configurations.
    """
    
    def __init__(self, environment: Optional[str] = None):
        super().__init__()
        self.environment = environment or os.environ.get('ENVIRONMENT', 'dev')
        
    def load_config(self, config_path: str, use_cache: bool = True) -> Dict[str, Any]:
        """Load environment-specific configuration"""
        # Load base config
        base_config = super().load_config(config_path, use_cache)
        
        # Apply environment-specific overrides
        env_config = self._get_environment_config(base_config)
        
        return env_config
    
    def _get_environment_config(self, base_config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment-specific configuration overrides"""
        if 'environments' not in base_config:
            return base_config
        
        env_overrides = base_config['environments'].get(self.environment, {})
        
        if env_overrides:
            logging.info(f"Applying {self.environment} environment overrides")
            result = self._merge_configs(base_config, env_overrides)
        else:
            result = base_config
        
        # Remove environments section from final config
        if 'environments' in result:
            del result['environments']
        
        return result

# Singleton instance for global use
config_loader = ConfigLoader()
env_config_loader = EnvironmentConfigLoader()
