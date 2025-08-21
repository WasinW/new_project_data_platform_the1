# dataflow/transforms/dependency_checker.py
import apache_beam as beam
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging

class DependencyChecker(beam.DoFn):
    """
    Check upstream dependencies before processing records.
    Supports multiple dependency types and caching for performance.
    """
    
    def __init__(self, dependencies: List[Dict], cache_duration_minutes: int = 5):
        """
        Initialize dependency checker.
        
        Args:
            dependencies: List of dependency configurations
            cache_duration_minutes: How long to cache dependency check results
        """
        self.dependencies = dependencies
        self.cache_duration = timedelta(minutes=cache_duration_minutes)
        self.dependency_cache = {}
        logging.info(f"DependencyChecker initialized with {len(dependencies)} dependencies")
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        """
        Process element after checking all dependencies.
        """
        element_id = element.get('id', element.get('member_id', 'unknown'))
        failed_dependencies = []
        
        # Check all dependencies
        for dep in self.dependencies:
            dep_name = dep.get('name', f"dep_{dep.get('table', 'unknown')}")
            
            if not self._check_dependency(dep, element):
                failed_dependencies.append({
                    'name': dep_name,
                    'table': f"{dep['project']}.{dep['dataset']}.{dep['table']}",
                    'condition': dep['condition']
                })
        
        if failed_dependencies:
            # Dependencies failed
            yield beam.pvalue.TaggedOutput('failed_dependency', {
                'element_id': element_id,
                'element': element,
                'failed_dependencies': failed_dependencies,
                'timestamp': datetime.utcnow().isoformat(),
                'check_type': 'dependency_validation'
            })
        else:
            # All dependencies passed
            element['_dependency_check_passed'] = True
            element['_dependency_check_time'] = datetime.utcnow().isoformat()
            yield beam.pvalue.TaggedOutput('main', element)
    
    def _check_dependency(self, dep: Dict, element: Dict) -> bool:
        """
        Check individual dependency with caching.
        """
        dep_key = self._create_dependency_key(dep, element)
        current_time = datetime.utcnow()
        
        # Check cache first
        if dep_key in self.dependency_cache:
            cached_result, cached_time = self.dependency_cache[dep_key]
            if current_time - cached_time < self.cache_duration:
                return cached_result
        
        # Perform actual check
        result = self._perform_dependency_check(dep, element)
        
        # Cache result
        self.dependency_cache[dep_key] = (result, current_time)
        
        # Clean old cache entries
        self._clean_cache(current_time)
        
        return result
    
    def _create_dependency_key(self, dep: Dict, element: Dict) -> str:
        """Create cache key for dependency"""
        base_key = f"{dep['project']}.{dep['dataset']}.{dep['table']}"
        
        # Include element-specific parameters if condition uses element data
        condition = dep['condition']
        if 'element.' in condition or '@' in condition:
            element_id = element.get('id', element.get('member_id', ''))
            base_key += f"#{element_id}"
        
        return base_key
    
    def _perform_dependency_check(self, dep: Dict, element: Dict) -> bool:
        """
        Perform actual BigQuery dependency check.
        """
        try:
            query = self._build_dependency_query(dep, element)
            
            # Execute query
            result = self.bq_client.query(query).result()
            
            for row in result:
                count = getattr(row, 'count', 0)
                return count > 0
            
            return False
            
        except Exception as e:
            logging.error(f"Dependency check failed for {dep.get('name', 'unknown')}: {e}")
            
            # Check if we should fail open or closed
            fail_open = dep.get('fail_open', False)
            return fail_open
    
    def _build_dependency_query(self, dep: Dict, element: Dict) -> str:
        """
        Build BigQuery query for dependency check.
        """
        base_query = f"""
            SELECT COUNT(*) as count
            FROM `{dep['project']}.{dep['dataset']}.{dep['table']}`
            WHERE {dep['condition']}
        """
        
        # Replace element placeholders
        query = base_query
        for key, value in element.items():
            placeholder = f"element.{key}"
            if placeholder in query:
                if isinstance(value, str):
                    query = query.replace(placeholder, f"'{value}'")
                else:
                    query = query.replace(placeholder, str(value))
        
        return query
    
    def _clean_cache(self, current_time: datetime):
        """Clean expired cache entries"""
        expired_keys = []
        
        for key, (_, cached_time) in self.dependency_cache.items():
            if current_time - cached_time > self.cache_duration:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.dependency_cache[key]

class AdvancedDependencyChecker(beam.DoFn):
    """
    Advanced dependency checker with support for complex conditions
    and multiple dependency types (BigQuery, Pub/Sub, Cloud Storage, etc.)
    """
    
    def __init__(self, dependency_config: Dict):
        self.dependency_config = dependency_config
        self.bigquery_deps = dependency_config.get('bigquery', [])
        self.pubsub_deps = dependency_config.get('pubsub', [])
        self.storage_deps = dependency_config.get('storage', [])
        self.api_deps = dependency_config.get('api', [])
    
    def setup(self):
        from google.cloud import bigquery, storage, pubsub_v1
        import requests
        
        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        self.pubsub_client = pubsub_v1.PublisherClient()
        self.requests = requests
    
    def process(self, element):
        """Process with advanced dependency checking"""
        failed_checks = []
        
        # Check BigQuery dependencies
        for dep in self.bigquery_deps:
            if not self._check_bigquery_dependency(dep, element):
                failed_checks.append(('bigquery', dep))
        
        # Check Pub/Sub dependencies
        for dep in self.pubsub_deps:
            if not self._check_pubsub_dependency(dep, element):
                failed_checks.append(('pubsub', dep))
        
        # Check Storage dependencies
        for dep in self.storage_deps:
            if not self._check_storage_dependency(dep, element):
                failed_checks.append(('storage', dep))
        
        # Check API dependencies
        for dep in self.api_deps:
            if not self._check_api_dependency(dep, element):
                failed_checks.append(('api', dep))
        
        if failed_checks:
            yield beam.pvalue.TaggedOutput('failed_dependency', {
                'element': element,
                'failed_checks': failed_checks,
                'timestamp': datetime.utcnow().isoformat()
            })
        else:
            yield beam.pvalue.TaggedOutput('main', element)
    
    def _check_bigquery_dependency(self, dep: Dict, element: Dict) -> bool:
        """Check BigQuery dependency"""
        try:
            query = dep['query']
            # Replace element placeholders
            for key, value in element.items():
                placeholder = f"${key}"
                if placeholder in query:
                    query = query.replace(placeholder, str(value))
            
            result = self.bq_client.query(query).result()
            for row in result:
                return getattr(row, 'count', 0) > 0
            return False
        except Exception as e:
            logging.error(f"BigQuery dependency check failed: {e}")
            return dep.get('fail_open', False)
    
    def _check_pubsub_dependency(self, dep: Dict, element: Dict) -> bool:
        """Check Pub/Sub topic exists and is accessible"""
        try:
            topic_path = self.pubsub_client.topic_path(dep['project'], dep['topic'])
            self.pubsub_client.get_topic(request={"topic": topic_path})
            return True
        except Exception as e:
            logging.error(f"Pub/Sub dependency check failed: {e}")
            return dep.get('fail_open', False)
    
    def _check_storage_dependency(self, dep: Dict, element: Dict) -> bool:
        """Check Cloud Storage object exists"""
        try:
            bucket = self.storage_client.bucket(dep['bucket'])
            blob_name = dep['object_name']
            
            # Replace element placeholders
            for key, value in element.items():
                placeholder = f"${key}"
                if placeholder in blob_name:
                    blob_name = blob_name.replace(placeholder, str(value))
            
            blob = bucket.blob(blob_name)
            return blob.exists()
        except Exception as e:
            logging.error(f"Storage dependency check failed: {e}")
            return dep.get('fail_open', False)
    
    def _check_api_dependency(self, dep: Dict, element: Dict) -> bool:
        """Check external API dependency"""
        try:
            url = dep['url']
            method = dep.get('method', 'GET')
            timeout = dep.get('timeout', 30)
            
            # Replace element placeholders in URL
            for key, value in element.items():
                placeholder = f"${key}"
                if placeholder in url:
                    url = url.replace(placeholder, str(value))
            
            response = self.requests.request(method, url, timeout=timeout)
            return response.status_code == 200
        except Exception as e:
            logging.error(f"API dependency check failed: {e}")
            return dep.get('fail_open', False)
