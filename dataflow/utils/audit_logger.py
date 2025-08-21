# dataflow/utils/audit_logger.py
import apache_beam as beam
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import json

class AuditLogger(beam.DoFn):
    """
    Comprehensive audit logging for pipeline processing.
    Tracks data lineage, processing metrics, and data quality.
    """
    
    def __init__(self, pipeline_config: Dict):
        self.pipeline_config = pipeline_config
        self.audit_table = f"{pipeline_config['project']}.{pipeline_config['domain']}_audit.processing_logs"
        self.lineage_table = f"{pipeline_config['project']}.{pipeline_config['domain']}_audit.data_lineage"
        self.metrics_table = f"{pipeline_config['project']}.{pipeline_config['domain']}_audit.processing_metrics"
        
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
        self.batch_size = 100
        self.audit_batch = []
        self.lineage_batch = []
        self.metrics_batch = []
    
    def process(self, element):
        """Process element and log audit information"""
        try:
            # Create audit record
            audit_record = self._create_audit_record(element)
            self.audit_batch.append(audit_record)
            
            # Create lineage record
            lineage_record = self._create_lineage_record(element)
            self.lineage_batch.append(lineage_record)
            
            # Create metrics record
            metrics_record = self._create_metrics_record(element)
            self.metrics_batch.append(metrics_record)
            
            # Flush batches if they reach batch size
            if len(self.audit_batch) >= self.batch_size:
                self._flush_batches()
            
            # Pass through the element
            yield element
            
        except Exception as e:
            logging.error(f"Audit logging failed: {e}")
            # Don't fail the pipeline for audit issues
            yield element
    
    def finish_bundle(self):
        """Flush remaining batches at the end of bundle"""
        if self.audit_batch or self.lineage_batch or self.metrics_batch:
            self._flush_batches()
    
    def _create_audit_record(self, element: Dict) -> Dict:
        """Create comprehensive audit record"""
        return {
            'pipeline_name': self.pipeline_config['name'],
            'pipeline_mode': self.pipeline_config['mode'],
            'domain': self.pipeline_config['domain'],
            'record_id': self._extract_record_id(element),
            'processing_timestamp': datetime.utcnow().isoformat(),
            'source_timestamp': element.get('_timestamp'),
            'ingestion_timestamp': element.get('_ingestion_timestamp'),
            'target_table': element.get('_target_table'),
            'transform_module': element.get('_transform_module'),
            'status': 'SUCCESS',
            'data_size_bytes': self._calculate_data_size(element),
            'field_count': len(element),
            'validation_status': element.get('_validation_results', {}).get('quality_score'),
            'processing_duration_ms': self._calculate_processing_duration(element),
            'pipeline_version': self.pipeline_config.get('version', '1.0.0'),
            'environment': self.pipeline_config.get('environment', 'unknown')
        }
    
    def _create_lineage_record(self, element: Dict) -> Dict:
        """Create data lineage record"""
        return {
            'record_id': self._extract_record_id(element),
            'source_system': element.get('_source', 'unknown'),
            'source_table': f"{self.pipeline_config.get('source_project', '')}.{self.pipeline_config.get('source_dataset', '')}.{self.pipeline_config.get('source_table', '')}",
            'target_table': element.get('_target_table'),
            'transformation_applied': element.get('_transform_module'),
            'lineage_timestamp': datetime.utcnow().isoformat(),
            'pipeline_run_id': self.pipeline_config.get('run_id'),
            'data_classification': self._classify_data(element),
            'processing_stage': element.get('_processing_stage', 'unknown')
        }
    
    def _create_metrics_record(self, element: Dict) -> Dict:
        """Create processing metrics record"""
        return {
            'pipeline_name': self.pipeline_config['name'],
            'domain': self.pipeline_config['domain'],
            'metric_timestamp': datetime.utcnow().isoformat(),
            'metric_type': 'record_processed',
            'metric_value': 1,
            'target_table': element.get('_target_table'),
            'processing_stage': element.get('_processing_stage', 'unknown'),
            'data_quality_score': element.get('_validation_results', {}).get('quality_score'),
            'record_size_bytes': self._calculate_data_size(element),
            'pipeline_mode': self.pipeline_config['mode']
        }
    
    def _extract_record_id(self, element: Dict) -> str:
        """Extract unique record identifier"""
        for key in ['id', 'record_id', 'member_id', 'customer_id', 'order_id']:
            if key in element and element[key]:
                return str(element[key])
        
        # Generate hash-based ID if no clear identifier
        import hashlib
        record_str = json.dumps(element, sort_keys=True, default=str)
        return hashlib.md5(record_str.encode()).hexdigest()[:16]
    
    def _calculate_data_size(self, element: Dict) -> int:
        """Calculate approximate size of element in bytes"""
        try:
            return len(json.dumps(element, default=str).encode('utf-8'))
        except Exception:
            return 0
    
    def _calculate_processing_duration(self, element: Dict) -> Optional[float]:
        """Calculate processing duration if timestamps are available"""
        try:
            start_time = element.get('_processing_start_time')
            if start_time:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                duration = (datetime.utcnow() - start_dt).total_seconds() * 1000
                return duration
        except Exception:
            pass
        return None
    
    def _classify_data(self, element: Dict) -> str:
        """Classify data based on content"""
        # Simple data classification logic
        sensitive_fields = ['email', 'phone', 'address', 'ssn', 'credit_card']
        
        for field in sensitive_fields:
            if field in element and element[field]:
                return 'PII'
        
        if any(key.startswith('financial_') for key in element.keys()):
            return 'FINANCIAL'
        
        return 'GENERAL'
    
    def _flush_batches(self):
        """Flush all batches to BigQuery"""
        try:
            if self.audit_batch:
                self._write_batch(self.audit_table, self.audit_batch)
                self.audit_batch.clear()
            
            if self.lineage_batch:
                self._write_batch(self.lineage_table, self.lineage_batch)
                self.lineage_batch.clear()
            
            if self.metrics_batch:
                self._write_batch(self.metrics_table, self.metrics_batch)
                self.metrics_batch.clear()
                
        except Exception as e:
            logging.error(f"Failed to flush audit batches: {e}")
    
    def _write_batch(self, table_id: str, batch: List[Dict]):
        """Write batch to BigQuery table"""
        try:
            errors = self.bq_client.insert_rows_json(table_id, batch)
            if errors:
                logging.error(f"BigQuery insert errors for {table_id}: {errors}")
        except Exception as e:
            logging.error(f"Failed to write to {table_id}: {e}")

class MetricsCollector(beam.DoFn):
    """
    Specialized transform for collecting pipeline metrics.
    """
    
    def __init__(self, metrics_config: Dict):
        self.metrics_config = metrics_config
        self.metrics_table = metrics_config['table']
        self.collection_interval = metrics_config.get('interval_seconds', 60)
        
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
        self.metrics_buffer = {}
        self.last_flush_time = datetime.utcnow()
    
    def process(self, element):
        """Collect metrics from processed elements"""
        try:
            # Extract metrics
            table = element.get('_target_table', 'unknown')
            processing_time = datetime.utcnow()
            
            # Update metrics buffer
            if table not in self.metrics_buffer:
                self.metrics_buffer[table] = {
                    'record_count': 0,
                    'total_size_bytes': 0,
                    'error_count': 0,
                    'first_record_time': processing_time,
                    'last_record_time': processing_time
                }
            
            metrics = self.metrics_buffer[table]
            metrics['record_count'] += 1
            metrics['total_size_bytes'] += self._calculate_size(element)
            metrics['last_record_time'] = processing_time
            
            if element.get('_status') == 'ERROR':
                metrics['error_count'] += 1
            
            # Check if it's time to flush metrics
            if (processing_time - self.last_flush_time).seconds >= self.collection_interval:
                self._flush_metrics()
            
            yield element
            
        except Exception as e:
            logging.error(f"Metrics collection failed: {e}")
            yield element
    
    def finish_bundle(self):
        """Flush metrics at end of bundle"""
        self._flush_metrics()
    
    def _calculate_size(self, element: Dict) -> int:
        """Calculate element size"""
        try:
            return len(json.dumps(element, default=str).encode('utf-8'))
        except Exception:
            return 0
    
    def _flush_metrics(self):
        """Flush collected metrics to BigQuery"""
        try:
            current_time = datetime.utcnow()
            metrics_records = []
            
            for table, metrics in self.metrics_buffer.items():
                if metrics['record_count'] > 0:
                    # Calculate throughput
                    duration = (metrics['last_record_time'] - metrics['first_record_time']).total_seconds()
                    throughput = metrics['record_count'] / max(duration, 1)
                    
                    record = {
                        'timestamp': current_time.isoformat(),
                        'table_name': table,
                        'record_count': metrics['record_count'],
                        'total_size_bytes': metrics['total_size_bytes'],
                        'error_count': metrics['error_count'],
                        'throughput_records_per_second': throughput,
                        'avg_record_size_bytes': metrics['total_size_bytes'] / metrics['record_count'],
                        'error_rate': metrics['error_count'] / metrics['record_count'],
                        'collection_interval_seconds': self.collection_interval
                    }
                    metrics_records.append(record)
            
            if metrics_records:
                errors = self.bq_client.insert_rows_json(self.metrics_table, metrics_records)
                if errors:
                    logging.error(f"Metrics insert errors: {errors}")
                else:
                    logging.info(f"Flushed {len(metrics_records)} metrics records")
            
            # Reset metrics buffer
            self.metrics_buffer.clear()
            self.last_flush_time = current_time
            
        except Exception as e:
            logging.error(f"Failed to flush metrics: {e}")

class DataQualityLogger(beam.DoFn):
    """
    Specialized logger for data quality metrics and issues.
    """
    
    def __init__(self, quality_config: Dict):
        self.quality_config = quality_config
        self.quality_table = quality_config['table']
        
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        """Log data quality information"""
        try:
            validation_results = element.get('_validation_results', {})
            
            if validation_results:
                quality_record = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'record_id': element.get('id', element.get('member_id', 'unknown')),
                    'table_name': element.get('_target_table'),
                    'quality_score': validation_results.get('quality_score', 0),
                    'passed_rules': json.dumps(validation_results.get('passed_rules', [])),
                    'failed_rules': json.dumps(validation_results.get('failed_rules', [])),
                    'quality_issues': json.dumps(validation_results.get('quality_issues', [])),
                    'rule_count': len(validation_results.get('passed_rules', [])) + len(validation_results.get('failed_rules', [])),
                    'pass_rate': len(validation_results.get('passed_rules', [])) / max(1, len(validation_results.get('passed_rules', [])) + len(validation_results.get('failed_rules', [])))
                }
                
                # Write immediately for quality issues
                errors = self.bq_client.insert_rows_json(self.quality_table, [quality_record])
                if errors:
                    logging.error(f"Quality logging errors: {errors}")
            
            yield element
            
        except Exception as e:
            logging.error(f"Data quality logging failed: {e}")
            yield element
