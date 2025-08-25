# dataflow/pipelines/reconciliation_pipeline.py
"""
Reconciliation Dataflow Pipeline - Following Context Detail Requirements
✅ Loads config from GCS with ConfigLoader
✅ Retrieves secrets from Secret Manager
✅ Compares S3 data (via GCS) with BigQuery data
✅ Uses proper error handling and audit logging
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# Import custom modules (these would be in the actual project)
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_loader import ConfigLoader
from utils.secret_manager import SecretManager
from utils.audit_logger import AuditLogger
from utils.client_manager import ClientManager


class ReconciliationOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--mode', dest='mode', default='reconciliation')
        parser.add_argument('--domain', dest='domain', required=True)
        parser.add_argument('--config_path', dest='config_path', required=True)
        parser.add_argument('--reconciliation_date', dest='reconciliation_date', required=True)
        parser.add_argument('--s3_data_path', dest='s3_data_path', required=True)
        parser.add_argument('--bigquery_dataset', dest='bigquery_dataset', required=True)


class LoadS3Data(beam.DoFn):
    """Load S3 data that was transferred to GCS via STS"""
    
    def __init__(self, s3_data_path: str, config: Dict[str, Any]):
        self.s3_data_path = s3_data_path
        self.config = config
        
    def setup(self):
        self.audit_logger = AuditLogger(self.config)
        
    def process(self, element):
        """Load S3 data from GCS path"""
        try:
            # Element contains the file pattern
            file_pattern = f"{self.s3_data_path}/*.json"
            
            # Read files using beam.io
            with beam.io.gcp.gcsio.GcsIO() as gcs:
                files = gcs.list_prefix(self.s3_data_path.replace('gs://', ''))
                
                for file_path in files:
                    if file_path.endswith('.json'):
                        full_path = f"gs://{file_path}"
                        file_content = gcs.open(full_path, 'r').read()
                        
                        for line in file_content.split('\n'):
                            if line.strip():
                                try:
                                    record = json.loads(line)
                                    record['_source'] = 's3'
                                    record['_file_path'] = full_path
                                    record['_load_timestamp'] = datetime.utcnow().isoformat()
                                    yield record
                                except json.JSONDecodeError as e:
                                    self.audit_logger.log_error(
                                        'reconciliation',
                                        f"JSON decode error in {full_path}: {str(e)}",
                                        {'file_path': full_path, 'line': line[:100]}
                                    )
                                    
        except Exception as e:
            self.audit_logger.log_error(
                'reconciliation',
                f"Error loading S3 data: {str(e)}",
                {'s3_data_path': self.s3_data_path}
            )


class LoadBigQueryData(beam.DoFn):
    """Load BigQuery data for comparison"""
    
    def __init__(self, bigquery_dataset: str, reconciliation_date: str, config: Dict[str, Any]):
        self.bigquery_dataset = bigquery_dataset
        self.reconciliation_date = reconciliation_date
        self.config = config
        
    def setup(self):
        self.audit_logger = AuditLogger(self.config)
        self.client_manager = ClientManager(self.config)
        
    def process(self, element):
        """Load BigQuery data for the reconciliation date"""
        try:
            # Get the table list from element
            table_name = element
            
            query = f"""
                SELECT 
                    *,
                    '_source' as 'bigquery',
                    '{table_name}' as '_table_name',
                    CURRENT_TIMESTAMP() as '_load_timestamp'
                FROM `{self.bigquery_dataset}.{table_name}`
                WHERE DATE(_processing_timestamp) = '{self.reconciliation_date}'
            """
            
            # Execute query using client manager
            bigquery_client = self.client_manager.get_bigquery_client()
            query_job = bigquery_client.query(query)
            
            for row in query_job:
                record = dict(row)
                record['_source'] = 'bigquery'
                record['_table_name'] = table_name
                record['_load_timestamp'] = datetime.utcnow().isoformat()
                yield record
                
        except Exception as e:
            self.audit_logger.log_error(
                'reconciliation',
                f"Error loading BigQuery data: {str(e)}",
                {'table_name': table_name, 'dataset': self.bigquery_dataset}
            )


class ReconcileData(beam.DoFn):
    """Compare S3 and BigQuery records"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def setup(self):
        self.audit_logger = AuditLogger(self.config)
        
    def process(self, element):
        """Reconcile S3 and BigQuery records"""
        try:
            record_id, data = element
            s3_records = data.get('s3', [])
            bq_records = data.get('bigquery', [])
            
            # Handle missing records
            if not s3_records and bq_records:
                for bq_record in bq_records:
                    yield {
                        'record_id': record_id,
                        'status': 'MISSING_S3',
                        'table_name': bq_record.get('_table_name', 'unknown'),
                        'bigquery_record': bq_record,
                        's3_record': None,
                        'reconciliation_timestamp': datetime.utcnow().isoformat(),
                        'error_type': 'missing_source_data'
                    }
                    
            elif s3_records and not bq_records:
                for s3_record in s3_records:
                    yield {
                        'record_id': record_id,
                        'status': 'MISSING_BQ',
                        'table_name': s3_record.get('target_table', 'unknown'),
                        'bigquery_record': None,
                        's3_record': s3_record,
                        'reconciliation_timestamp': datetime.utcnow().isoformat(),
                        'error_type': 'missing_processed_data'
                    }
                    
            elif s3_records and bq_records:
                # Compare records field by field
                for s3_record in s3_records:
                    for bq_record in bq_records:
                        if self._records_match_key(s3_record, bq_record):
                            comparison_result = self._compare_records(s3_record, bq_record)
                            yield {
                                'record_id': record_id,
                                'status': 'MATCH' if comparison_result['is_match'] else 'MISMATCH',
                                'table_name': bq_record.get('_table_name', 'unknown'),
                                'bigquery_record': bq_record,
                                's3_record': s3_record,
                                'reconciliation_timestamp': datetime.utcnow().isoformat(),
                                'field_differences': comparison_result.get('differences', []),
                                'error_type': None if comparison_result['is_match'] else 'data_mismatch'
                            }
                            
        except Exception as e:
            self.audit_logger.log_error(
                'reconciliation',
                f"Error reconciling data: {str(e)}",
                {'record_id': record_id}
            )
            
    def _records_match_key(self, s3_record: Dict, bq_record: Dict) -> bool:
        """Check if records match on key fields"""
        # Define key fields for matching (adjust based on your data structure)
        key_fields = ['id', 'element_id', 'primary_key']
        
        for field in key_fields:
            if field in s3_record and field in bq_record:
                return s3_record[field] == bq_record[field]
        return False
        
    def _compare_records(self, s3_record: Dict, bq_record: Dict) -> Dict[str, Any]:
        """Compare two records field by field"""
        differences = []
        
        # Get all fields from both records
        all_fields = set(s3_record.keys()) | set(bq_record.keys())
        
        # Exclude metadata fields from comparison
        exclude_fields = {'_source', '_load_timestamp', '_file_path', '_table_name', 
                         '_processing_timestamp', '_ingestion_timestamp', '_source_timestamp'}
        
        for field in all_fields:
            if field in exclude_fields:
                continue
                
            s3_value = s3_record.get(field)
            bq_value = bq_record.get(field)
            
            if s3_value != bq_value:
                differences.append({
                    'field': field,
                    's3_value': s3_value,
                    'bq_value': bq_value
                })
        
        return {
            'is_match': len(differences) == 0,
            'differences': differences
        }


class WriteReconciliationResults(beam.DoFn):
    """Write reconciliation results to BigQuery"""
    
    def __init__(self, output_table: str, config: Dict[str, Any]):
        self.output_table = output_table
        self.config = config
        
    def setup(self):
        self.audit_logger = AuditLogger(self.config)
        
    def process(self, element):
        """Prepare record for BigQuery output"""
        try:
            # Flatten the record for BigQuery
            output_record = {
                'record_id': element['record_id'],
                'status': element['status'],
                'table_name': element['table_name'],
                'reconciliation_timestamp': element['reconciliation_timestamp'],
                'reconciliation_date': self.config.get('reconciliation_date'),
                'error_type': element.get('error_type'),
                'field_differences_json': json.dumps(element.get('field_differences', [])),
                's3_record_json': json.dumps(element.get('s3_record', {})),
                'bq_record_json': json.dumps(element.get('bigquery_record', {})),
                'domain': self.config.get('domain'),
                'pipeline_run_id': self.config.get('pipeline_run_id', datetime.utcnow().isoformat())
            }
            
            yield output_record
            
        except Exception as e:
            self.audit_logger.log_error(
                'reconciliation',
                f"Error writing reconciliation result: {str(e)}",
                {'element': str(element)[:500]}
            )


def run_reconciliation_pipeline(argv=None):
    """Main reconciliation pipeline function"""
    
    # Parse pipeline options
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    reconciliation_options = pipeline_options.view_as(ReconciliationOptions)
    
    # Load configuration and secrets
    config_loader = ConfigLoader()
    config = config_loader.load_config(reconciliation_options.config_path)
    
    secret_manager = SecretManager(config.get('project_id'))
    config.update(secret_manager.get_pipeline_secrets(reconciliation_options.domain))
    
    # Add runtime parameters to config
    config.update({
        'domain': reconciliation_options.domain,
        'reconciliation_date': reconciliation_options.reconciliation_date,
        's3_data_path': reconciliation_options.s3_data_path,
        'bigquery_dataset': reconciliation_options.bigquery_dataset,
        'pipeline_run_id': datetime.utcnow().isoformat()
    })
    
    # Define output table
    output_table = f"{reconciliation_options.bigquery_dataset.replace('.', ':')}.reconciliation_results"
    
    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Get table list for domain
        tables_to_reconcile = config.get('reconciliation_tables', [
            f'{reconciliation_options.domain}_raw_a1',
            f'{reconciliation_options.domain}_raw_a2',
            f'{reconciliation_options.domain}_refined_b1'
        ])
        
        # Load S3 data (transferred via STS)
        s3_data = (
            pipeline
            | 'Create S3 Path' >> beam.Create([reconciliation_options.s3_data_path])
            | 'Load S3 Data' >> beam.ParDo(LoadS3Data(reconciliation_options.s3_data_path, config))
            | 'Key S3 by ID' >> beam.Map(lambda x: (x.get('id', x.get('element_id', 'unknown')), x))
        )
        
        # Load BigQuery data
        bq_data = (
            pipeline
            | 'Create Table List' >> beam.Create(tables_to_reconcile)
            | 'Load BQ Data' >> beam.ParDo(LoadBigQueryData(
                reconciliation_options.bigquery_dataset, 
                reconciliation_options.reconciliation_date, 
                config
            ))
            | 'Key BQ by ID' >> beam.Map(lambda x: (x.get('id', x.get('element_id', 'unknown')), x))
        )
        
        # Reconcile data
        reconciliation_results = (
            {'s3': s3_data, 'bigquery': bq_data}
            | 'Group by Key' >> beam.CoGroupByKey()
            | 'Reconcile Data' >> beam.ParDo(ReconcileData(config))
            | 'Prepare Output' >> beam.ParDo(WriteReconciliationResults(output_table, config))
        )
        
        # Write results to BigQuery
        reconciliation_results | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=output_table,
            schema={
                'fields': [
                    {'name': 'record_id', 'type': 'STRING'},
                    {'name': 'status', 'type': 'STRING'},
                    {'name': 'table_name', 'type': 'STRING'},
                    {'name': 'reconciliation_timestamp', 'type': 'TIMESTAMP'},
                    {'name': 'reconciliation_date', 'type': 'DATE'},
                    {'name': 'error_type', 'type': 'STRING'},
                    {'name': 'field_differences_json', 'type': 'STRING'},
                    {'name': 's3_record_json', 'type': 'STRING'},
                    {'name': 'bq_record_json', 'type': 'STRING'},
                    {'name': 'domain', 'type': 'STRING'},
                    {'name': 'pipeline_run_id', 'type': 'STRING'},
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_reconciliation_pipeline()
