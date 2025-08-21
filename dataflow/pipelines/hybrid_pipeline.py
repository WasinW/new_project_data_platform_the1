# dataflow/pipelines/hybrid_pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import json
import yaml
import argparse
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging

class HybridPipelineOptions(PipelineOptions):
    """Custom pipeline options for hybrid pipeline"""
    
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--mode', required=True, choices=['batch', 'realtime'])
        parser.add_argument('--config_path', required=True)
        parser.add_argument('--domain', required=True)
        parser.add_argument('--batch_window_hours', type=int, default=1)

class DependencyChecker(beam.DoFn):
    """Check upstream dependencies before processing"""
    
    def __init__(self, dependencies: List[Dict]):
        self.dependencies = dependencies
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        # Check all dependencies
        for dep in self.dependencies:
            if not self._check_dependency(dep):
                yield beam.pvalue.TaggedOutput('failed_dependency', {
                    'element': element,
                    'failed_dependency': dep,
                    'timestamp': datetime.utcnow().isoformat()
                })
                return
        
        # All dependencies passed
        yield beam.pvalue.TaggedOutput('main', element)
    
    def _check_dependency(self, dep: Dict) -> bool:
        """Check individual dependency"""
        query = f"""
            SELECT COUNT(*) as count
            FROM `{dep['project']}.{dep['dataset']}.{dep['table']}`
            WHERE {dep['condition']}
        """
        
        try:
            result = self.bq_client.query(query).result()
            for row in result:
                return row.count > 0
        except Exception as e:
            logging.error(f"Dependency check failed: {e}")
            return False
        
        return False

class FetchFromBigQuery(beam.DoFn):
    """Fetch full records from BigQuery based on keys"""
    
    def __init__(self, project: str, dataset: str, table: str):
        self.project = project
        self.dataset = dataset
        self.table = table
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        # Extract key for lookup
        key_value = element.get('id') or element.get('member_id')
        
        if not key_value:
            yield beam.pvalue.TaggedOutput('fetch_error', {
                'input': element,
                'error': 'No key found for lookup',
                'timestamp': datetime.utcnow().isoformat()
            })
            return
        
        # Query for full record
        query = f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{self.table}`
            WHERE id = @key_value OR member_id = @key_value
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("key_value", "STRING", str(key_value))
            ]
        )
        
        try:
            result = self.bq_client.query(query, job_config=job_config).result()
            records = [dict(row) for row in result]
            
            if records:
                for record in records:
                    # Add metadata
                    record['_timestamp'] = element.get('_timestamp', datetime.utcnow().isoformat())
                    record['_source'] = 'bigquery_fetch'
                    yield beam.pvalue.TaggedOutput('main', record)
            else:
                yield beam.pvalue.TaggedOutput('fetch_error', {
                    'input': element,
                    'error': 'No records found for key',
                    'key': key_value,
                    'timestamp': datetime.utcnow().isoformat()
                })
        
        except Exception as e:
            logging.error(f"BigQuery fetch failed: {e}")
            yield beam.pvalue.TaggedOutput('fetch_error', {
                'input': element,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })

class DataDistributor(beam.DoFn):
    """Distribute data to multiple target tables based on mapping"""
    
    def __init__(self, distribution_mapping: Dict[str, List[str]]):
        self.distribution_mapping = distribution_mapping
    
    def process(self, element):
        for table, columns in self.distribution_mapping.items():
            # Create record with only specified columns
            table_record = {}
            for col in columns:
                if col in element:
                    table_record[col] = element[col]
            
            # Add metadata
            table_record['_source_timestamp'] = element.get('_timestamp', datetime.utcnow().isoformat())
            table_record['_ingestion_timestamp'] = datetime.utcnow().isoformat()
            
            yield beam.pvalue.TaggedOutput(table, table_record)

class ColumnMapper(beam.DoFn):
    """Apply column mapping and transformations"""
    
    def __init__(self, table: str, mapping: Dict):
        self.table = table
        self.mapping = mapping
    
    def process(self, element):
        result = {}
        
        for target_col, source_spec in self.mapping.items():
            if isinstance(source_spec, str):
                # Simple rename
                result[target_col] = element.get(source_spec)
            elif isinstance(source_spec, dict):
                # Complex mapping
                if source_spec['type'] == 'concat':
                    values = [str(element.get(c, '')) for c in source_spec['columns']]
                    result[target_col] = source_spec.get('separator', '').join(values)
                elif source_spec['type'] == 'constant':
                    result[target_col] = source_spec['value']
                elif source_spec['type'] == 'expression':
                    # Evaluate simple Python expression
                    try:
                        result[target_col] = eval(source_spec['expression'], {'element': element})
                    except Exception as e:
                        logging.error(f"Expression evaluation failed: {e}")
                        result[target_col] = None
        
        # Add table metadata
        result['_table'] = self.table
        result['_mapped_timestamp'] = datetime.utcnow().isoformat()
        
        yield result

class ComplexTransform(beam.DoFn):
    """Apply complex business logic transformations"""
    
    def __init__(self, transform_module: str, params: Dict):
        self.transform_module = transform_module
        self.params = params
    
    def setup(self):
        # Dynamically import transform module
        import importlib
        module_path, class_name = self.transform_module.rsplit('.', 1)
        module = importlib.import_module(module_path)
        self.transform_class = getattr(module, class_name)
        self.transformer = self.transform_class(self.params)
    
    def process(self, element):
        try:
            transformed = self.transformer.transform(element)
            yield transformed
        except Exception as e:
            logging.error(f"Transform failed for {self.transform_module}: {e}")
            yield beam.pvalue.TaggedOutput('transform_error', {
                'input': element,
                'error': str(e),
                'transform': self.transform_module,
                'timestamp': datetime.utcnow().isoformat()
            })

class AuditLogger(beam.DoFn):
    """Log audit information for processed records"""
    
    def __init__(self, pipeline_config: Dict):
        self.pipeline_config = pipeline_config
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        audit_record = {
            'pipeline_name': self.pipeline_config['name'],
            'pipeline_mode': self.pipeline_config['mode'],
            'domain': self.pipeline_config['domain'],
            'record_id': element.get('id') or element.get('member_id'),
            'processing_timestamp': datetime.utcnow().isoformat(),
            'source_timestamp': element.get('_timestamp'),
            'status': 'SUCCESS'
        }
        
        # Write to audit table
        table_id = f"{self.pipeline_config['project']}.{self.pipeline_config['domain']}_audit.processing_logs"
        
        try:
            errors = self.bq_client.insert_rows_json(table_id, [audit_record])
            if errors:
                logging.error(f"Audit logging failed: {errors}")
        except Exception as e:
            logging.error(f"Audit logging exception: {e}")
        
        # Pass through the element
        yield element

def run_pipeline(pipeline_options: HybridPipelineOptions):
    """Main pipeline execution"""
    
    # Load configuration
    with open(pipeline_options.config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Set streaming mode
    options = pipeline_options.view_as(StandardOptions)
    options.streaming = (pipeline_options.mode == 'realtime')
    
    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Step 1: Read source data
        if pipeline_options.mode == 'realtime':
            # Read from Pub/Sub
            messages = (
                pipeline
                | 'ReadFromPubSub' >> ReadFromPubSub(
                    subscription=config['pubsub']['subscription']
                )
                | 'ParsePubSubMessage' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            )
        else:
            # Read from BigQuery (batch mode)
            hours_back = pipeline_options.batch_window_hours
            query = f"""
                SELECT *
                FROM `{config['source']['project']}.{config['source']['dataset']}.{config['source']['table']}`
                WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
            """
            
            messages = (
                pipeline
                | 'ReadFromBigQuery' >> ReadFromBigQuery(query=query, use_standard_sql=True)
                | 'ConvertToDict' >> beam.Map(lambda x: dict(x))
            )
        
        # Step 2: Check dependencies
        dependency_results = (
            messages
            | 'CheckDependencies' >> beam.ParDo(
                DependencyChecker(config.get('dependencies', []))
            ).with_outputs('main', 'failed_dependency')
        )
        
        validated_messages = dependency_results['main']
        failed_dependencies = dependency_results['failed_dependency']
        
        # Handle failed dependencies
        (failed_dependencies
         | 'FormatFailedDeps' >> beam.Map(lambda x: json.dumps(x))
         | 'WriteFailedDepsToDLQ' >> WriteToBigQuery(
             table=f"{config['project']}.{config['domain']}_audit.failed_dependencies",
             schema='SCHEMA_AUTODETECT',
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))
        
        # Step 3: Fetch full records from source
        source_records = (
            validated_messages
            | 'FetchSourceData' >> beam.ParDo(
                FetchFromBigQuery(
                    config['source']['project'],
                    config['source']['dataset'],
                    config['source']['table']
                )
            ).with_outputs('main', 'fetch_error')
        )
        
        fetched_records = source_records['main']
        fetch_errors = source_records['fetch_error']
        
        # Handle fetch errors
        (fetch_errors
         | 'FormatFetchErrors' >> beam.Map(lambda x: json.dumps(x))
         | 'WriteFetchErrorsToDLQ' >> WriteToBigQuery(
             table=f"{config['project']}.{config['domain']}_audit.fetch_errors",
             schema='SCHEMA_AUTODETECT',
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))
        
        # Step 4: Distribute to multiple tables
        distributed = (
            fetched_records
            | 'DistributeToTables' >> beam.ParDo(
                DataDistributor(config['distribution_mapping'])
            ).with_outputs(*config['distribution_mapping'].keys())
        )
        
        # Step 5: Process each table
        for table in config['distribution_mapping'].keys():
            table_records = distributed[table]
            
            # Apply column mapping
            mapped_records = (
                table_records
                | f'MapColumns_{table}' >> beam.ParDo(
                    ColumnMapper(table, config['column_mappings'].get(table, {}))
                )
            )
            
            # Apply complex transformations if configured
            if table in config.get('complex_transforms', {}):
                transform_config = config['complex_transforms'][table]
                transformed = (
                    mapped_records
                    | f'ComplexTransform_{table}' >> beam.ParDo(
                        ComplexTransform(
                            transform_config['module'],
                            transform_config.get('params', {})
                        )
                    ).with_outputs('main', 'transform_error')
                )
                
                final_records = transformed['main']
                transform_errors = transformed['transform_error']
                
                # Handle transform errors
                (transform_errors
                 | f'FormatTransformErrors_{table}' >> beam.Map(lambda x: json.dumps(x))
                 | f'WriteTransformErrorsToDLQ_{table}' >> WriteToBigQuery(
                     table=f"{config['project']}.{config['domain']}_audit.transform_errors",
                     schema='SCHEMA_AUTODETECT',
                     write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
                 ))
            else:
                final_records = mapped_records
            
            # Write to BigQuery
            dataset = config['target_datasets'].get(table.split('_')[0], config['domain'] + '_raw')
            
            (final_records
             | f'WriteToBigQuery_{table}' >> WriteToBigQuery(
                 table=f"{config['project']}.{dataset}.{table}",
                 schema='SCHEMA_AUTODETECT',
                 create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
                 write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
             ))
        
        # Step 6: Audit logging
        (fetched_records
         | 'AuditLogging' >> beam.ParDo(AuditLogger({
             'name': f"{config['domain']}_pipeline",
             'mode': pipeline_options.mode,
             'domain': config['domain'],
             'project': config['project']
         }))
        )

def main():
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    hybrid_options = pipeline_options.view_as(HybridPipelineOptions)
    
    run_pipeline(hybrid_options)

if __name__ == '__main__':
    main()
