# dataflow/pipelines/hybrid_pipeline.py
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows, SlidingWindows, Sessions
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import json
import yaml
import argparse
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging

# Import windowing utilities
from utils.windowing import WindowingConfig, WindowedDependencyChecker, WindowedAggregator, BatchWindowProcessor, WindowAuditLogger
from utils.dataplex_manager import DataplexManager
from transforms.distributor import DataDistributor
from transforms.complex_transforms import ColumnMapper, ComplexTransform
from utils.audit_logger import AuditLogger

class HybridPipelineOptions(PipelineOptions):
    """Custom pipeline options for hybrid pipeline"""
    
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--mode', required=True, choices=['batch', 'realtime'])
        parser.add_argument('--config_path', required=True)
        parser.add_argument('--domain', required=True)
        parser.add_argument('--batch_window_hours', type=int, default=1)
        parser.add_argument('--enable_windowing', type=bool, default=True)

class DependencyChecker(beam.DoFn):
    """Check upstream dependencies before processing (legacy for non-windowed mode)"""
    
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
        from google.cloud import bigquery
        
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

class DataplexLineageTracker(beam.DoFn):
    """Track data lineage in Dataplex for batch and realtime pipelines"""
    
    def __init__(self, pipeline_config: Dict[str, Any]):
        self.pipeline_config = pipeline_config
        self.project_id = pipeline_config.get('project')
        self.region = pipeline_config.get('region')
        self.domain = pipeline_config.get('domain')
        self.pipeline_mode = pipeline_config.get('mode')
    
    def setup(self):
        """Initialize Dataplex manager"""
        self.dataplex_manager = DataplexManager(self.project_id, self.region)
    
    def process(self, element):
        """Track lineage for processed element"""
        try:
            # Extract source and target information
            source_table = self.pipeline_config.get('source', {}).get('table')
            target_table = element.get('_target_table')
            
            if not source_table or not target_table:
                yield element
                return
            
            # Create pipeline information
            pipeline_info = {
                'name': f"{self.domain}_{self.pipeline_mode}_pipeline",
                'type': self.pipeline_mode,
                'domain': self.domain,
                'execution_id': element.get('_execution_id', f"exec_{int(datetime.utcnow().timestamp())}")
            }
            
            # Create source information
            source_dataset = self.pipeline_config.get('source', {}).get('dataset')
            source_info = {
                'fully_qualified_name': f"bigquery:{self.project_id}.{source_dataset}.{source_table}"
            }
            
            # Create target information
            target_dataset = self.pipeline_config.get('target_datasets', {}).get(
                target_table.split('_')[0], f"{self.domain}_raw"
            )
            target_info = [{
                'table_name': target_table,
                'fully_qualified_name': f"bigquery:{self.project_id}.{target_dataset}.{target_table}"
            }]
            
            # Track lineage
            self.dataplex_manager.track_pipeline_lineage(
                pipeline_info, source_info, target_info
            )
            
            # Add lineage tracking metadata to element
            element['_lineage_tracked'] = True
            element['_lineage_timestamp'] = datetime.utcnow().isoformat()
            
            yield element
            
        except Exception as e:
            logging.error(f"Failed to track lineage: {e}")
            # Continue processing even if lineage tracking fails
            element['_lineage_tracked'] = False
            element['_lineage_error'] = str(e)
            yield element

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
        from google.cloud import bigquery
        
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

def run_pipeline_with_windowing(pipeline_options: PipelineOptions):
    """Enhanced pipeline with comprehensive windowing support"""
    
    # Parse options
    options = pipeline_options.view_as(StandardOptions)
    custom_options = pipeline_options.view_as(HybridPipelineOptions)
    
    # Load configuration
    from utils.config_loader import ConfigLoader
    config_loader = ConfigLoader()
    config = config_loader.load_config(custom_options.config_path)
    
    # Initialize windowing configuration
    windowing_config = WindowingConfig(config.get('windowing', {}))
    
    # Determine if batch mode needs windowing
    use_batch_windowing = (
        custom_options.mode == 'batch' and 
        config.get('windowing', {}).get('batch_windowing', {}).get('enabled', False)
    )
    
    # Set streaming mode
    options.streaming = (custom_options.mode == 'realtime')
    
    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Step 1: Read source data with optional windowing
        if custom_options.mode == 'realtime':
            messages = (
                pipeline
                | 'ReadFromPubSub' >> ReadFromPubSub(
                    subscription=config['pubsub']['subscription'],
                    with_attributes=True,
                    timestamp_attribute='publish_time'
                )
                | 'ParsePubSubMessage' >> beam.Map(lambda x: json.loads(x.data.decode('utf-8')))
            )
            
            # Apply ingestion windowing
            messages = windowing_config.apply_window(messages, 'message_ingestion')
            
        else:  # Batch mode
            hours_back = custom_options.batch_window_hours
            query = f"""
                SELECT *
                FROM `{config['source']['project']}.{config['source']['dataset']}.{config['source']['table']}`
                WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
                ORDER BY updated_at
            """
            
            messages = (
                pipeline
                | 'ReadFromBigQuery' >> ReadFromBigQuery(query=query, use_standard_sql=True)
                | 'ConvertToDict' >> beam.Map(lambda x: dict(x))
            )
            
            # Apply batch windowing if enabled
            if use_batch_windowing:
                batch_window_config = config['windowing']['batch_windowing']
                messages = (
                    messages
                    | 'AddTimestamps' >> beam.Map(
                        lambda x: beam.window.TimestampedValue(
                            x, 
                            datetime.fromisoformat(str(x.get('updated_at', datetime.utcnow()))).timestamp()
                        )
                    )
                    | 'BatchWindow' >> beam.WindowInto(
                        FixedWindows(batch_window_config.get('duration_seconds', 300))
                    )
                )
        
        # Step 2: Check dependencies with windowing
        if windowing_config.enabled and custom_options.mode == 'realtime':
            messages = windowing_config.apply_window(messages, 'dependency_check')
            dependency_checker = WindowedDependencyChecker(config.get('dependencies', []))
        else:
            dependency_checker = DependencyChecker(config.get('dependencies', []))
        
        dependency_results = (
            messages
            | 'CheckDependencies' >> beam.ParDo(dependency_checker).with_outputs('main', 'failed_dependency')
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
        
        # Step 3: Fetch with windowing
        if windowing_config.enabled and custom_options.mode == 'realtime':
            validated_messages = windowing_config.apply_window(validated_messages, 'fetch_source')
        
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
        
        # Step 4: Distribution with windowing
        if windowing_config.enabled and custom_options.mode == 'realtime':
            fetched_records = windowing_config.apply_window(fetched_records, 'distribution')
        
        distributed = (
            fetched_records
            | 'DistributeToTables' >> beam.ParDo(
                DataDistributor(config['distribution_mapping'])
            ).with_outputs(*config['distribution_mapping'].keys())
        )
        
        # Step 5: Process each table with transformation windowing
        for table in config['distribution_mapping'].keys():
            table_records = distributed[table]
            
            # Apply transformation windowing
            if windowing_config.enabled and custom_options.mode == 'realtime':
                table_records = windowing_config.apply_window(table_records, 'transformation')
            
            # Column mapping
            mapped_records = (
                table_records
                | f'MapColumns_{table}' >> beam.ParDo(
                    ColumnMapper(table, config['column_mappings'].get(table, {}))
                )
            )
            
            # Complex transformations
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
            else:
                final_records = mapped_records
            
            # Optional aggregation with windowing
            if config.get('aggregation', {}).get('enabled', False) and table in config['aggregation'].get('tables', []):
                if windowing_config.enabled:
                    final_records = windowing_config.apply_window(final_records, 'aggregation')
                
                aggregated = (
                    final_records
                    | f'GroupByKey_{table}' >> beam.Map(lambda x: (x.get('member_id'), x))
                    | f'Aggregate_{table}' >> beam.CombinePerKey(WindowedAggregator())
                    | f'FlattenAggregated_{table}' >> beam.FlatMap(lambda kv: kv[1]['elements'])
                )
                final_records = aggregated
            
            # Batch processing for batch mode
            if use_batch_windowing and custom_options.mode == 'batch':
                batch_size = config['windowing']['batch_windowing'].get('max_elements', 10000)
                final_records = (
                    final_records
                    | f'BatchProcess_{table}' >> beam.ParDo(BatchWindowProcessor(batch_size))
                    | f'FlattenBatches_{table}' >> beam.FlatMap(lambda x: x)
                )
            
            # Write to BigQuery (no windowing for writes)
            dataset = config['target_datasets'].get(table.split('_')[0], config['domain'] + '_raw')
            
            # Add write-time batching for efficiency
            write_batch_size = config.get('performance', {}).get('write_batch_size', 500)
            
            (final_records
             | f'WriteToBigQuery_{table}' >> WriteToBigQuery(
                 table=f"{config['project']}.{dataset}.{table}",
                 schema='SCHEMA_AUTODETECT',
                 create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
                 write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND,
                 batch_size=write_batch_size,
                 triggering_frequency=30 if custom_options.mode == 'realtime' else None
             ))
        
        # Step 6: Windowed audit logging
        if windowing_config.enabled and custom_options.mode == 'realtime':
            audit_records = windowing_config.apply_window(fetched_records, 'audit')
            
            # Aggregate audit logs by window
            (audit_records
             | 'PrepareAuditKey' >> beam.Map(lambda x: ((x.get('_window_start', 'unknown'), x.get('_window_end', 'unknown')), x))
             | 'GroupAuditByWindow' >> beam.GroupByKey()
             | 'CreateAuditSummary' >> beam.Map(lambda kv: {
                 'window_start': kv[0][0],
                 'window_end': kv[0][1],
                 'record_count': len(list(kv[1])),
                 'pipeline_name': f"{config['domain']}_pipeline",
                 'pipeline_mode': custom_options.mode,
                 'audit_timestamp': datetime.utcnow().isoformat()
             })
             | 'WriteAuditSummary' >> WriteToBigQuery(
                 table=f"{config['project']}.{config['domain']}_audit.window_summaries",
                 schema='SCHEMA_AUTODETECT',
                 write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
             ))
        else:
            # Standard audit logging for non-windowed or batch mode
            (fetched_records
             | 'StandardAuditLog' >> beam.ParDo(WindowAuditLogger({
                 'name': f"{config['domain']}_pipeline",
                 'mode': custom_options.mode,
                 'domain': config['domain'],
                 'project': config['project']
             }))
             | 'WriteStandardAudit' >> WriteToBigQuery(
                 table=f"{config['project']}.{config['domain']}_audit.processing_logs",
                 schema='SCHEMA_AUTODETECT',
                 write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
             ))

def run_pipeline(pipeline_options: PipelineOptions):
    """Legacy pipeline function - now calls windowing version"""
    logging.info("Using enhanced pipeline with windowing support")
    return run_pipeline_with_windowing(pipeline_options)
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
            
            # Add target table metadata and track lineage
            final_records_with_lineage = (
                final_records
                | f'AddTargetTableMetadata_{table}' >> beam.Map(
                    lambda x, table_name=table: {**x, '_target_table': table_name}
                )
                | f'TrackDataplexLineage_{table}' >> beam.ParDo(
                    DataplexLineageTracker({
                        **config,
                        'mode': pipeline_options.mode
                    })
                )
            )
            
            # Write to BigQuery
            dataset = config['target_datasets'].get(table.split('_')[0], config['domain'] + '_raw')
            
            (final_records_with_lineage
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
    """Main entry point with windowing support"""
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Log windowing configuration
    custom_options = pipeline_options.view_as(HybridPipelineOptions)
    logging.info(f"Starting pipeline in {custom_options.mode} mode with windowing")
    
    run_pipeline_with_windowing(pipeline_options)

if __name__ == '__main__':
    main()
