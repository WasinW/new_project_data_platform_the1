ผมจะปรับปรุง Dataflow pipeline ให้รองรับ windowing configuration ที่ flexible สำหรับทั้ง realtime และ batch modes:

## Updated Hybrid Dataflow Pipeline with Windowing

### 1. Enhanced Configuration with Windowing

```yaml
# config/pipeline_config.yaml (updated windowing section)
windowing:
  enabled: true  # Global windowing toggle
  
  # Default window configuration
  default:
    type: fixed  # fixed, sliding, session
    duration_seconds: 30
    allowed_lateness_seconds: 60
    accumulation_mode: discarding  # discarding, accumulating
    trigger:
      type: after_watermark
      early_firings: 1
      late_firings: 1
  
  # Step-specific window configurations
  steps:
    message_ingestion:
      enabled: true
      type: fixed
      duration_seconds: 10
      accumulation_mode: accumulating
      
    dependency_check:
      enabled: true  
      type: fixed
      duration_seconds: 30
      allowed_lateness_seconds: 120
      
    fetch_source:
      enabled: true
      type: sliding
      duration_seconds: 60
      period_seconds: 30
      
    distribution:
      enabled: true
      type: fixed
      duration_seconds: 30
      
    transformation:
      enabled: true
      type: session
      gap_duration_seconds: 10
      
    aggregation:
      enabled: true
      type: fixed
      duration_seconds: 60
      trigger:
        type: repeatedly
        after_count: 100
        
    write_output:
      enabled: false  # No windowing for writes
      
  # Batch mode specific
  batch_windowing:
    enabled: true  # Enable windowing for batch mode
    type: fixed
    duration_seconds: 300  # 5 minute windows for batch
    max_elements: 10000  # Process in chunks
```

### 2. Updated Hybrid Pipeline with Windowing

```python
# dataflow/pipelines/hybrid_pipeline.py (enhanced version)
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

class WindowingConfig:
    """Helper class to manage windowing configurations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.enabled = config.get('enabled', False)
        
    def apply_window(self, pcoll, step_name: str):
        """Apply windowing to a PCollection based on configuration"""
        
        if not self.enabled:
            return pcoll
            
        # Get step-specific or default config
        step_config = self.config.get('steps', {}).get(step_name, self.config.get('default', {}))
        
        if not step_config.get('enabled', True):
            return pcoll
            
        # Create window function
        window_fn = self._create_window_fn(step_config)
        
        # Create trigger
        trigger_fn = self._create_trigger(step_config.get('trigger', {}))
        
        # Apply windowing
        windowed = (
            pcoll
            | f'Window_{step_name}' >> beam.WindowInto(
                window_fn,
                trigger=trigger_fn,
                accumulation_mode=self._get_accumulation_mode(step_config),
                allowed_lateness=step_config.get('allowed_lateness_seconds', 0)
            )
        )
        
        return windowed
    
    def _create_window_fn(self, config: Dict):
        """Create appropriate window function based on config"""
        window_type = config.get('type', 'fixed')
        
        if window_type == 'fixed':
            return FixedWindows(config.get('duration_seconds', 60))
        elif window_type == 'sliding':
            return SlidingWindows(
                config.get('duration_seconds', 60),
                config.get('period_seconds', 30)
            )
        elif window_type == 'session':
            return Sessions(config.get('gap_duration_seconds', 10))
        else:
            return FixedWindows(60)  # Default fallback
    
    def _create_trigger(self, trigger_config: Dict):
        """Create trigger based on configuration"""
        trigger_type = trigger_config.get('type', 'after_watermark')
        
        if trigger_type == 'after_watermark':
            base_trigger = trigger.AfterWatermark(
                early=trigger.AfterCount(trigger_config.get('early_firings', 1)),
                late=trigger.AfterCount(trigger_config.get('late_firings', 1))
            )
        elif trigger_type == 'after_count':
            base_trigger = trigger.AfterCount(trigger_config.get('count', 100))
        elif trigger_type == 'after_processing_time':
            base_trigger = trigger.AfterProcessingTime(trigger_config.get('delay_seconds', 60))
        elif trigger_type == 'repeatedly':
            base_trigger = trigger.Repeatedly(
                trigger.AfterCount(trigger_config.get('after_count', 100))
            )
        else:
            base_trigger = trigger.Default()
        
        return base_trigger
    
    def _get_accumulation_mode(self, config: Dict):
        """Get accumulation mode from config"""
        mode = config.get('accumulation_mode', 'discarding')
        
        if mode == 'accumulating':
            return trigger.AccumulationMode.ACCUMULATING
        else:
            return trigger.AccumulationMode.DISCARDING

class WindowedDependencyChecker(beam.DoFn):
    """Enhanced dependency checker with window context"""
    
    def __init__(self, dependencies: List[Dict]):
        self.dependencies = dependencies
        self.window_info = {}
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
        # Log window information
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        
        logging.info(f"Processing element in window: {window_start} to {window_end}")
        
        # Check dependencies with window context
        for dep in self.dependencies:
            if not self._check_dependency_with_window(dep, window_start, window_end):
                yield beam.pvalue.TaggedOutput('failed_dependency', {
                    'element': element,
                    'failed_dependency': dep,
                    'window_start': window_start.isoformat(),
                    'window_end': window_end.isoformat(),
                    'timestamp': datetime.utcnow().isoformat()
                })
                return
        
        # Add window info to element
        element['_window_start'] = window_start.isoformat()
        element['_window_end'] = window_end.isoformat()
        element['_processing_time'] = datetime.utcnow().isoformat()
        
        yield beam.pvalue.TaggedOutput('main', element)
    
    def _check_dependency_with_window(self, dep: Dict, window_start, window_end) -> bool:
        """Check dependency within window context"""
        query = f"""
            SELECT COUNT(*) as count
            FROM `{dep['project']}.{dep['dataset']}.{dep['table']}`
            WHERE {dep['condition']}
            AND created_timestamp >= @window_start
            AND created_timestamp < @window_end
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end)
            ]
        )
        
        try:
            result = self.bq_client.query(query, job_config=job_config).result()
            for row in result:
                return row.count > 0
        except Exception as e:
            logging.error(f"Dependency check failed: {e}")
            return False
        
        return False

class WindowedAggregator(beam.CombineFn):
    """Custom aggregator for windowed data"""
    
    def create_accumulator(self):
        return {
            'count': 0,
            'elements': [],
            'window_info': {}
        }
    
    def add_input(self, accumulator, element):
        accumulator['count'] += 1
        accumulator['elements'].append(element)
        
        # Track window metadata
        if '_window_start' in element:
            accumulator['window_info']['start'] = element['_window_start']
            accumulator['window_info']['end'] = element['_window_end']
        
        return accumulator
    
    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged['count'] += acc['count']
            merged['elements'].extend(acc['elements'])
            if acc['window_info']:
                merged['window_info'] = acc['window_info']
        return merged
    
    def extract_output(self, accumulator):
        return {
            'count': accumulator['count'],
            'elements': accumulator['elements'],
            'window_info': accumulator['window_info'],
            'aggregation_time': datetime.utcnow().isoformat()
        }

class BatchWindowProcessor(beam.DoFn):
    """Special processor for batch mode with optional windowing"""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.current_batch = []
    
    def process(self, element):
        self.current_batch.append(element)
        
        if len(self.current_batch) >= self.batch_size:
            yield self.current_batch
            self.current_batch = []
    
    def finish_bundle(self):
        if self.current_batch:
            yield beam.utils.windowed_value.WindowedValue(
                value=self.current_batch,
                timestamp=0,
                windows=[beam.transforms.window.GlobalWindow()]
            )
            self.current_batch = []

def run_pipeline_with_windowing(pipeline_options: PipelineOptions):
    """Enhanced pipeline with comprehensive windowing support"""
    
    # Parse options
    options = pipeline_options.view_as(StandardOptions)
    custom_options = pipeline_options.view_as(HybridPipelineOptions)
    
    # Load configuration
    with open(custom_options.config_path, 'r') as f:
        config = yaml.safe_load(f)
    
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
        
        dependency_results = (
            messages
            | 'CheckDependencies' >> beam.ParDo(
                WindowedDependencyChecker(config.get('dependencies', []))
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
            write_batch_size = config.get('write_batch_size', 500)
            
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
                 'record_count': len(kv[1]),
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
             | 'StandardAuditLog' >> beam.ParDo(AuditLogger({
                 'name': f"{config['domain']}_pipeline",
                 'mode': custom_options.mode,
                 'domain': config['domain'],
                 'project': config['project']
             }))
            )

class HybridPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--mode', required=True, choices=['batch', 'realtime'])
        parser.add_argument('--config_path', required=True)
        parser.add_argument('--domain', required=True)
        parser.add_argument('--batch_window_hours', type=int, default=1)
        parser.add_argument('--enable_windowing', type=bool, default=True)

def main():
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    run_pipeline_with_windowing(pipeline_options)

if __name__ == '__main__':
    main()
```

### 3. Monitoring Queries for Windowed Processing

```sql
-- Window processing statistics
CREATE OR REPLACE VIEW member_audit.window_processing_stats AS
SELECT 
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as processing_date,
    window_start,
    window_end,
    pipeline_mode,
    COUNT(*) as window_count,
    SUM(record_count) as total_records,
    AVG(record_count) as avg_records_per_window,
    MIN(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) as first_processed,
    MAX(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) as last_processed,
    TIMESTAMP_DIFF(
        MAX(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)),
        MIN(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)),
        SECOND
    ) as processing_duration_seconds
FROM `project.member_audit.window_summaries`
GROUP BY processing_date, window_start, window_end, pipeline_mode;

-- Window latency analysis
SELECT 
    pipeline_mode,
    EXTRACT(HOUR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as hour_of_day,
    AVG(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    )) as avg_window_latency_seconds,
    PERCENTILE_CONT(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    ), 0.5) OVER (PARTITION BY pipeline_mode) as median_latency,
    PERCENTILE_CONT(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    ), 0.95) OVER (PARTITION BY pipeline_mode) as p95_latency
FROM `project.member_audit.window_summaries`
WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) = CURRENT_DATE()
GROUP BY pipeline_mode, hour_of_day
ORDER BY pipeline_mode, hour_of_day;
```

### Key Features ของ Windowing Implementation:

1. **Flexible Configuration**: สามารถ config windowing แยกตาม step ได้
2. **Multiple Window Types**: รองรับ Fixed, Sliding, และ Session windows
3. **Trigger Support**: มี trigger types หลายแบบ (watermark, count, time-based)
4. **Batch Mode Optimization**: เปิด windowing สำหรับ batch ได้เพื่อ performance
5. **Window Context Tracking**: เก็บ window metadata ไว้ใน elements
6. **Aggregation Support**: รองรับ windowed aggregation
7. **Monitoring**: มี queries สำหรับ monitor window processing

การใช้งาน windowing จะช่วย:
- **Realtime**: จัดการ late data, ควบคุม throughput, และ aggregate data
- **Batch**: แบ่ง data เป็น chunks เพื่อ parallel processing และ memory management