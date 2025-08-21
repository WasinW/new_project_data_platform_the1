# dataflow/utils/windowing.py
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows, SlidingWindows, Sessions
from typing import Dict, List, Any
from datetime import datetime
import logging
from google.cloud import bigquery

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

class WindowAuditLogger(beam.DoFn):
    """Audit logger that captures window information"""
    
    def __init__(self, config: Dict):
        self.config = config
    
    def process(self, element, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
        # Extract window information
        window_start = window.start.to_utc_datetime() if hasattr(window, 'start') else None
        window_end = window.end.to_utc_datetime() if hasattr(window, 'end') else None
        
        audit_record = {
            'pipeline_name': self.config.get('name', 'unknown'),
            'pipeline_mode': self.config.get('mode', 'unknown'),
            'domain': self.config.get('domain', 'unknown'),
            'project': self.config.get('project', 'unknown'),
            'element_id': element.get('_element_id', element.get('member_id', 'unknown')),
            'window_start': window_start.isoformat() if window_start else None,
            'window_end': window_end.isoformat() if window_end else None,
            'processing_timestamp': datetime.utcnow().isoformat(),
            'element_timestamp': timestamp.to_utc_datetime().isoformat() if timestamp else None,
            'target_table': element.get('_target_table', 'unknown'),
            'status': 'processed'
        }
        
        yield audit_record
