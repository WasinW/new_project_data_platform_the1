# dataflow/pipelines/reconciliation_pipeline.py
# dataflow/pipelines/reconciliation_pipeline.py
"""
Reconciliation Dataflow Pipeline - Compare S3 external table with native BigQuery table
Optimized for scalability with proper client management
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
import argparse
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging
import json

# Import optimized client manager
from utils.client_manager import DataflowClientMixin


class ReconciliationOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--s3_external_table', required=True, 
                           help='External table pointing to S3 data')
        parser.add_argument('--native_table', required=True,
                           help='Native BigQuery table to compare against')
        parser.add_argument('--output_table', required=True,
                           help='Output table for reconciliation results')
        parser.add_argument('--key_columns', required=True,
                           help='Comma-separated key columns for comparison')
        parser.add_argument('--comparison_columns', required=True,
                           help='Comma-separated columns to compare values')
        parser.add_argument('--tolerance_config', default='{}',
                           help='JSON config for comparison tolerances')


class RecordMatcher(beam.DoFn, DataflowClientMixin):
    """Match and compare records between S3 and native tables"""
    
    def __init__(self, key_columns: List[str], comparison_columns: List[str], tolerance_config: Dict):
        super().__init__()
        self.key_columns = key_columns
        self.comparison_columns = comparison_columns
        self.tolerance_config = tolerance_config
    
    def process(self, element):
        """Process a pair of (key, {'s3': [records], 'native': [records]})"""
        key, grouped_records = element
        
        s3_records = grouped_records.get('s3', [])
        native_records = grouped_records.get('native', [])
        
        # Handle different scenarios
        if not s3_records and not native_records:
            return  # Should not happen
        
        if s3_records and not native_records:
            for s3_record in s3_records:
                yield self._create_result(key, 'MISSING_IN_NATIVE', s3_record, None)
        
        elif native_records and not s3_records:
            for native_record in native_records:
                yield self._create_result(key, 'MISSING_IN_S3', None, native_record)
        
        else:
            # Both have records - compare them
            for s3_record in s3_records:
                matched = False
                for native_record in native_records:
                    if self._records_match_exactly(s3_record, native_record):
                        yield self._create_result(key, 'MATCH', s3_record, native_record)
                        matched = True
                        break
                
                if not matched:
                    # Find best match for detailed comparison
                    best_match = self._find_best_match(s3_record, native_records)
                    mismatches = self._compare_records(s3_record, best_match)
                    
                    result = self._create_result(key, 'MISMATCH', s3_record, best_match)
                    result['mismatches'] = mismatches
                    result['mismatch_count'] = len(mismatches)
                    yield result
    
    def _create_result(self, key: str, status: str, s3_record: Optional[Dict], 
                      native_record: Optional[Dict]) -> Dict:
        """Create standardized reconciliation result"""
        return {
            'reconciliation_timestamp': datetime.utcnow().isoformat(),
            'reconciliation_date': datetime.utcnow().date().isoformat(),
            'record_key': key,
            'status': status,
            's3_record': json.dumps(s3_record) if s3_record else None,
            'native_record': json.dumps(native_record) if native_record else None,
            'processed_at': datetime.utcnow().isoformat()
        }
    
    def _records_match_exactly(self, s3_record: Dict, native_record: Dict) -> bool:
        """Check if two records match exactly on comparison columns"""
        for col in self.comparison_columns:
            s3_val = s3_record.get(col)
            native_val = native_record.get(col)
            
            if not self._values_match(col, s3_val, native_val):
                return False
        
        return True
    
    def _find_best_match(self, s3_record: Dict, native_records: List[Dict]) -> Dict:
        """Find the native record that best matches the S3 record"""
        if len(native_records) == 1:
            return native_records[0]
        
        # For multiple matches, find the one with fewest differences
        best_match = native_records[0]
        min_differences = len(self.comparison_columns)
        
        for native_record in native_records:
            differences = sum(
                1 for col in self.comparison_columns
                if not self._values_match(col, s3_record.get(col), native_record.get(col))
            )
            
            if differences < min_differences:
                min_differences = differences
                best_match = native_record
        
        return best_match
    
    def _compare_records(self, s3_record: Dict, native_record: Dict) -> List[Dict]:
        """Compare two records and return list of mismatches"""
        mismatches = []
        
        for col in self.comparison_columns:
            s3_val = s3_record.get(col)
            native_val = native_record.get(col)
            
            if not self._values_match(col, s3_val, native_val):
                mismatches.append({
                    'column': col,
                    's3_value': str(s3_val) if s3_val is not None else None,
                    'native_value': str(native_val) if native_val is not None else None,
                    'difference_type': self._get_difference_type(s3_val, native_val)
                })
        
        return mismatches
    
    def _values_match(self, column: str, val1, val2) -> bool:
        """Check if two values match with configured tolerance"""
        if val1 is None and val2 is None:
            return True
        
        if val1 is None or val2 is None:
            return False
        
        # Apply column-specific or type-specific tolerance
        tolerance = self.tolerance_config.get(column, self.tolerance_config.get('default', {}))
        
        # Numeric tolerance
        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            numeric_tolerance = tolerance.get('numeric_percent', 0)
            if numeric_tolerance > 0:
                diff_percent = abs(val1 - val2) / max(abs(val1), abs(val2), 1) * 100
                return diff_percent <= numeric_tolerance
        
        # String comparison (case-insensitive option)
        if tolerance.get('case_insensitive', False):
            return str(val1).lower() == str(val2).lower()
        
        # Default exact match
        return str(val1) == str(val2)
    
    def _get_difference_type(self, val1, val2) -> str:
        """Classify the type of difference between values"""
        if val1 is None:
            return 'missing_in_s3'
        elif val2 is None:
            return 'missing_in_native'
        elif type(val1) != type(val2):
            return 'type_mismatch'
        elif isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            return 'numeric_difference'
        else:
            return 'value_difference'


class StatisticsAggregator(beam.DoFn):
    """Aggregate reconciliation statistics"""
    
    def process(self, element):
        """Process reconciliation results and emit statistics"""
        status, count = element
        
        yield {
            'reconciliation_date': datetime.utcnow().date().isoformat(),
            'status': status,
            'record_count': count,
            'percentage': 0.0,  # Will be calculated in post-processing
            'generated_at': datetime.utcnow().isoformat()
        }


def run_reconciliation(options: ReconciliationOptions):
    """Main reconciliation pipeline"""
    
    key_columns = [col.strip() for col in options.key_columns.split(',')]
    comparison_columns = [col.strip() for col in options.comparison_columns.split(',')]
    tolerance_config = json.loads(options.tolerance_config)
    
    with beam.Pipeline(options=options) as pipeline:
        
        # Read S3 data via external table
        s3_data = (
            pipeline
            | 'ReadS3Data' >> ReadFromBigQuery(
                table=options.s3_external_table,
                use_standard_sql=True
            )
            | 'ConvertS3ToDict' >> beam.Map(lambda x: dict(x))
            | 'KeyS3Records' >> beam.Map(
                lambda x: (
                    '|'.join([str(x.get(col, '')) for col in key_columns]),
                    x
                )
            )
        )
        
        # Read native BigQuery data
        native_data = (
            pipeline
            | 'ReadNativeData' >> ReadFromBigQuery(
                table=options.native_table,
                use_standard_sql=True
            )
            | 'ConvertNativeToDict' >> beam.Map(lambda x: dict(x))
            | 'KeyNativeRecords' >> beam.Map(
                lambda x: (
                    '|'.join([str(x.get(col, '')) for col in key_columns]),
                    x
                )
            )
        )
        
        # Group by key and compare
        comparison_results = (
            {'s3': s3_data, 'native': native_data}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'CompareRecords' >> beam.ParDo(
                RecordMatcher(key_columns, comparison_columns, tolerance_config)
            )
        )
        
        # Write detailed results
        (comparison_results
         | 'WriteDetailedResults' >> WriteToBigQuery(
             table=options.output_table,
             schema='SCHEMA_AUTODETECT',
             create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_TRUNCATE,
             additional_bq_parameters={
                 'timePartitioning': {
                     'type': 'DAY',
                     'field': 'reconciliation_date'
                 }
             }
         ))
        
        # Calculate and write statistics
        stats = (
            comparison_results
            | 'ExtractStatus' >> beam.Map(lambda x: (x['status'], 1))
            | 'CountByStatus' >> beam.CombinePerKey(sum)
            | 'FormatStats' >> beam.ParDo(StatisticsAggregator())
        )
        
        (stats
         | 'WriteStats' >> WriteToBigQuery(
             table=options.output_table.replace('_results', '_stats'),
             schema='SCHEMA_AUTODETECT',
             create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    reconciliation_options = pipeline_options.view_as(ReconciliationOptions)
    
    run_reconciliation(reconciliation_options)


if __name__ == '__main__':
    main()
