# dataflow/pipelines/reconciliation_pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
import argparse
from typing import Dict, List, Tuple
from datetime import datetime
import logging

class ReconciliationOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--s3_table', required=True)
        parser.add_argument('--native_table', required=True)
        parser.add_argument('--output_table', required=True)
        parser.add_argument('--key_columns', required=True)
        parser.add_argument('--comparison_columns', required=True)

class RecordComparator(beam.DoFn):
    """Compare records between S3 and native tables"""
    
    def __init__(self, key_columns: List[str], comparison_columns: List[str]):
        self.key_columns = key_columns
        self.comparison_columns = comparison_columns
    
    def process(self, element):
        s3_record, native_record = element
        
        # Create comparison result
        result = {
            'reconciliation_timestamp': datetime.utcnow().isoformat(),
            'key': self._create_key(s3_record or native_record),
            'status': 'UNKNOWN'
        }
        
        if s3_record and not native_record:
            result['status'] = 'MISSING_IN_NATIVE'
            result['details'] = 'Record exists in S3 but not in native table'
            result['s3_record'] = s3_record
        elif native_record and not s3_record:
            result['status'] = 'MISSING_IN_S3'
            result['details'] = 'Record exists in native table but not in S3'
            result['native_record'] = native_record
        elif s3_record and native_record:
            # Compare field values
            mismatches = []
            for col in self.comparison_columns:
                s3_value = s3_record.get(col)
                native_value = native_record.get(col)
                
                if s3_value != native_value:
                    mismatches.append({
                        'column': col,
                        's3_value': str(s3_value),
                        'native_value': str(native_value)
                    })
            
            if mismatches:
                result['status'] = 'MISMATCH'
                result['details'] = f"Found {len(mismatches)} column mismatches"
                result['mismatches'] = mismatches
                result['s3_record'] = s3_record
                result['native_record'] = native_record
            else:
                result['status'] = 'MATCH'
                result['details'] = 'Records match perfectly'
        
        yield result
    
    def _create_key(self, record: Dict) -> str:
        """Create composite key from key columns"""
        if not record:
            return 'UNKNOWN'
        
        key_values = [str(record.get(col, '')) for col in self.key_columns]
        return '|'.join(key_values)

def run_reconciliation(options: ReconciliationOptions):
    """Main reconciliation pipeline"""
    
    key_columns = options.key_columns.split(',')
    comparison_columns = options.comparison_columns.split(',')
    
    with beam.Pipeline(options=options) as pipeline:
        
        # Read S3 data (external table)
        s3_data = (
            pipeline
            | 'ReadS3Data' >> ReadFromBigQuery(
                table=options.s3_table,
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
        
        # Read native data
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
        
        # Join and compare
        comparison_results = (
            {'s3': s3_data, 'native': native_data}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'CompareRecords' >> beam.ParDo(
                RecordComparator(key_columns, comparison_columns)
            )
        )
        
        # Calculate statistics
        stats = (
            comparison_results
            | 'ExtractStatus' >> beam.Map(lambda x: (x['status'], 1))
            | 'CountByStatus' >> beam.CombinePerKey(sum)
            | 'FormatStats' >> beam.Map(lambda x: {
                'status': x[0],
                'count': x[1],
                'timestamp': datetime.utcnow().isoformat()
            })
        )
        
        # Write detailed results
        (comparison_results
         | 'WriteDetailedResults' >> WriteToBigQuery(
             table=options.output_table,
             schema='SCHEMA_AUTODETECT',
             create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_TRUNCATE
         ))
        
        # Write statistics
        (stats
         | 'WriteStats' >> WriteToBigQuery(
             table=options.output_table.replace('results', 'stats'),
             schema='SCHEMA_AUTODETECT',
             create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))

def main():
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    options = PipelineOptions(pipeline_args)
    recon_options = options.view_as(ReconciliationOptions)
    
    run_reconciliation(recon_options)

if __name__ == '__main__':
    main()
