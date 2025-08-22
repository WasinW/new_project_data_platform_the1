# airflow/dags/batch_pipeline_v2.py
"""
Batch Pipeline V2 - Native Operators Solution
✅ Uses DataflowCreatePythonJobOperator with hybrid_pipeline_v2.py
✅ No client creation - all done through Native I/O
✅ Reuses realtime code with mode=batch parameter
"""
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'data-platform',
    'depends_on_past': True,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def create_batch_dag_v2(domain: str):
    """Create batch pipeline DAG V2 using native operators and hybrid_pipeline_v2.py"""
    
    dag = DAG(
        f'batch_{domain}_pipeline_v2',
        default_args=default_args,
        description=f'Hourly batch pipeline for {domain} domain (V2 - Native I/O)',
        schedule_interval='@hourly',
        catchup=False,
        tags=['batch', domain, 'v2', 'native-io']
    )
    
    def prepare_batch_config(**context):
        """Prepare batch configuration for pipeline"""
        config = {
            'domain': domain,
            'mode': 'batch',
            'batch_window_hours': 1,
            'processing_timestamp': context['execution_date'].isoformat(),
            'source_table': f"{Variable.get('gcp_project_id')}.{domain}_staging.batch_input",
            'output_table': f"{Variable.get('gcp_project_id')}.{domain}_raw.batch_processed",
            'temp_location': f"gs://dataflow-temp/{domain}/batch/{{ ds }}",
            'staging_location': f"gs://dataflow-staging/{domain}/batch/{{ ds }}"
        }
        return config
    
    # Prepare configuration
    prepare_config = PythonOperator(
        task_id='prepare_batch_config',
        python_callable=prepare_batch_config,
        dag=dag
    )
    
    # ✅ Run batch processing using hybrid_pipeline_v2.py with Native I/O
    run_batch_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_batch_dataflow_v2',
        py_file='gs://dataflow-templates/hybrid_pipeline_v2.py',  # ✅ Use V2 with Native I/O
        job_name=f"batch-{domain}-v2-{{{{ ds_nodash }}}}",
        options={
            # ✅ Mode parameter - same code, different behavior
            'mode': 'batch',
            'domain': domain,
            'batch_window_hours': 1,
            'enable_windowing': True,
            
            # Pipeline configuration
            'config_path': f'gs://pipeline-configs/{domain}/config.yaml',
            'temp_location': f'gs://dataflow-temp/{domain}/batch/{{{{ ds }}}}',
            'staging_location': f'gs://dataflow-staging/{domain}/batch/{{{{ ds }}}}',
            
            # Dataflow runner settings
            'runner': 'DataflowRunner',
            'project': '{{ var.value.gcp_project_id }}',
            'region': '{{ var.value.gcp_region }}',
            'setup_file': './setup.py',
            
            # ✅ Native I/O specific settings
            'use_storage_write_api': True,  # Enable fast BigQuery writes
            'dataflow_kms_key': '{{ var.value.kms_key_name }}',
            'enable_streaming_engine': False,  # Batch mode
        },
        dataflow_config={
            'job_name': f'batch-{domain}-v2',
            'num_workers': 3,
            'max_num_workers': 10,
            'machine_type': 'n2-standard-4',
            'disk_size_gb': 50,
            'service_account_email': '{{ var.value.dataflow_service_account }}',
            'network': '{{ var.value.dataflow_network }}',
            'subnetwork': '{{ var.value.dataflow_subnetwork }}',
            
            # ✅ Optimizations for Native I/O
            'experiments': [
                'use_runner_v2',  # Use Dataflow Runner V2
                'use_portable_job_submission'  # Better job submission
            ]
        },
        dag=dag
    )
    
    # ✅ Validate results using BigQuery Native Operator (no client!)
    validate_batch_results = BigQueryInsertJobOperator(
        task_id='validate_batch_results',
        configuration={
            'query': {
                'query': f"""
                    SELECT 
                        'batch_validation' as validation_type,
                        '{domain}' as domain,
                        COUNT(*) as processed_records,
                        MIN(_processing_timestamp) as earliest_processed,
                        MAX(_processing_timestamp) as latest_processed,
                        CURRENT_TIMESTAMP() as validation_timestamp,
                        '{{{{ ds }}}}' as processing_date
                    FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_raw.batch_processed`
                    WHERE DATE(_processing_timestamp) = '{{{{ ds }}}}'
                """,
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': '{{ var.value.gcp_project_id }}',
                    'datasetId': f'{domain}_monitoring',
                    'tableId': 'batch_validation_results'
                },
                'writeDisposition': 'WRITE_APPEND',
                'createDisposition': 'CREATE_IF_NEEDED'
            }
        },
        dag=dag
    )
    
    # ✅ Check data quality using BigQuery SQL (no client needed!)
    check_data_quality = BigQueryInsertJobOperator(
        task_id='check_data_quality',
        configuration={
            'query': {
                'query': f"""
                    WITH quality_checks AS (
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT member_id) as unique_members,
                            COUNT(CASE WHEN member_id IS NULL THEN 1 END) as null_member_ids,
                            COUNT(CASE WHEN _processing_timestamp IS NULL THEN 1 END) as null_timestamps,
                            AVG(CASE WHEN amount IS NOT NULL THEN amount END) as avg_amount
                        FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_raw.batch_processed`
                        WHERE DATE(_processing_timestamp) = '{{{{ ds }}}}'
                    )
                    SELECT 
                        *,
                        CASE 
                            WHEN null_member_ids > total_records * 0.05 THEN 'FAIL'
                            WHEN null_timestamps > 0 THEN 'FAIL'
                            WHEN total_records = 0 THEN 'FAIL'
                            ELSE 'PASS'
                        END as quality_status,
                        '{{{{ ds }}}}' as check_date,
                        CURRENT_TIMESTAMP() as check_timestamp
                    FROM quality_checks
                """,
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': '{{ var.value.gcp_project_id }}',
                    'datasetId': f'{domain}_monitoring',
                    'tableId': 'data_quality_results'
                },
                'writeDisposition': 'WRITE_APPEND',
                'createDisposition': 'CREATE_IF_NEEDED'
            }
        },
        dag=dag
    )
    
    # Chain tasks
    prepare_config >> run_batch_dataflow >> validate_batch_results >> check_data_quality
    
    return dag


# ✅ Create DAGs for each domain using V2 approach
domains_config = json.loads(Variable.get('batch_domains_v2', '["member", "order", "product"]'))
for domain in domains_config:
    globals()[f'batch_{domain}_v2_dag'] = create_batch_dag_v2(domain)
