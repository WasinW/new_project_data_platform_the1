# airflow/dags/batch_pipeline_sts.py
"""
Batch Pipeline - Following Context Detail Requirements
✅ Uses Dataflow with hybrid_pipeline_sts.py for processing
✅ Composer triggers Dataflow with proper parameters
✅ Dataflow handles data transformation, windowing, secrets
✅ Follows the original design: Airflow orchestration, Dataflow processing
"""
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'data-platform',
    'depends_on_past': True,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

def create_batch_dag(domain: str):
    """Create batch pipeline DAG following context detail requirements"""
    
    dag = DAG(
        f'batch_{domain}_pipeline',
        default_args=default_args,
        description=f'Hourly batch pipeline for {domain} domain (STS compliant)',
        schedule_interval='@hourly',
        catchup=False,
        max_active_runs=1,
        tags=['batch', domain, 'hourly', 'sts-compliant']
    )
    
    def prepare_batch_config(**context):
        """Prepare batch configuration for Dataflow"""
        execution_date = context['execution_date']
        config = {
            'mode': 'batch',
            'domain': domain,
            'enable_windowing': True,
            'config_path': f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/{domain}/config.yaml',
            'batch_window_start': execution_date.isoformat(),
            'batch_window_end': (execution_date + timedelta(hours=1)).isoformat(),
            'processing_timestamp': context['ts']
        }
        return config
    
    # Check data availability for this hour
    check_data_availability = BigQueryCheckOperator(
        task_id='check_data_availability',
        sql=f"""
            SELECT COUNT(*) as record_count
            FROM `{{{{ var.value.gcp_project_id }}}}.staging_data.{domain}_batch_input`
            WHERE TIMESTAMP(created_at) >= TIMESTAMP('{{{{ ds }}}} {{{{ execution_date.hour }}}}:00:00')
              AND TIMESTAMP(created_at) < TIMESTAMP_ADD(TIMESTAMP('{{{{ ds }}}} {{{{ execution_date.hour }}}}:00:00'), INTERVAL 1 HOUR)
        """,
        dag=dag
    )
    
    # Prepare batch configuration
    prepare_config = PythonOperator(
        task_id='prepare_batch_config',
        python_callable=prepare_batch_config,
        dag=dag
    )
    
    # Check upstream dependencies
    check_upstream_dependencies = BigQueryCheckOperator(
        task_id='check_upstream_dependencies',
        sql=f"""
            WITH dependency_status AS (
                SELECT 
                    COUNT(*) as completed_dependencies,
                    COUNTIF(status = 'SUCCESS') as successful_dependencies
                FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_upstream_jobs`
                WHERE DATE(completion_time) = CURRENT_DATE()
                  AND EXTRACT(HOUR FROM completion_time) <= {{{{ execution_date.hour }}}}
            )
            SELECT 
                CASE 
                    WHEN completed_dependencies > 0 AND successful_dependencies = completed_dependencies 
                    THEN 1 
                    ELSE 0 
                END as dependencies_ready
            FROM dependency_status
        """,
        dag=dag
    )
    
    # Main Dataflow batch job using hybrid_pipeline_sts.py
    run_batch_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_batch_dataflow',
        py_file=f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/dataflow/hybrid_pipeline_sts.py',
        job_name=f'batch-{domain}-{{{{ ds_nodash }}}}-{{{{ execution_date.hour }}}}',
        dataflow_config={
            'project_id': '{{ var.value.gcp_project_id }}',
            'region': '{{ var.value.gcp_region }}',
            'service_account_email': '{{ var.value.dataflow_service_account }}',
            'network': '{{ var.value.dataflow_network }}',
            'subnetwork': '{{ var.value.dataflow_subnetwork }}',
            
            # STS Compliant parameters
            'mode': 'batch',
            'domain': domain,
            'config_path': f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/{domain}/config.yaml',
            'enable_windowing': 'true',
            
            # Batch processing window
            'batch_window_start': '{{ execution_date.isoformat() }}',
            'batch_window_end': '{{ next_execution_date.isoformat() }}',
            
            # Pipeline configuration
            'temp_location': f'gs://{Variable.get("gcp_project_id")}-dataflow-temp/{domain}/batch/{{{{ ds }}}}/{{{{ execution_date.hour }}}}',
            'staging_location': f'gs://{Variable.get("gcp_project_id")}-dataflow-staging/{domain}/batch/{{{{ ds }}}}/{{{{ execution_date.hour }}}}',
            
            # Dataflow runner settings
            'runner': 'DataflowRunner',
            'setup_file': './setup.py',
            
            # Batch optimizations  
            'use_public_ips': False,
            'max_num_workers': 20,
            'num_workers': 5,
            'machine_type': 'n2-standard-4',
            'disk_size_gb': 100,
        },
        dag=dag
    )
    
    # Validate batch results
    validate_batch_results = BigQueryInsertJobOperator(
        task_id='validate_batch_results',
        configuration={
            'query': {
                'query': f"""
                    WITH batch_validation AS (
                        SELECT 
                            COUNT(*) as processed_records,
                            COUNT(DISTINCT _target_table) as target_tables,
                            COUNT(DISTINCT _element_id) as unique_elements,
                            MIN(_processing_timestamp) as earliest_processed,
                            MAX(_processing_timestamp) as latest_processed,
                            COUNTIF(_target_table IS NULL) as null_target_tables,
                            COUNTIF(_element_id IS NULL) as null_element_ids
                        FROM `{{{{ var.value.gcp_project_id }}}}.raw_data.{domain}_batch_processed`
                        WHERE DATE(_processing_timestamp) = CURRENT_DATE()
                          AND EXTRACT(HOUR FROM TIMESTAMP(_processing_timestamp)) = {{{{ execution_date.hour }}}}
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_batch_validation`
                    SELECT 
                        *,
                        CASE 
                            WHEN processed_records = 0 THEN 'NO_DATA'
                            WHEN null_target_tables > 0 OR null_element_ids > 0 THEN 'DATA_QUALITY_ISSUES'
                            WHEN target_tables < 2 THEN 'INSUFFICIENT_TARGETS'
                            ELSE 'SUCCESS'
                        END as validation_status,
                        CURRENT_TIMESTAMP() as validation_timestamp,
                        '{domain}' as domain,
                        {{{{ execution_date.hour }}}} as batch_hour
                    FROM batch_validation
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Update batch completion status
    update_completion_status = BigQueryInsertJobOperator(
        task_id='update_completion_status',
        configuration={
            'query': {
                'query': f"""
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_batch_jobs` 
                    (domain, batch_date, batch_hour, job_status, completion_time, processed_records)
                    SELECT 
                        '{domain}' as domain,
                        CURRENT_DATE() as batch_date,
                        {{{{ execution_date.hour }}}} as batch_hour,
                        v.validation_status as job_status,
                        CURRENT_TIMESTAMP() as completion_time,
                        v.processed_records
                    FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_batch_validation` v
                    WHERE v.batch_hour = {{{{ execution_date.hour }}}}
                      AND DATE(v.validation_timestamp) = CURRENT_DATE()
                    ORDER BY v.validation_timestamp DESC
                    LIMIT 1
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Generate metrics for monitoring
    generate_batch_metrics = BigQueryInsertJobOperator(
        task_id='generate_batch_metrics',
        configuration={
            'query': {
                'query': f"""
                    WITH hourly_metrics AS (
                        SELECT 
                            '{domain}' as domain,
                            CURRENT_DATE() as metric_date,
                            {{{{ execution_date.hour }}}} as metric_hour,
                            COUNT(*) as total_records,
                            COUNT(DISTINCT _target_table) as tables_written,
                            AVG(TIMESTAMP_DIFF(TIMESTAMP(_processing_timestamp), TIMESTAMP(_source_timestamp), SECOND)) as avg_processing_latency_seconds,
                            COUNTIF(_target_table LIKE '%_raw_%') as raw_table_records,
                            COUNTIF(_target_table LIKE '%_refined_%') as refined_table_records
                        FROM `{{{{ var.value.gcp_project_id }}}}.raw_data.{domain}_batch_processed`
                        WHERE DATE(_processing_timestamp) = CURRENT_DATE()
                          AND EXTRACT(HOUR FROM TIMESTAMP(_processing_timestamp)) = {{{{ execution_date.hour }}}}
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_batch_metrics`
                    SELECT *, CURRENT_TIMESTAMP() as generated_at
                    FROM hourly_metrics
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Chain tasks
    (check_data_availability >> prepare_config >> check_upstream_dependencies 
     >> run_batch_dataflow >> validate_batch_results 
     >> [update_completion_status, generate_batch_metrics])
    
    return dag


# Create DAGs for each domain
domains_config = json.loads(Variable.get('batch_domains', '["the1", "member", "order"]'))
for domain in domains_config:
    globals()[f'batch_{domain}_dag'] = create_batch_dag(domain)
