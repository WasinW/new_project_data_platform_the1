# airflow/dags/realtime_trigger.py
"""
Realtime Trigger - Native Operators Solution
✅ Uses BigQueryInsertJobOperator for dependency checks
✅ Uses DataflowCreatePythonJobOperator with hybrid_pipeline.py
✅ No manual client creation
"""
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 0,  # No retries for realtime
    'retry_delay': timedelta(minutes=1)
}

def create_realtime_dag(domain: str):
    """Create realtime pipeline DAG using native operators"""
    
    dag = DAG(
        f'realtime_{domain}_pipeline',
        default_args=default_args,
        description=f'Realtime streaming pipeline for {domain} domain (Native Operators)',
        schedule_interval=None,  # Always running
        catchup=False,
        max_active_runs=1,
        tags=['realtime', domain, 'streaming', 'native-operators']
    )
    
    # ✅ Check dependencies using BigQuery Native Operator (no client!)
    check_batch_dependencies = BigQueryInsertJobOperator(
        task_id='check_batch_dependencies',
        configuration={
            'query': {
                'query': f"""
                    WITH dependency_check AS (
                        SELECT 
                            COUNT(*) as completed_jobs,
                            MAX(completion_time) as latest_completion,
                            CASE 
                                WHEN COUNT(*) > 0 THEN TRUE 
                                ELSE FALSE 
                            END as dependencies_ready
                        FROM `{{{{ var.value.gcp_project_id }}}}.batch_control.job_status`
                        WHERE job_name = 'daily_etl'
                        AND status = 'COMPLETED'
                        AND DATE(completion_time) = CURRENT_DATE()
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.{domain}_monitoring.dependency_checks`
                    SELECT 
                        *,
                        '{domain}' as domain,
                        'realtime_pipeline' as pipeline_type,
                        CURRENT_TIMESTAMP() as check_timestamp
                    FROM dependency_check
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    def prepare_realtime_config(**context):
        """Prepare realtime pipeline configuration"""
        config = {
            'mode': 'realtime',
            'domain': domain,
            'enable_windowing': True,
            'pubsub_subscription': f'projects/{{{{ var.value.gcp_project_id }}}}/subscriptions/{domain}-events-sub',
            'output_table': f'{{{{ var.value.gcp_project_id }}}}.{domain}_raw.realtime_events',
            'error_table': f'{{{{ var.value.gcp_project_id }}}}.{domain}_errors.processing_errors',
            'temp_location': f'gs://dataflow-temp/{domain}/realtime',
            'staging_location': f'gs://dataflow-staging/{domain}/realtime',
            'window_duration_seconds': 300,  # 5 minutes
            'allowed_lateness_seconds': 60
        }
        return config
    
    # Prepare realtime configuration
    prepare_config = PythonOperator(
        task_id='prepare_realtime_config',
        python_callable=prepare_realtime_config,
        dag=dag
    )
    
    # ✅ Start realtime Dataflow job using hybrid_pipeline.py with Native I/O
    start_realtime_dataflow = DataflowCreatePythonJobOperator(
        task_id='start_realtime_dataflow',
        py_file='gs://dataflow-templates/hybrid_pipeline.py',  # ✅ Use current version with Native I/O
        job_name=f"realtime-{domain}-{{{{ ts_nodash }}}}",
        options={
            # ✅ Mode parameter - realtime processing
            'mode': 'realtime',
            'domain': domain,
            'enable_windowing': True,
            
            # Pipeline configuration
            'config_path': f'gs://pipeline-configs/{domain}/config.yaml',
            'temp_location': f'gs://dataflow-temp/{domain}/realtime',
            'staging_location': f'gs://dataflow-staging/{domain}/realtime',
            
            # Dataflow runner settings
            'runner': 'DataflowRunner',
            'project': '{{ var.value.gcp_project_id }}',
            'region': '{{ var.value.gcp_region }}',
            'setup_file': './setup.py',
            
            # ✅ Realtime specific settings with Native I/O
            'streaming': True,
            'enable_streaming_engine': True,  # Use Streaming Engine
            'use_storage_write_api': True,  # Fast BigQuery writes
            'dataflow_kms_key': '{{ var.value.kms_key_name }}',
            
            # Window configuration
            'window_duration_seconds': 300,
            'allowed_lateness_seconds': 60,
            'trigger_frequency_seconds': 60,
        },
        dataflow_config={
            'job_name': f'realtime-{domain}',
            'num_workers': 2,
            'max_num_workers': 10,
            'machine_type': 'n2-standard-2',
            'disk_size_gb': 30,
            'service_account_email': '{{ var.value.dataflow_service_account }}',
            'network': '{{ var.value.dataflow_network }}',
            'subnetwork': '{{ var.value.dataflow_subnetwork }}',
            
            # ✅ Optimizations for streaming
            'experiments': [
                'enable_streaming_engine',
                'use_portable_job_submission'
            ]
        },
        dag=dag
    )
    
    # ✅ Monitor realtime pipeline health using BigQuery (no monitoring client!)
    monitor_pipeline_health = BigQueryInsertJobOperator(
        task_id='monitor_pipeline_health',
        configuration={
            'query': {
                'query': f"""
                    WITH health_metrics AS (
                        SELECT 
                            COUNT(*) as events_processed_last_hour,
                            COUNT(DISTINCT member_id) as unique_members_last_hour,
                            AVG(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(_processing_timestamp), SECOND)) as avg_processing_delay_seconds,
                            COUNT(CASE WHEN _processing_timestamp IS NULL THEN 1 END) as null_timestamp_count,
                            MAX(_processing_timestamp) as latest_processed_event
                        FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_raw.realtime_events`
                        WHERE _processing_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                    ),
                    
                    error_metrics AS (
                        SELECT 
                            COUNT(*) as errors_last_hour,
                            COUNT(DISTINCT error) as unique_error_types
                        FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_errors.processing_errors`
                        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                    )
                    
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.{domain}_monitoring.realtime_health`
                    SELECT 
                        h.*,
                        e.errors_last_hour,
                        e.unique_error_types,
                        CASE 
                            WHEN h.events_processed_last_hour = 0 THEN 'CRITICAL'
                            WHEN h.avg_processing_delay_seconds > 300 THEN 'WARNING'
                            WHEN e.errors_last_hour > h.events_processed_last_hour * 0.05 THEN 'WARNING'
                            ELSE 'HEALTHY'
                        END as health_status,
                        '{domain}' as domain,
                        'realtime_pipeline' as pipeline_type,
                        CURRENT_TIMESTAMP() as check_timestamp
                    FROM health_metrics h
                    CROSS JOIN error_metrics e
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # ✅ Create alerts based on health metrics using BigQuery
    create_health_alerts = BigQueryInsertJobOperator(
        task_id='create_health_alerts',
        configuration={
            'query': {
                'query': f"""
                    WITH alert_conditions AS (
                        SELECT 
                            *,
                            CASE 
                                WHEN health_status = 'CRITICAL' THEN 'Pipeline stopped processing events'
                                WHEN health_status = 'WARNING' AND avg_processing_delay_seconds > 300 THEN 'High processing delay detected'
                                WHEN health_status = 'WARNING' AND errors_last_hour > events_processed_last_hour * 0.05 THEN 'High error rate detected'
                                ELSE NULL
                            END as alert_message
                        FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_monitoring.realtime_health`
                        WHERE check_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
                        AND health_status IN ('CRITICAL', 'WARNING')
                    )
                    
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.{domain}_monitoring.pipeline_alerts`
                    SELECT 
                        domain,
                        pipeline_type,
                        health_status as alert_severity,
                        alert_message,
                        check_timestamp as alert_timestamp,
                        events_processed_last_hour,
                        errors_last_hour,
                        avg_processing_delay_seconds
                    FROM alert_conditions
                    WHERE alert_message IS NOT NULL
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Chain tasks
    check_batch_dependencies >> prepare_config >> start_realtime_dataflow >> monitor_pipeline_health >> create_health_alerts
    
    return dag


# ✅ Create DAGs for each domain using native operators
domains_config = json.loads(Variable.get('realtime_domains', '["member", "order", "product"]'))
for domain in domains_config:
    globals()[f'realtime_{domain}_dag'] = create_realtime_dag(domain)
