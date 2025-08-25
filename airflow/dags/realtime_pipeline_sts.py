# airflow/dags/realtime_pipeline_sts.py
"""
Realtime Pipeline - Following Context Detail Requirements
✅ Uses Dataflow with hybrid_pipeline_sts.py for processing
✅ Composer triggers Dataflow with proper parameters
✅ Dataflow handles Pub/Sub consumption, windowing, secrets, transformations
✅ Follows the original design: Airflow orchestration, Dataflow processing
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
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 0,  # No retries for realtime
    'retry_delay': timedelta(minutes=1)
}

def create_realtime_dag(domain: str):
    """Create realtime pipeline DAG following context detail requirements"""
    
    dag = DAG(
        f'realtime_{domain}_pipeline',
        default_args=default_args,
        description=f'Realtime streaming pipeline for {domain} domain (STS compliant)',
        schedule_interval=None,  # Always running via trigger
        catchup=False,
        max_active_runs=1,
        tags=['realtime', domain, 'streaming', 'sts-compliant']
    )
    
    def prepare_realtime_config(**context):
        """Prepare realtime configuration for Dataflow"""
        config = {
            'mode': 'realtime',
            'domain': domain,
            'enable_windowing': True,
            'config_path': f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/{domain}/config.yaml',
            'source_project': Variable.get('gcp_project_id'),
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
    
    # Check batch dependencies using BigQuery (Native approach)
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
                                WHEN COUNT(*) > 0 AND MAX(completion_time) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
                                THEN 'READY'
                                ELSE 'NOT_READY'
                            END as status
                        FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_batch_jobs`
                        WHERE DATE(completion_time) = CURRENT_DATE()
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_dependency_checks`
                    SELECT *, CURRENT_TIMESTAMP() as check_time, '{domain}' as domain, 'realtime' as pipeline
                    FROM dependency_check
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Main Dataflow job using hybrid_pipeline_sts.py
    run_realtime_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_realtime_dataflow',
        py_file=f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/dataflow/hybrid_pipeline_sts.py',
        job_name=f'realtime-{domain}-{{{{ ds_nodash }}}}',
        dataflow_config={
            'project_id': '{{ var.value.gcp_project_id }}',
            'region': '{{ var.value.gcp_region }}',
            'service_account_email': '{{ var.value.dataflow_service_account }}',
            'network': '{{ var.value.dataflow_network }}',
            'subnetwork': '{{ var.value.dataflow_subnetwork }}',
            
            # STS Compliant parameters
            'mode': 'realtime',
            'domain': domain,
            'config_path': f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/{domain}/config.yaml',
            'enable_windowing': 'true',
            
            # Pipeline configuration
            'temp_location': f'gs://{Variable.get("gcp_project_id")}-dataflow-temp/{domain}/realtime',
            'staging_location': f'gs://{Variable.get("gcp_project_id")}-dataflow-staging/{domain}/realtime',
            
            # Dataflow runner settings
            'runner': 'DataflowRunner',
            'setup_file': './setup.py',
            
            # Streaming optimizations
            'streaming': True,
            'enable_streaming_engine': True,
            'use_public_ips': False,
            'max_num_workers': 10,
            'num_workers': 2,
            'machine_type': 'n2-standard-2',
            'disk_size_gb': 30,
        },
        dag=dag
    )
    
    # Monitor realtime health
    monitor_realtime_health = BigQueryInsertJobOperator(
        task_id='monitor_realtime_health',
        configuration={
            'query': {
                'query': f"""
                    WITH realtime_metrics AS (
                        SELECT 
                            COUNT(*) as processed_records,
                            COUNT(DISTINCT _target_table) as target_tables,
                            AVG(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(_processing_timestamp), SECOND)) as avg_latency_seconds,
                            MIN(_processing_timestamp) as earliest_processed,
                            MAX(_processing_timestamp) as latest_processed
                        FROM `{{{{ var.value.gcp_project_id }}}}.raw_data.{domain}_realtime_events`
                        WHERE TIMESTAMP(_processing_timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_realtime_health`
                    SELECT 
                        *,
                        CASE 
                            WHEN processed_records = 0 THEN 'NO_DATA'
                            WHEN avg_latency_seconds > 300 THEN 'HIGH_LATENCY'
                            ELSE 'HEALTHY'
                        END as health_status,
                        CURRENT_TIMESTAMP() as check_timestamp,
                        '{domain}' as domain
                    FROM realtime_metrics
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Alert on anomalies
    check_anomalies = BigQueryInsertJobOperator(
        task_id='check_anomalies',
        configuration={
            'query': {
                'query': f"""
                    WITH anomaly_check AS (
                        SELECT 
                            health_status,
                            processed_records,
                            avg_latency_seconds,
                            CASE 
                                WHEN health_status != 'HEALTHY' THEN 'ALERT'
                                WHEN processed_records < 100 THEN 'WARNING' 
                                ELSE 'OK'
                            END as alert_level
                        FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_realtime_health`
                        WHERE check_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
                        ORDER BY check_timestamp DESC
                        LIMIT 1
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_pipeline_alerts`
                    SELECT 
                        *,
                        CURRENT_TIMESTAMP() as alert_timestamp,
                        '{domain}' as domain,
                        'realtime' as pipeline_type
                    FROM anomaly_check
                    WHERE alert_level IN ('ALERT', 'WARNING')
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Chain tasks
    prepare_config >> check_batch_dependencies >> run_realtime_dataflow >> monitor_realtime_health >> check_anomalies
    
    return dag


# Create DAGs for each domain
domains_config = json.loads(Variable.get('realtime_domains', '["the1", "member", "order"]'))
for domain in domains_config:
    globals()[f'realtime_{domain}_dag'] = create_realtime_dag(domain)
