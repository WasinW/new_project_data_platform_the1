# airflow/dags/reconciliation_pipeline.py
"""
Reconciliation Pipeline - Following Context Detail Requirements
✅ Uses Dataflow with reconciliation_pipeline.py for processing  
✅ Compares BigQuery data with S3 source data
✅ Uses Storage Transfer Service for S3 access when needed
✅ Follows the original design: Airflow orchestration, Dataflow processing
"""
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceRunJobOperator
)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

def create_reconciliation_dag(domain: str):
    """Create reconciliation pipeline DAG following context detail requirements"""
    
    dag = DAG(
        f'reconciliation_{domain}_pipeline',
        default_args=default_args,
        description=f'Daily reconciliation pipeline for {domain} domain (STS compliant)',
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        tags=['reconciliation', domain, 'daily', 'sts-compliant']
    )
    
    def prepare_reconciliation_config(**context):
        """Prepare reconciliation configuration for Dataflow"""
        execution_date = context['execution_date']
        config = {
            'mode': 'reconciliation',
            'domain': domain,
            'config_path': f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/{domain}/config.yaml',
            'reconciliation_date': execution_date.strftime('%Y-%m-%d'),
            's3_bucket': Variable.get(f'{domain}_s3_source_bucket'),
            'gcs_bucket': f'{Variable.get("gcp_project_id")}-reconciliation-data',
            'bigquery_dataset': f'{Variable.get("gcp_project_id")}.{domain}_reconciliation'
        }
        return config
    
    # Check if daily batch processing completed successfully
    check_daily_batch_completion = BigQueryCheckOperator(
        task_id='check_daily_batch_completion',
        sql=f"""
            SELECT COUNT(*) as completed_batches
            FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_batch_jobs`
            WHERE DATE(completion_time) = DATE('{{{{ ds }}}}')
              AND job_status = 'SUCCESS'
            HAVING COUNT(*) >= 24  -- All 24 hourly batches completed
        """,
        dag=dag
    )
    
    # Prepare reconciliation configuration
    prepare_config = PythonOperator(
        task_id='prepare_reconciliation_config',
        python_callable=prepare_reconciliation_config,
        dag=dag
    )
    
    # Create STS job to copy S3 data for reconciliation
    create_sts_reconciliation_job = CloudDataTransferServiceCreateJobOperator(
        task_id='create_sts_reconciliation_job',
        body={
            'description': f'Daily reconciliation data transfer for {domain} - {{{{ ds }}}}',
            'status': 'ENABLED',
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': '{{ var.value.' + f'{domain}_s3_source_bucket' + ' }}',
                    'path': f'{domain}/data/{{{{ ds }}}}/',
                    'awsAccessKey': {
                        'accessKeyId': '{{ var.value.aws_access_key_id }}',
                        'secretAccessKey': '{{ var.value.aws_secret_access_key }}'
                    }
                },
                'gcsDataSink': {
                    'bucketName': '{{ var.value.gcp_project_id }}-reconciliation-data',
                    'path': f'{domain}/reconciliation/{{{{ ds }}}}/'
                },
                'transferOptions': {
                    'overwriteObjectsAlreadyExistingInSink': True,
                    'deleteObjectsUniqueInSink': False
                }
            },
            'schedule': {
                'scheduleStartDate': {
                    'year': int('{{ ds[:4] }}'),
                    'month': int('{{ ds[5:7] }}'),
                    'day': int('{{ ds[8:10] }}')
                },
                'scheduleEndDate': {
                    'year': int('{{ ds[:4] }}'),
                    'month': int('{{ ds[5:7] }}'),
                    'day': int('{{ ds[8:10] }}')
                }
            }
        },
        dag=dag
    )
    
    # Run the STS job
    run_sts_reconciliation_job = CloudDataTransferServiceRunJobOperator(
        task_id='run_sts_reconciliation_job',
        job_name='{{ task_instance.xcom_pull(task_ids="create_sts_reconciliation_job")["name"] }}',
        wait=True,
        timeout=3600,  # 1 hour timeout
        dag=dag
    )
    
    # Run reconciliation Dataflow job
    run_reconciliation_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_reconciliation_dataflow',
        py_file=f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/dataflow/reconciliation_pipeline.py',
        job_name=f'reconciliation-{domain}-{{{{ ds_nodash }}}}',
        dataflow_config={
            'project_id': '{{ var.value.gcp_project_id }}',
            'region': '{{ var.value.gcp_region }}',
            'service_account_email': '{{ var.value.dataflow_service_account }}',
            'network': '{{ var.value.dataflow_network }}',
            'subnetwork': '{{ var.value.dataflow_subnetwork }}',
            
            # STS Compliant parameters
            'mode': 'reconciliation',
            'domain': domain,
            'config_path': f'gs://{Variable.get("gcp_project_id")}-pipeline-configs/{domain}/config.yaml',
            'reconciliation_date': '{{ ds }}',
            
            # Data sources
            's3_data_path': f'gs://{Variable.get("gcp_project_id")}-reconciliation-data/{domain}/reconciliation/{{{{ ds }}}}',
            'bigquery_dataset': f'{Variable.get("gcp_project_id")}.{domain}_data',
            
            # Pipeline configuration
            'temp_location': f'gs://{Variable.get("gcp_project_id")}-dataflow-temp/{domain}/reconciliation/{{{{ ds }}}}',
            'staging_location': f'gs://{Variable.get("gcp_project_id")}-dataflow-staging/{domain}/reconciliation/{{{{ ds }}}}',
            
            # Dataflow runner settings
            'runner': 'DataflowRunner',
            'setup_file': './setup.py',
            
            # Reconciliation optimizations
            'use_public_ips': False,
            'max_num_workers': 15,
            'num_workers': 3,
            'machine_type': 'n2-standard-8',
            'disk_size_gb': 200,
        },
        dag=dag
    )
    
    # Analyze reconciliation results
    analyze_reconciliation_results = BigQueryInsertJobOperator(
        task_id='analyze_reconciliation_results',
        configuration={
            'query': {
                'query': f"""
                    WITH reconciliation_summary AS (
                        SELECT 
                            '{domain}' as domain,
                            '{{{{ ds }}}}' as reconciliation_date,
                            COUNT(*) as total_comparisons,
                            COUNTIF(status = 'MATCH') as matched_records,
                            COUNTIF(status = 'MISMATCH') as mismatched_records,
                            COUNTIF(status = 'MISSING_S3') as missing_in_s3,
                            COUNTIF(status = 'MISSING_BQ') as missing_in_bigquery,
                            COUNT(DISTINCT table_name) as tables_reconciled
                        FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_reconciliation.reconciliation_results`
                        WHERE reconciliation_date = '{{{{ ds }}}}'
                    ),
                    data_quality_metrics AS (
                        SELECT
                            AVG(CASE WHEN status = 'MATCH' THEN 1.0 ELSE 0.0 END) * 100 as match_percentage,
                            AVG(CASE WHEN status = 'MISMATCH' THEN 1.0 ELSE 0.0 END) * 100 as mismatch_percentage,
                            COUNT(DISTINCT error_type) as unique_error_types
                        FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_reconciliation.reconciliation_results`
                        WHERE reconciliation_date = '{{{{ ds }}}}'
                          AND status != 'MATCH'
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_reconciliation_summary`
                    SELECT 
                        r.*,
                        q.match_percentage,
                        q.mismatch_percentage,
                        q.unique_error_types,
                        CASE 
                            WHEN q.match_percentage >= 95.0 THEN 'EXCELLENT'
                            WHEN q.match_percentage >= 90.0 THEN 'GOOD'
                            WHEN q.match_percentage >= 80.0 THEN 'FAIR'
                            ELSE 'POOR'
                        END as data_quality_grade,
                        CURRENT_TIMESTAMP() as analysis_timestamp
                    FROM reconciliation_summary r
                    CROSS JOIN data_quality_metrics q
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Generate reconciliation alerts for critical issues
    generate_reconciliation_alerts = BigQueryInsertJobOperator(
        task_id='generate_reconciliation_alerts',
        configuration={
            'query': {
                'query': f"""
                    WITH critical_issues AS (
                        SELECT 
                            '{domain}' as domain,
                            '{{{{ ds }}}}' as alert_date,
                            'RECONCILIATION_FAILURE' as alert_type,
                            CASE 
                                WHEN match_percentage < 80.0 THEN 'CRITICAL'
                                WHEN match_percentage < 90.0 THEN 'WARNING'
                                ELSE 'INFO'
                            END as severity,
                            CONCAT('Data quality for {domain} on {{{{ ds }}}} is ', 
                                   data_quality_grade, ' (', 
                                   ROUND(match_percentage, 2), '% match rate)') as message,
                            matched_records,
                            mismatched_records,
                            missing_in_s3,
                            missing_in_bigquery
                        FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_reconciliation_summary`
                        WHERE reconciliation_date = '{{{{ ds }}}}'
                          AND data_quality_grade IN ('FAIR', 'POOR')
                    )
                    INSERT INTO `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_pipeline_alerts`
                    SELECT 
                        *,
                        CURRENT_TIMESTAMP() as alert_timestamp,
                        'reconciliation' as pipeline_type
                    FROM critical_issues
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Clean up old reconciliation data (keep last 7 days)
    cleanup_old_reconciliation_data = BigQueryInsertJobOperator(
        task_id='cleanup_old_reconciliation_data',
        configuration={
            'query': {
                'query': f"""
                    DELETE FROM `{{{{ var.value.gcp_project_id }}}}.{domain}_reconciliation.reconciliation_results`
                    WHERE reconciliation_date < DATE_SUB('{{{{ ds }}}}', INTERVAL 7 DAY);
                    
                    DELETE FROM `{{{{ var.value.gcp_project_id }}}}.monitoring_data.{domain}_reconciliation_summary`
                    WHERE reconciliation_date < DATE_SUB('{{{{ ds }}}}', INTERVAL 30 DAY);
                """,
                'useLegacySql': False
            }
        },
        dag=dag
    )
    
    # Chain tasks
    (check_daily_batch_completion >> prepare_config 
     >> create_sts_reconciliation_job >> run_sts_reconciliation_job 
     >> run_reconciliation_dataflow >> analyze_reconciliation_results 
     >> [generate_reconciliation_alerts, cleanup_old_reconciliation_data])
    
    return dag


# Create DAGs for each domain
domains_config = json.loads(Variable.get('reconciliation_domains', '["the1", "member", "order"]'))
for domain in domains_config:
    globals()[f'reconciliation_{domain}_dag'] = create_reconciliation_dag(domain)
