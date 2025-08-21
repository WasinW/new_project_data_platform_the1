# airflow/dags/reconciliation_pipeline.py
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.storage_transfer import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceRunJobOperator
)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import yaml
import sys
import os

# Add dataflow utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'dataflow', 'utils'))

try:
    from secret_manager import SecretManagerClient, get_secrets_from_config
except ImportError as e:
    print(f"Warning: Could not import secret_manager: {e}")
    def get_secrets_from_config(config, project_id):
        return {}

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def create_reconciliation_dag(domain: str, tables: list):
    """Create reconciliation pipeline DAG for a specific domain"""
    
    dag = DAG(
        f'reconciliation_{domain}_pipeline',
        default_args=default_args,
        description=f'Daily reconciliation pipeline for {domain} domain',
        schedule_interval='@daily',
        catchup=False,
        tags=['reconciliation', domain, 'validation']
    )
    
    def get_reconciliation_secrets(**context):
        """Retrieve secrets for reconciliation from Secret Manager"""
        domain = context['params']['domain']
        
        try:
            # Load pipeline configuration
            config_path = Variable.get(f'{domain}_config_path', 'config/pipeline_config.yaml')
            
            if config_path.startswith('gs://'):
                # Load from GCS
                from google.cloud import storage
                storage_client = storage.Client()
                bucket_name = config_path.split('/')[2]
                blob_path = '/'.join(config_path.split('/')[3:])
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(blob_path)
                config_content = blob.download_as_text()
                config = yaml.safe_load(config_content)
            else:
                # Load local file
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
            
            project_id = Variable.get('gcp_project_id')
            
            # Get secrets from Secret Manager
            secrets = get_secrets_from_config(config, project_id)
            
            # Store secrets in XCom for other tasks
            context['task_instance'].xcom_push(key='reconciliation_secrets', value=secrets)
            
            print(f"Retrieved reconciliation secrets for domain {domain}")
            return secrets
            
        except Exception as e:
            print(f"Error retrieving reconciliation secrets: {e}")
            # Return empty dict to allow pipeline to continue with defaults
            return {}
    
    def prepare_s3_copy(**context):
        """Prepare S3 copy job for reconciliation using secrets"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        # Get secrets from XCom
        secrets = context['task_instance'].xcom_pull(key='reconciliation_secrets') or {}
        s3_creds = secrets.get('s3_credentials', {})
        
        # Use secrets or fall back to Airflow variables
        aws_access_key = s3_creds.get('aws_access_key_id', Variable.get('aws_access_key_id'))
        aws_secret_key = s3_creds.get('aws_secret_access_key', Variable.get('aws_secret_access_key'))
        s3_bucket = s3_creds.get('s3_bucket_name', Variable.get('s3_bucket'))
        
        copy_job_config = {
            'description': f'Daily reconciliation copy for {table}',
            'status': 'ENABLED',
            'projectId': Variable.get('gcp_project_id'),
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': s3_bucket,
                    'path': f'{domain}/{table}/{{ ds }}/',
                    'awsAccessKey': {
                        'accessKeyId': aws_access_key,
                        'secretAccessKey': aws_secret_key
                    }
                },
                'gcsDataSink': {
                    'bucketName': f'{domain}-reconcile-temp',
                    'path': f'{table}/{{ ds }}/'
                }
            }
        }
        
        return copy_job_config
    
    def run_reconciliation_dataflow(**context):
        """Run Dataflow job for reconciliation"""
        table = context['params']['table']
        
        job_params = {
            's3_table': f"{Variable.get('gcp_project_id')}.{domain}_reconcile_temp.{table}_external",
            'native_table': f"{Variable.get('gcp_project_id')}.{domain}_raw.{table}",
            'output_table': f"{Variable.get('gcp_project_id')}.{domain}_audit.reconciliation_results",
            'key_columns': 'id,member_id',  # Configurable per table
            'comparison_columns': 'name,email,phone,status,updated_at'  # Configurable per table
        }
        
        return job_params
    
    def analyze_reconciliation_results(**context):
        """Analyze reconciliation results and create alerts"""
        from google.cloud import bigquery
        
        table = context['params']['table']
        execution_date = context['execution_date']
        
        client = bigquery.Client()
        
        # Query reconciliation statistics
        query = f"""
            SELECT 
                status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM `{Variable.get('gcp_project_id')}.{domain}_audit.reconciliation_results`
            WHERE table_name = @table
            AND DATE(reconciliation_timestamp) = @execution_date
            GROUP BY status
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table", "STRING", table),
                bigquery.ScalarQueryParameter("execution_date", "DATE", execution_date.date())
            ]
        )
        
        results = client.query(query, job_config=job_config).result()
        
        stats = {}
        for row in results:
            stats[row.status] = {'count': row.count, 'percentage': row.percentage}
        
        # Check for alerts
        mismatch_percentage = stats.get('MISMATCH', {}).get('percentage', 0)
        missing_percentage = (
            stats.get('MISSING_IN_NATIVE', {}).get('percentage', 0) +
            stats.get('MISSING_IN_S3', {}).get('percentage', 0)
        )
        
        alerts = []
        if mismatch_percentage > 5:
            alerts.append(f"High mismatch rate: {mismatch_percentage}%")
        if missing_percentage > 10:
            alerts.append(f"High missing record rate: {missing_percentage}%")
        
        if alerts:
            # Send alert (implementation depends on notification system)
            print(f"ALERTS for {table}: {', '.join(alerts)}")
        
        return stats
    
    # Setup secrets task
    get_secrets = PythonOperator(
        task_id='get_reconciliation_secrets',
        python_callable=get_reconciliation_secrets,
        params={'domain': domain},
        dag=dag
    )
    
    # Create tasks for each table
    for table in tables:
        # Copy S3 data to temp location using secrets
        copy_s3_data = PythonOperator(
            task_id=f'copy_s3_data_{table}',
            python_callable=prepare_s3_copy,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        run_s3_copy = CloudDataTransferServiceRunJobOperator(
            task_id=f'run_s3_copy_{table}',
            job_name="{{ task_instance.xcom_pull(task_ids='copy_s3_data_" + table + "') }}",
            dag=dag
        )
        
        # Create external table for S3 data
        create_external_table = PythonOperator(
            task_id=f'create_external_table_{table}',
            python_callable=lambda **context: print(f"Creating external table for {table}"),
            params={'table': table},
            dag=dag
        )
        
        # Run reconciliation Dataflow
        run_reconciliation = DataflowTemplatedJobStartOperator(
            task_id=f'run_reconciliation_{table}',
            template='gs://dataflow-templates/reconciliation_pipeline.json',
            job_name=f"reconciliation-{domain}-{table}-{{ ds_nodash }}",
            parameters={
                's3_table': f"{Variable.get('gcp_project_id')}.{domain}_reconcile_temp.{table}_external",
                'native_table': f"{Variable.get('gcp_project_id')}.{domain}_raw.{table}",
                'output_table': f"{Variable.get('gcp_project_id')}.{domain}_audit.reconciliation_results",
                'key_columns': 'id,member_id',
                'comparison_columns': 'name,email,phone,status,updated_at'
            },
            dataflow_default_options={
                'project': Variable.get('gcp_project_id'),
                'region': Variable.get('gcp_region', 'asia-southeast1'),
                'tempLocation': f"gs://dataflow-temp/{domain}/reconciliation/{{ ds }}",
            },
            dag=dag
        )
        
        # Analyze results
        analyze_results = PythonOperator(
            task_id=f'analyze_results_{table}',
            python_callable=analyze_reconciliation_results,
            params={'table': table},
            dag=dag
        )
        
        # Set dependencies
        get_secrets >> copy_s3_data >> run_s3_copy >> create_external_table >> run_reconciliation >> analyze_results
    
    return dag

# Create DAGs for each domain
domains_config = json.loads(Variable.get('domains_config', '{}'))
for domain, config in domains_config.items():
    if config.get('enabled', False):
        globals()[f'reconciliation_{domain}_dag'] = create_reconciliation_dag(domain, config['tables'])
