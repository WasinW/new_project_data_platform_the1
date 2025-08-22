# airflow/dags/reconciliation_pipeline.py
"""
Reconciliation Pipeline - Config-driven, single table processing with Dataflow
Steps: Get Secrets → Copy S3 → Create External Table → Run Dataflow → Analyze Results
"""

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.storage_transfer import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceRunJobOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
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
    from client_manager import get_secret_manager_client, get_bigquery_client
except ImportError as e:
    print(f"Warning: Could not import client_manager: {e}")
    def get_secret_manager_client():
        from google.cloud import secretmanager
        return secretmanager.SecretManagerServiceClient()
    def get_bigquery_client():
        from google.cloud import bigquery
        return bigquery.Client()

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
        """Step 1: Retrieve secrets for reconciliation from Secret Manager"""
        domain = context['params']['domain']
        table = context['params']['table']
        
        try:
            # Load pipeline configuration
            config_path = Variable.get(f'{domain}_config_path', 'config/pipeline_config.yaml')
            
            if config_path.startswith('gs://'):
                # Load from GCS using client manager
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
            
            # Get table-specific reconciliation config
            reconciliation_config = config.get('reconciliation', {})
            table_config = reconciliation_config.get('tables', {}).get(table, {})
            
            project_id = Variable.get('gcp_project_id')
            
            # Get secrets using optimized client manager
            secret_client = get_secret_manager_client()
            
            secrets = {}
            secret_mappings = table_config.get('secrets', {})
            
            for secret_type, secret_name in secret_mappings.items():
                try:
                    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
                    response = secret_client.access_secret_version(request={"name": secret_path})
                    secrets[secret_type] = response.payload.data.decode('UTF-8')
                except Exception as e:
                    print(f"Warning: Could not get secret {secret_name}: {e}")
            
            # Store table config and secrets in XCom for other tasks
            context['task_instance'].xcom_push(key='table_config', value=table_config)
            context['task_instance'].xcom_push(key='reconciliation_secrets', value=secrets)
            
            print(f"Retrieved reconciliation config and secrets for {domain}.{table}")
            return {'table_config': table_config, 'secrets': secrets}
            
        except Exception as e:
            print(f"Error retrieving reconciliation config: {e}")
            # Return minimal config to allow pipeline to continue
            return {'table_config': {}, 'secrets': {}}
    
    def copy_s3_to_temp(**context):
        """Step 2: Copy S3 data to temp GCS location using retrieved secrets"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        # Get config and secrets from XCom
        table_config = context['task_instance'].xcom_pull(key='table_config') or {}
        secrets = context['task_instance'].xcom_pull(key='reconciliation_secrets') or {}
        
        # Get S3 configuration for this table
        s3_config = table_config.get('s3', {})
        temp_storage_config = table_config.get('temp_storage', {})
        
        # Use secrets or fall back to Airflow variables
        aws_access_key = secrets.get('aws_access_key_id', Variable.get('aws_access_key_id', ''))
        aws_secret_key = secrets.get('aws_secret_access_key', Variable.get('aws_secret_access_key', ''))
        
        s3_bucket = s3_config.get('bucket_name', Variable.get('s3_bucket', ''))
        s3_path = s3_config.get('path_pattern', f'{domain}/{table}/{{{{ ds }}}}/')
        
        temp_bucket = temp_storage_config.get('bucket_name', f'{domain}-reconcile-temp')
        temp_path = temp_storage_config.get('path_pattern', f'{table}/{{{{ ds }}}}/')
        
        copy_job_config = {
            'description': f'Reconciliation S3 copy for {domain}.{table}',
            'status': 'ENABLED',
            'projectId': Variable.get('gcp_project_id'),
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': s3_bucket,
                    'path': s3_path,
                    'awsAccessKey': {
                        'accessKeyId': aws_access_key,
                        'secretAccessKey': aws_secret_key
                    }
                },
                'gcsDataSink': {
                    'bucketName': temp_bucket,
                    'path': temp_path
                }
            }
        }
        
        print(f"Prepared S3 copy job for {domain}.{table}")
        context['task_instance'].xcom_push(key='copy_job_config', value=copy_job_config)
        return copy_job_config
    
    def create_external_table(**context):
        """Step 3: Create external table pointing to S3 data in temp storage"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        # Get table config from XCom
        table_config = context['task_instance'].xcom_pull(key='table_config') or {}
        temp_storage_config = table_config.get('temp_storage', {})
        
        temp_bucket = temp_storage_config.get('bucket_name', f'{domain}-reconcile-temp')
        temp_path = temp_storage_config.get('path_pattern', f'{table}/{{{{ ds }}}}/')
        
        # Use optimized BigQuery client
        bq_client = get_bigquery_client()
        
        project_id = Variable.get('gcp_project_id')
        dataset_id = f'{domain}_reconcile_temp'
        table_id = f'{table}_external'
        
        # Create dataset if not exists
        try:
            dataset = bq_client.get_dataset(f'{project_id}.{dataset_id}')
        except Exception:
            dataset = bq_client.create_dataset(f'{project_id}.{dataset_id}')
        
        # Create external table
        external_config = {
            "sourceFormat": "PARQUET",  # or CSV based on S3 data format
            "sourceUris": [f"gs://{temp_bucket}/{temp_path}*"],
            "autodetect": True
        }
        
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.create_table(table_ref, exists_ok=True)
        table.external_data_configuration = external_config
        
        updated_table = bq_client.update_table(table, ['external_data_configuration'])
        
        print(f"Created external table {project_id}.{dataset_id}.{table_id}")
        return f'{project_id}.{dataset_id}.{table_id}'
    
    def run_reconciliation_dataflow(**context):
        """Step 4: Run Dataflow job for reconciliation with table-specific config"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        # Get table config from XCom
        table_config = context['task_instance'].xcom_pull(key='table_config') or {}
        dataflow_config = table_config.get('dataflow', {})
        comparison_config = table_config.get('comparison', {})
        
        # Get configurable parameters
        key_columns = ','.join(comparison_config.get('key_columns', ['id']))
        comparison_columns = ','.join(comparison_config.get('comparison_columns', ['name', 'email']))
        tolerance_config = json.dumps(comparison_config.get('tolerance', {}))
        
        project_id = Variable.get('gcp_project_id')
        
        job_params = {
            's3_external_table': f"{project_id}.{domain}_reconcile_temp.{table}_external",
            'native_table': f"{project_id}.{domain}_raw.{table}",
            'output_table': f"{project_id}.{domain}_audit.reconciliation_{table}_results",
            'key_columns': key_columns,
            'comparison_columns': comparison_columns,
            'tolerance_config': tolerance_config
        }
        
        # Store job params for Dataflow operator
        context['task_instance'].xcom_push(key='dataflow_params', value=job_params)
        
        print(f"Prepared Dataflow reconciliation job for {domain}.{table}")
        return job_params
    def analyze_reconciliation_results(**context):
        """Step 5: Analyze reconciliation results and create alerts"""
        table = context['params']['table']
        domain = context['params']['domain']
        execution_date = context['execution_date']
        
        # Get table config from XCom
        table_config = context['task_instance'].xcom_pull(key='table_config') or {}
        results_config = table_config.get('results', {})
        
        # Use optimized BigQuery client
        bq_client = get_bigquery_client()
        
        project_id = Variable.get('gcp_project_id')
        
        # Query reconciliation statistics
        query = f"""
            SELECT 
                status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM `{project_id}.{domain}_audit.reconciliation_{table}_results`
            WHERE DATE(reconciliation_date) = @execution_date
            GROUP BY status
        """
        
        from google.cloud import bigquery
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("execution_date", "DATE", execution_date.date())
            ]
        )
        
        results = bq_client.query(query, job_config=job_config).result()
        
        stats = {}
        for row in results:
            stats[row.status] = {'count': row.count, 'percentage': row.percentage}
        
        # Check for alerts based on table-specific thresholds
        alert_thresholds = results_config.get('alert_thresholds', {})
        mismatch_threshold = alert_thresholds.get('mismatch_percentage', 5)
        missing_threshold = alert_thresholds.get('missing_percentage', 10)
        
        mismatch_percentage = stats.get('MISMATCH', {}).get('percentage', 0)
        missing_percentage = (
            stats.get('MISSING_IN_NATIVE', {}).get('percentage', 0) +
            stats.get('MISSING_IN_S3', {}).get('percentage', 0)
        )
        
        alerts = []
        if mismatch_percentage > mismatch_threshold:
            alerts.append(f"High mismatch rate: {mismatch_percentage}% (threshold: {mismatch_threshold}%)")
        if missing_percentage > missing_threshold:
            alerts.append(f"High missing record rate: {missing_percentage}% (threshold: {missing_threshold}%)")
        
        if alerts:
            # Send alert (implementation depends on notification system)
            print(f"ALERTS for {domain}.{table}: {', '.join(alerts)}")
            
            # Store alerts in BigQuery for monitoring
            alert_table = f"{project_id}.{domain}_audit.reconciliation_alerts"
            alert_rows = [{
                'domain': domain,
                'table_name': table,
                'execution_date': execution_date.date().isoformat(),
                'alert_type': 'RECONCILIATION_THRESHOLD',
                'alert_message': alert,
                'alert_severity': 'WARNING' if 'mismatch' in alert.lower() else 'ERROR',
                'generated_at': datetime.utcnow().isoformat()
            } for alert in alerts]
            
            errors = bq_client.insert_rows_json(alert_table, alert_rows)
            if errors:
                print(f"Error inserting alerts: {errors}")
        
        print(f"Reconciliation analysis complete for {domain}.{table}: {stats}")
        return stats
    
    # Create DAG that processes one table per run based on config
    for table in tables:
        table_dag = DAG(
            f'reconciliation_{domain}_{table}',
            default_args=default_args,
            description=f'Daily reconciliation for {domain}.{table}',
            schedule_interval='@daily',
            catchup=False,
            tags=['reconciliation', domain, table, 'validation']
        )
        
        # Step 1: Get secrets
        get_secrets_task = PythonOperator(
            task_id='get_reconciliation_secrets',
            python_callable=get_reconciliation_secrets,
            params={'domain': domain, 'table': table},
            dag=table_dag
        )
        
        # Step 2: Copy S3 data to temp location
        copy_s3_task = PythonOperator(
            task_id='copy_s3_to_temp',
            python_callable=copy_s3_to_temp,
            params={'table': table, 'domain': domain},
            dag=table_dag
        )
        
        # Step 3: Create external table for S3 data
        create_external_task = PythonOperator(
            task_id='create_external_table',
            python_callable=create_external_table,
            params={'table': table, 'domain': domain},
            dag=table_dag
        )
        
        # Step 4: Run reconciliation Dataflow
        prepare_dataflow_task = PythonOperator(
            task_id='prepare_dataflow_params',
            python_callable=run_reconciliation_dataflow,
            params={'table': table, 'domain': domain},
            dag=table_dag
        )
        
        run_dataflow_task = DataflowTemplatedJobStartOperator(
            task_id='run_reconciliation_dataflow',
            template='gs://dataflow-templates/reconciliation_pipeline.json',
            job_name=f"reconciliation-{domain}-{table}-{{{{ ds_nodash }}}}",
            parameters="{{ task_instance.xcom_pull(task_ids='prepare_dataflow_params') }}",
            dataflow_default_options={
                'project': Variable.get('gcp_project_id'),
                'region': Variable.get('gcp_region', 'asia-southeast1'),
                'tempLocation': f"gs://dataflow-temp/{domain}/reconciliation/{{{{ ds }}}}",
                'subnetwork': Variable.get('dataflow_subnetwork', ''),
                'serviceAccountEmail': Variable.get('dataflow_service_account', '')
            },
            dag=table_dag
        )
        
        # Step 5: Analyze results
        analyze_task = PythonOperator(
            task_id='analyze_results',
            python_callable=analyze_reconciliation_results,
            params={'table': table, 'domain': domain},
            dag=table_dag
        )
        
        # Set step sequence dependencies
        get_secrets_task >> copy_s3_task >> create_external_task >> prepare_dataflow_task >> run_dataflow_task >> analyze_task
        
        # Register the table-specific DAG
        globals()[f'reconciliation_{domain}_{table}_dag'] = table_dag
    return None  # Function creates DAGs directly in globals


# Helper function to get secrets from config (used by secret retrieval step)
def get_secrets_from_config(config: dict, project_id: str) -> dict:
    """Extract and retrieve secrets from configuration"""
    secrets = {}
    
    try:
        secret_client = get_secret_manager_client()
        
        # Get reconciliation-specific secrets
        reconciliation_config = config.get('reconciliation', {})
        secret_mappings = reconciliation_config.get('secrets', {})
        
        for secret_type, secret_name in secret_mappings.items():
            try:
                secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
                response = secret_client.access_secret_version(request={"name": secret_path})
                secrets[secret_type] = response.payload.data.decode('UTF-8')
            except Exception as e:
                print(f"Warning: Could not get secret {secret_name}: {e}")
    
    except Exception as e:
        print(f"Error retrieving secrets: {e}")
    
    return secrets


# Create reconciliation DAGs for each domain/table combination
domains_config = json.loads(Variable.get('domains_config', '{}'))
for domain, config in domains_config.items():
    if config.get('enabled', False) and config.get('reconciliation_enabled', False):
        tables = config.get('tables', [])
        if tables:
            create_reconciliation_dag(domain, tables)
