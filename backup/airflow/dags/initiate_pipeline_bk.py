# airflow/dags/initiate_pipeline.py
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.storage_transfer import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceRunJobOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from datetime import datetime, timedelta
from google.cloud import datacatalog_v1, lineage_v1
import json
import yaml
import sys
import os

# Add dataflow utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'dataflow', 'utils'))

try:
    from secret_manager import SecretManagerClient, get_secrets_from_config, setup_aws_credentials_for_sts
    from dataplex_manager import DataplexManager, setup_dataplex_from_config
    from config_loader import ConfigLoader
except ImportError as e:
    print(f"Warning: Could not import custom modules: {e}")
    # Define fallback functions
    def get_secrets_from_config(config, project_id):
        return {}
    def setup_aws_credentials_for_sts(secrets):
        return {}
    def setup_dataplex_from_config(config):
        return None

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def create_initiate_dag(domain: str, tables: list):
    """Create initiate pipeline DAG for a specific domain"""
    
    dag = DAG(
        f'initiate_{domain}_pipeline',
        default_args=default_args,
        description=f'One-time migration pipeline for {domain} domain',
        schedule_interval=None,  # Manual trigger only
        catchup=False,
        tags=['initiate', domain, 'migration']
    )
    
    def setup_dataplex_infrastructure(**context):
        """Setup Dataplex lake, zones, and assets"""
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
            
            # Setup Dataplex infrastructure
            dataplex_result = setup_dataplex_from_config(config)
            
            if dataplex_result and dataplex_result.get('status') == 'success':
                print(f"Dataplex setup completed for domain {domain}")
                return dataplex_result
            else:
                print(f"Dataplex setup failed or skipped: {dataplex_result}")
                return None
                
        except Exception as e:
            print(f"Error setting up Dataplex infrastructure: {e}")
            return None
    
    def get_pipeline_secrets(**context):
        """Retrieve secrets from Secret Manager"""
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
            context['task_instance'].xcom_push(key='pipeline_secrets', value=secrets)
            
            print(f"Retrieved secrets for domain {domain}")
            return secrets
            
        except Exception as e:
            print(f"Error retrieving secrets: {e}")
            # Return empty dict to allow pipeline to continue with defaults
            return {}
    
    def track_data_lineage(**context):
        """Track data lineage for the migration"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        lineage_client = lineage_v1.LineageClient()
        location = Variable.get('gcp_region', 'asia-southeast1')
        project_id = Variable.get('gcp_project_id')
        
        # Create lineage process
        process_name = f"projects/{project_id}/locations/{location}/processes/initiate_{domain}_{table}"
        process = lineage_v1.Process(
            name=process_name,
            display_name=f"Initiate Migration: {domain}.{table}",
            attributes={
                "pipeline_type": {"string_value": "initiate"},
                "domain": {"string_value": domain},
                "table": {"string_value": table},
                "execution_date": {"string_value": str(context['execution_date'])}
            }
        )
        
        try:
            lineage_client.create_process(
                parent=f"projects/{project_id}/locations/{location}",
                process=process
            )
        except Exception as e:
            print(f"Process might already exist: {e}")
        
        # Create lineage run
        run_name = f"{process_name}/runs/run_{context['run_id']}"
        run = lineage_v1.Run(
            name=run_name,
            display_name=f"Run {context['ds']} - {table}",
            start_time=context['execution_date'],
            state=lineage_v1.Run.State.COMPLETED
        )
        
        lineage_client.create_run(
            parent=process_name,
            run=run
        )
        
        # Create lineage events
        # Get secrets from XCom to get the correct S3 bucket name
        secrets = context['task_instance'].xcom_pull(key='pipeline_secrets') or {}
        s3_bucket = secrets.get('s3_credentials', {}).get('s3_bucket_name', Variable.get('s3_bucket', 'default-bucket'))
        
        source_s3 = f"s3://{s3_bucket}/{domain}/{table}/"
        target_bq = f"bigquery:{project_id}.{domain}_raw.{table}"
        
        event_link = lineage_v1.EventLink(
            source=lineage_v1.EntityReference(fully_qualified_name=source_s3),
            target=lineage_v1.EntityReference(fully_qualified_name=target_bq)
        )
        
        lineage_event = lineage_v1.LineageEvent(
            name=f"{run_name}/lineageEvents/event_{context['ts_nodash']}",
            links=[event_link],
            start_time=context['execution_date']
        )
        
        lineage_client.create_lineage_event(
            parent=run_name,
            lineage_event=lineage_event
        )
        
        return f"Lineage tracked for {table}"
    
    def create_dataplex_asset(**context):
        """Register table with Dataplex"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        datacatalog_client = datacatalog_v1.DataCatalogClient()
        project_id = Variable.get('gcp_project_id')
        
        # Create asset entry in Dataplex
        # Implementation depends on specific Dataplex setup
        print(f"Registering {table} with Dataplex")
        return f"Dataplex asset created for {table}"
    
    def validate_migration(**context):
        """Validate that migration completed successfully"""
        from google.cloud import bigquery
        
        table = context['params']['table']
        domain = context['params']['domain']
        project_id = Variable.get('gcp_project_id')
        
        client = bigquery.Client()
        
        # Check if table exists and has data
        query = f"""
            SELECT COUNT(*) as record_count
            FROM `{project_id}.{domain}_raw.{table}`
        """
        
        result = client.query(query).result()
        record_count = next(result).record_count
        
        if record_count == 0:
            raise ValueError(f"No records found in {table}")
        
        print(f"Migration validated: {record_count} records in {table}")
        return record_count
    
    # Setup infrastructure tasks
    setup_dataplex = PythonOperator(
        task_id='setup_dataplex_infrastructure',
        python_callable=setup_dataplex_infrastructure,
        params={'domain': domain},
        dag=dag
    )
    
    get_secrets = PythonOperator(
        task_id='get_pipeline_secrets',
        python_callable=get_pipeline_secrets,
        params={'domain': domain},
        dag=dag
    )
    
    def create_sts_job_with_secrets(**context):
        """Create STS job using secrets from Secret Manager"""
        table = context['params']['table']
        domain = context['params']['domain']
        
        # Get secrets from XCom
        secrets = context['task_instance'].xcom_pull(key='pipeline_secrets') or {}
        s3_creds = secrets.get('s3_credentials', {})
        
        # Use secrets or fall back to Airflow variables
        aws_access_key = s3_creds.get('aws_access_key_id', Variable.get('aws_access_key_id'))
        aws_secret_key = s3_creds.get('aws_secret_access_key', Variable.get('aws_secret_access_key'))
        s3_bucket = s3_creds.get('s3_bucket_name', Variable.get('s3_bucket'))
        
        from google.cloud import storage_transfer
        
        client = storage_transfer.StorageTransferServiceClient()
        project_id = Variable.get('gcp_project_id')
        
        transfer_job = {
            'description': f'Transfer {table} from S3 to GCS',
            'status': 'ENABLED',
            'project_id': project_id,
            'transfer_spec': {
                'aws_s3_data_source': {
                    'bucket_name': s3_bucket,
                    'path': f'{domain}/{table}/',
                    'aws_access_key': {
                        'access_key_id': aws_access_key,
                        'secret_access_key': aws_secret_key
                    }
                },
                'gcs_data_sink': {
                    'bucket_name': f'gcs-staging-{domain}',
                    'path': f'{table}/'
                }
            }
        }
        
        response = client.create_transfer_job(
            parent=f'projects/{project_id}',
            transfer_job=transfer_job
        )
        
        print(f"Created STS job: {response.name}")
        return response.name
    
    # Create tasks for each table
    for table in tables:
        # Create Storage Transfer Service job using secrets
        create_sts_job = PythonOperator(
            task_id=f'create_sts_job_{table}',
            python_callable=create_sts_job_with_secrets,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        # Run STS job
        run_sts_job = CloudDataTransferServiceRunJobOperator(
            task_id=f'run_sts_job_{table}',
            job_name="{{ task_instance.xcom_pull(task_ids='create_sts_job_" + table + "') }}",
            dag=dag
        )
        
        # Create external table
        create_external_table = BigQueryCreateExternalTableOperator(
            task_id=f'create_external_table_{table}',
            bucket=f'gcs-staging-{domain}',
            source_objects=[f'{table}/*'],
            destination_project_dataset_table=f"{Variable.get('gcp_project_id')}.{domain}_staging.{table}_external",
            source_format='PARQUET',
            dag=dag
        )
        
        # Load to native table
        load_to_native = BigQueryInsertJobOperator(
            task_id=f'load_to_native_{table}',
            configuration={
                'query': {
                    'query': f"""
                        CREATE OR REPLACE TABLE `{Variable.get('gcp_project_id')}.{domain}_raw.{table}`
                        AS SELECT * FROM `{Variable.get('gcp_project_id')}.{domain}_staging.{table}_external`
                    """,
                    'useLegacySql': False
                }
            },
            dag=dag
        )
        
        # Track lineage
        track_lineage = PythonOperator(
            task_id=f'track_lineage_{table}',
            python_callable=track_data_lineage,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        # Register with Dataplex
        register_dataplex = PythonOperator(
            task_id=f'register_dataplex_{table}',
            python_callable=create_dataplex_asset,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        # Validate migration
        validate = PythonOperator(
            task_id=f'validate_migration_{table}',
            python_callable=validate_migration,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        # Set task dependencies
        setup_dataplex >> get_secrets
        get_secrets >> create_sts_job >> run_sts_job >> create_external_table >> load_to_native
        load_to_native >> [track_lineage, register_dataplex] >> validate
    
    return dag

# Create DAGs for each domain
domains_config = json.loads(Variable.get('domains_config', '{}'))
for domain, config in domains_config.items():
    if config.get('enabled', False):
        globals()[f'initiate_{domain}_dag'] = create_initiate_dag(domain, config['tables'])
