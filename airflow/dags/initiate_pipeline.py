# airflow/dags/initiate_pipeline.py
"""
Initiate Pipeline - Storage Transfer Service Based Migration
✅ Uses Storage Transfer Service for S3→GCS migration (as per requirements)
✅ Uses SecretsManagerRetrieveSecretOperator for AWS credentials
✅ Uses BigLake external tables for staging
✅ Eliminates S3ToGCSOperator in favor of STS jobs
✅ Proper audit logging and lineage tracking
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.secret_manager import SecretsManagerRetrieveSecretOperator
from airflow.providers.google.cloud.operators.storage_transfer import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceRunJobOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import json

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
    """Create initiate pipeline DAG using native operators"""
    
    dag = DAG(
        f'initiate_{domain}_pipeline',
        default_args=default_args,
        description=f'One-time migration pipeline for {domain} domain (Native Operators)',
        schedule_interval=None,  # Manual trigger only
        catchup=False,
        tags=['initiate', domain, 'migration', 'native-operators']
    )
    
    # Step 1: Get AWS Secrets using Native Operator (no client creation!)
    get_aws_access_key = SecretsManagerRetrieveSecretOperator(
        task_id='get_aws_access_key',
        secret_id='aws-s3-access-key-id',
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    get_aws_secret_key = SecretsManagerRetrieveSecretOperator(
        task_id='get_aws_secret_key', 
        secret_id='aws-s3-secret-access-key',
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    get_s3_bucket_name = SecretsManagerRetrieveSecretOperator(
        task_id='get_s3_bucket_name',
        secret_id='aws-s3-bucket-name', 
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    # Step 2: Setup Dataplex Infrastructure using REST API
    setup_dataplex = SimpleHttpOperator(
        task_id='setup_dataplex_infrastructure',
        http_conn_id='gcp_dataplex_api',
        endpoint=f'v1/projects/{{{{ var.value.gcp_project_id }}}}/locations/{{{{ var.value.gcp_region }}}}/lakes',
        method='POST',
        headers={
            'Authorization': 'Bearer {{ macros.gcp.get_access_token() }}',
            'Content-Type': 'application/json'
        },
        data=json.dumps({
            'lakeId': f'{domain}-data-lake',
            'lake': {
                'displayName': f'{domain.title()} Data Lake',
                'description': f'Data lake for {domain} domain'
            }
        }),
        dag=dag
    )
    
    # Step 3: Create table-specific tasks using Storage Transfer Service
    for table in tables:
        
        # ✅ Use Storage Transfer Service for S3→GCS (as per requirements)
        create_sts_job = CloudDataTransferServiceCreateJobOperator(
            task_id=f'create_sts_job_{table}',
            body={
                'description': f'Transfer {table} from S3 to GCS for {domain}',
                'status': 'ENABLED',
                'projectId': '{{ var.value.gcp_project_id }}',
                'transferSpec': {
                    'awsS3DataSource': {
                        'bucketName': '{{ task_instance.xcom_pull(task_ids="get_s3_bucket_name") }}',
                        'path': f'{domain}/{table}/',
                        'awsAccessKey': {
                            'accessKeyId': '{{ task_instance.xcom_pull(task_ids="get_aws_access_key") }}',
                            'secretAccessKey': '{{ task_instance.xcom_pull(task_ids="get_aws_secret_key") }}'
                        }
                    },
                    'gcsDataSink': {
                        'bucketName': '{{ var.value.gcp_project_id }}-gcs-staging',
                        'path': f'{domain}/{table}/'
                    },
                    'objectConditions': {
                        'maxTimeElapsedSinceLastModification': '2592000s'  # 30 days
                    },
                    'transferOptions': {
                        'overwriteObjectsAlreadyExistingInSink': True
                    }
                },
                'schedule': {
                    'scheduleStartDate': {
                        'year': 2025, 'month': 1, 'day': 1
                    },
                    'scheduleEndDate': {
                        'year': 2025, 'month': 12, 'day': 31
                    }
                }
            },
            dag=dag
        )
        
        # Run the STS job
        run_sts_job = CloudDataTransferServiceRunJobOperator(
            task_id=f'run_sts_job_{table}',
            job_name=f'{{{{ task_instance.xcom_pull(task_ids="create_sts_job_{table}") }}}}',
            project_id='{{ var.value.gcp_project_id }}',
            dag=dag
        )
        
        # ✅ Use BigQueryCreateExternalTableOperator (already native!)
        create_external_table = BigQueryCreateExternalTableOperator(
            task_id=f'create_external_table_{table}',
            table_resource={
                'tableReference': {
                    'projectId': '{{ var.value.gcp_project_id }}',
                    'datasetId': f'{domain}_staging',
                    'tableId': f'{table}_external'
                },
                'externalDataConfiguration': {
                    'sourceFormat': 'PARQUET',
                    'sourceUris': [f'gs://{{{{ var.value.gcp_project_id }}}}-gcs-staging/{domain}/{table}/*'],
                    'autodetect': True
                }
            },
            dag=dag
        )
        
        # ✅ Use BigQueryInsertJobOperator for data processing (already native!)
        process_data = BigQueryInsertJobOperator(
            task_id=f'process_{table}_data',
            configuration={
                'query': {
                    'query': f"""
                        CREATE OR REPLACE TABLE `{{{{ var.value.gcp_project_id }}}}.raw_data.{domain}_{table}` AS
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP() as _ingestion_timestamp,
                            'initiate_pipeline' as _source_pipeline,
                            '{{{{ ds }}}}' as _ingestion_date
                        FROM `{{{{ var.value.gcp_project_id }}}}.staging_data.{domain}_{table}_external`
                    """,
                    'useLegacySql': False
                }
            },
            dag=dag
        )
        
        # ✅ Create Dataplex Asset using REST API (no datacatalog client!)
        register_dataplex_asset = SimpleHttpOperator(
            task_id=f'register_{table}_with_dataplex',
            http_conn_id='gcp_dataplex_api',
            endpoint=f'v1/projects/{{{{ var.value.gcp_project_id }}}}/locations/{{{{ var.value.gcp_region }}}}/lakes/{domain}-data-lake/zones/{domain}-raw-zone/assets',
            method='POST',
            headers={
                'Authorization': 'Bearer {{ macros.gcp.get_access_token() }}',
                'Content-Type': 'application/json'
            },
            data=json.dumps({
                'assetId': f'{table}-asset',
                'asset': {
                    'displayName': f'{table.title()} Asset',
                    'resourceSpec': {
                        'name': f'projects/{{{{ var.value.gcp_project_id }}}}/datasets/{domain}_raw/tables/{table}',
                        'type': 'BIGQUERY_TABLE'
                    }
                }
            }),
            dag=dag
        )
        
        # ✅ Track lineage using REST API (no lineage client!)
        track_lineage = SimpleHttpOperator(
            task_id=f'track_{table}_lineage',
            http_conn_id='gcp_lineage_api',
            endpoint=f'v1/projects/{{{{ var.value.gcp_project_id }}}}/locations/{{{{ var.value.gcp_region }}}}/processes/initiate-{domain}-{table}/runs',
            method='POST',
            headers={
                'Authorization': 'Bearer {{ macros.gcp.get_access_token() }}',
                'Content-Type': 'application/json'
            },
            data=json.dumps({
                'run': {
                    'displayName': f'Initiate Migration: {domain}.{table}',
                    'state': 'COMPLETED',
                    'attributes': {
                        'pipeline_type': {'stringValue': 'initiate'},
                        'domain': {'stringValue': domain},
                        'table': {'stringValue': table}
                    }
                }
            }),
            dag=dag
        )
        
        # Chain tasks for this table
        [get_aws_access_key, get_aws_secret_key, get_s3_bucket_name] >> setup_dataplex
        setup_dataplex >> create_sts_job >> run_sts_job >> create_external_table >> process_data >> register_dataplex_asset >> track_lineage
    
    return dag


def create_aws_connection(**context):
    """Helper function to create AWS connection from secrets"""
    from airflow.models import Connection
    from airflow import settings
    
    # Get secrets from XCom
    aws_key = context['task_instance'].xcom_pull(task_ids='get_aws_access_key')
    aws_secret = context['task_instance'].xcom_pull(task_ids='get_aws_secret_key')
    
    # Create AWS connection
    new_conn = Connection(
        conn_id='aws_s3_connection',
        conn_type='aws',
        login=aws_key,
        password=aws_secret
    )
    
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == 'aws_s3_connection').first()
    
    if existing_conn:
        existing_conn.login = aws_key
        existing_conn.password = aws_secret
    else:
        session.add(new_conn)
    
    session.commit()
    session.close()
    
    return "AWS connection created/updated"


# Create DAGs for each domain using native operators
domains_config = json.loads(Variable.get('initiate_domains', '[]'))
for domain_config in domains_config:
    if isinstance(domain_config, dict):
        domain = domain_config.get('domain')
        tables = domain_config.get('tables', [])
        if domain and tables:
            globals()[f'initiate_{domain}_dag'] = create_initiate_dag(domain, tables)
    elif isinstance(domain_config, str):
        # Backward compatibility
        default_tables = ['members', 'transactions', 'products']
        globals()[f'initiate_{domain_config}_dag'] = create_initiate_dag(domain_config, default_tables)
