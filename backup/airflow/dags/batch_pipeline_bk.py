# airflow/dags/batch_pipeline.py
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
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

def create_batch_dag(domain: str):
    """Create batch pipeline DAG for a specific domain"""
    
    dag = DAG(
        f'batch_{domain}_pipeline',
        default_args=default_args,
        description=f'Hourly batch pipeline for {domain} domain',
        schedule_interval='@hourly',
        catchup=False,
        tags=['batch', domain]
    )
    
    def prepare_dataflow_params(**context):
        """Prepare parameters for Dataflow job"""
        params = {
            'mode': 'batch',
            'config_path': f"gs://pipeline-configs/{domain}/config.yaml",
            'domain': domain,
            'batch_window_hours': 1,
            'temp_location': f"gs://dataflow-temp/{domain}/batch/{{ ds }}",
            'staging_location': f"gs://dataflow-staging/{domain}/batch/{{ ds }}",
            'machine_type': 'n2-standard-4',
            'max_workers': 20,
            'region': Variable.get('gcp_region', 'asia-southeast1'),
            'subnetwork': Variable.get('dataflow_subnetwork'),
            'service_account_email': Variable.get('dataflow_service_account')
        }
        
        # Store in XCom for next task
        return json.dumps(params)
    
    def validate_batch_results(**context):
        """Validate batch processing results"""
        from google.cloud import bigquery
        
        domain = context['domain']
        execution_date = context['execution_date']
        
        client = bigquery.Client()
        
        # Check record counts
        query = f"""
            SELECT 
                table_name,
                COUNT(*) as record_count
            FROM `{Variable.get('gcp_project_id')}.{domain}_raw.INFORMATION_SCHEMA.TABLES` t
            JOIN `{Variable.get('gcp_project_id')}.{domain}_raw.*` d
            ON t.table_name = d._table_name
            WHERE d._ingestion_timestamp >= @execution_date
            GROUP BY table_name
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("execution_date", "TIMESTAMP", execution_date)
            ]
        )
        
        results = client.query(query, job_config=job_config).result()
        
        for row in results:
            print(f"Table {row.table_name}: {row.record_count} records processed")
        
        return True
    
    prepare_params = PythonOperator(
        task_id='prepare_dataflow_params',
        python_callable=prepare_dataflow_params,
        dag=dag
    )
    
    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id='run_batch_dataflow',
        template='gs://dataflow-templates/hybrid_pipeline.json',
        job_name=f"batch-{domain}-{{ ds_nodash }}",
        parameters={
            'mode': 'batch',
            'config_path': f"gs://pipeline-configs/{domain}/config.yaml",
            'domain': domain,
            'batch_window_hours': 1,
            'temp_location': f"gs://dataflow-temp/{domain}/batch/{{ ds }}",
            'staging_location': f"gs://dataflow-staging/{domain}/batch/{{ ds }}",
        },
        dataflow_default_options={
            'project': Variable.get('gcp_project_id'),
            'region': Variable.get('gcp_region', 'asia-southeast1'),
            'zone': Variable.get('gcp_zone', 'asia-southeast1-a'),
            'tempLocation': f"gs://dataflow-temp/{domain}/batch/{{ ds }}",
            'network': Variable.get('dataflow_network'),
            'subnetwork': Variable.get('dataflow_subnetwork')
        },
        dag=dag
    )
    
    validate_results = PythonOperator(
        task_id='validate_results',
        python_callable=validate_batch_results,
        op_kwargs={'domain': domain},
        dag=dag
    )
    
    prepare_params >> run_dataflow >> validate_results
    
    return dag

# Create DAGs for each domain
domains_config = json.loads(Variable.get('batch_domains', '["member", "order", "product"]'))
for domain in domains_config:
    globals()[f'batch_{domain}_dag'] = create_batch_dag(domain)
