# airflow/dags/realtime_trigger.py
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJobOperator
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
    """Create realtime pipeline DAG for a specific domain"""
    
    dag = DAG(
        f'realtime_{domain}_pipeline',
        default_args=default_args,
        description=f'Realtime streaming pipeline for {domain} domain',
        schedule_interval=None,  # Always running
        catchup=False,
        max_active_runs=1,
        tags=['realtime', domain, 'streaming']
    )
    
    def check_dependencies(**context):
        """Check if upstream dependencies are met"""
        from google.cloud import bigquery
        
        client = bigquery.Client()
        project_id = Variable.get('gcp_project_id')
        
        # Check if batch control table shows readiness
        query = f"""
            SELECT COUNT(*) as ready_count
            FROM `{project_id}.batch_control.job_status`
            WHERE job_name = 'daily_etl'
            AND status = 'COMPLETED'
            AND DATE(completion_time) = CURRENT_DATE()
        """
        
        result = client.query(query).result()
        ready_count = next(result).ready_count
        
        if ready_count == 0:
            raise ValueError("Upstream dependencies not ready")
        
        return True
    
    def prepare_dataflow_job(**context):
        """Prepare Dataflow job configuration"""
        job_config = {
            'jobName': f"realtime-{domain}-{context['ds_nodash']}-{context['ts_nodash']}",
            'gcsPath': 'gs://dataflow-templates/hybrid_pipeline.json',
            'parameters': {
                'mode': 'realtime',
                'config_path': f"gs://pipeline-configs/{domain}/config.yaml",
                'domain': domain,
                'temp_location': f"gs://dataflow-temp/{domain}/realtime",
                'staging_location': f"gs://dataflow-staging/{domain}/realtime",
            },
            'environment': {
                'maxWorkers': 50,
                'machineType': 'n2-standard-4',
                'zone': Variable.get('gcp_zone', 'asia-southeast1-a'),
                'serviceAccountEmail': Variable.get('dataflow_service_account'),
                'network': Variable.get('dataflow_network'),
                'subnetwork': Variable.get('dataflow_subnetwork'),
                'additionalExperiments': [
                    'enable_streaming_engine',
                    'enable_prime'
                ]
            }
        }
        
        return job_config
    
    def monitor_dataflow_job(**context):
        """Monitor the Dataflow job health"""
        from google.cloud import dataflow_v1beta3
        
        client = dataflow_v1beta3.JobsV1Beta3Client()
        project_id = Variable.get('gcp_project_id')
        location = Variable.get('gcp_region', 'asia-southeast1')
        
        # List current jobs
        request = dataflow_v1beta3.ListJobsRequest(
            project_id=project_id,
            location=location,
            filter=f'ACTIVE AND name:{domain}'
        )
        
        jobs = client.list_jobs(request=request)
        active_jobs = list(jobs)
        
        if not active_jobs:
            raise ValueError(f"No active Dataflow jobs found for {domain}")
        
        # Check job health
        for job in active_jobs:
            if job.current_state in ['JOB_STATE_FAILED', 'JOB_STATE_CANCELLED']:
                raise ValueError(f"Dataflow job {job.name} is in failed state")
        
        return f"Found {len(active_jobs)} healthy jobs"
    
    # Check dependencies
    dependency_check = PythonOperator(
        task_id='check_dependencies',
        python_callable=check_dependencies,
        dag=dag
    )
    
    # Prepare job configuration
    prepare_job = PythonOperator(
        task_id='prepare_dataflow_job',
        python_callable=prepare_dataflow_job,
        dag=dag
    )
    
    # Start Dataflow job
    start_dataflow = DataflowCreateJobOperator(
        task_id='start_realtime_dataflow',
        template='gs://dataflow-templates/hybrid_pipeline.json',
        job_name=f"realtime-{domain}-{{{{ ds_nodash }}}}",
        options={
            'mode': 'realtime',
            'config_path': f"gs://pipeline-configs/{domain}/config.yaml",
            'domain': domain,
            'temp_location': f"gs://dataflow-temp/{domain}/realtime",
            'staging_location': f"gs://dataflow-staging/{domain}/realtime",
        },
        dataflow_default_options={
            'project': Variable.get('gcp_project_id'),
            'region': Variable.get('gcp_region', 'asia-southeast1'),
            'zone': Variable.get('gcp_zone', 'asia-southeast1-a'),
            'serviceAccountEmail': Variable.get('dataflow_service_account'),
            'network': Variable.get('dataflow_network'),
            'subnetwork': Variable.get('dataflow_subnetwork'),
        },
        dag=dag
    )
    
    # Monitor job health
    monitor_job = PythonOperator(
        task_id='monitor_job_health',
        python_callable=monitor_dataflow_job,
        dag=dag
    )
    
    # Set dependencies
    dependency_check >> prepare_job >> start_dataflow >> monitor_job
    
    return dag

# Create DAGs for each domain
domains_config = json.loads(Variable.get('domains_config', '{}'))
for domain, config in domains_config.items():
    if config.get('enabled', False):
        globals()[f'realtime_{domain}_dag'] = create_realtime_dag(domain)
