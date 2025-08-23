จากการวิเคราะห์ project และ documents ที่คุณแนบมา ผมเห็นปัญหาหลักและ solution ที่ควรปรับ ดังนี้:

## 🎯 การปรับปรุงแต่ละ Pipeline ตาม Pipeline Details

### 1. **Initiate Pipeline** - ปรับใช้ SecretsManagerRetrieveSecretOperator

```python
# airflow/dags/initiate_pipeline.py (ปรับปรุง)
from airflow.providers.google.cloud.operators.secret_manager import SecretsManagerRetrieveSecretOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

def create_initiate_dag(domain: str, tables: list):
    dag = DAG(
        f'initiate_{domain}_pipeline',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
    )
    
    # Step 1: Get Secrets using Operator (ไม่ต้องสร้าง client)
    get_aws_key = SecretsManagerRetrieveSecretOperator(
        task_id='get_aws_access_key',
        secret_id='aws-s3-access-key-id',
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    get_aws_secret = SecretsManagerRetrieveSecretOperator(
        task_id='get_aws_secret_key',
        secret_id='aws-s3-secret-access-key',
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    get_s3_bucket = SecretsManagerRetrieveSecretOperator(
        task_id='get_s3_bucket',
        secret_id='aws-s3-bucket-name',
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    # Step 2: Use S3ToGCSOperator แทน STS client
    for table in tables:
        copy_s3_to_gcs = S3ToGCSOperator(
            task_id=f'copy_{table}_to_gcs',
            bucket=f'gcs-staging-{domain}',
            prefix=f'{table}/',
            aws_conn_id='aws_s3_connection',  # ใช้ Airflow Connection
            dest_gcs=f'gs://gcs-staging-{domain}/{table}/',
            replace=True,
            dag=dag
        )
        
        # Step 3: ใช้ BigQueryCreateExternalTableOperator (มีอยู่แล้ว ✅)
        # Step 4: ใช้ BigQueryInsertJobOperator (มีอยู่แล้ว ✅)
        
        # Chain tasks
        [get_aws_key, get_aws_secret, get_s3_bucket] >> copy_s3_to_gcs
```

### 2. **Realtime Pipeline** - ใช้ Native Beam I/O

```python
# dataflow/pipelines/hybrid_pipeline.py (ปรับปรุง)
import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub

class HybridPipeline:
    def run_realtime_with_native_io(self, pipeline_options):
        """Realtime pipeline using Native I/O - no client creation"""
        
        with beam.Pipeline(options=pipeline_options) as pipeline:
            
            # Step 1: Read from Pub/Sub - Native I/O
            messages = (
                pipeline
                | 'ReadFromPubSub' >> ReadFromPubSub(
                    subscription=f'projects/{project_id}/subscriptions/{domain}-events-sub',
                    with_attributes=True,
                    id_label='message_id'
                )
                | 'ParseJSON' >> beam.Map(lambda x: json.loads(x.data.decode('utf-8')))
            )
            
            # Step 2: Check dependencies - ใช้ ReadFromBigQuery
            dependency_check = (
                messages
                | 'CheckDependency' >> beam.FlatMap(
                    lambda x: self.check_dependency_native(x)
                )
            )
            
            # Step 3: Get secrets - ใช้ Pipeline Options แทน
            # ส่ง secrets ผ่าน pipeline options จาก Airflow
            
            # Step 4: Fetch source data - Native BigQuery I/O
            source_data = (
                dependency_check
                | 'FetchFromBigQuery' >> ReadFromBigQuery(
                    query=lambda x: f"""
                        SELECT * FROM `{x['source_table']}`
                        WHERE member_id = '{x['member_id']}'
                    """,
                    use_standard_sql=True,
                    gcs_location=temp_location
                )
            )
            
            # Step 5: Transform & Write - Native I/O
            (source_data
             | 'Transform' >> beam.Map(self.transform_data)
             | 'WriteToBigQuery' >> WriteToBigQuery(
                 table=lambda x: self.get_target_table(x),
                 schema='SCHEMA_AUTODETECT',
                 write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND,
                 create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
                 method='STORAGE_WRITE_API'  # ใช้ Storage Write API
             ))
    
    def check_dependency_native(self, element):
        """Check dependency using Native BigQuery I/O"""
        # ใช้ BigQuery I/O แทนการสร้าง client
        query = f"""
            SELECT COUNT(*) as count
            FROM `project.batch_control.job_status`
            WHERE job_name = 'daily_etl' 
            AND status = 'COMPLETED'
            AND DATE(completion_time) = CURRENT_DATE()
        """
        # Return element if dependency passed
        return [element] if self.validate_dependency(query) else []
```

### 3. **Batch Pipeline** - Reuse Realtime Code

```python
# airflow/dags/batch_pipeline.py (ปรับปรุง)
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

def create_batch_dag(domain: str):
    dag = DAG(
        f'batch_{domain}_pipeline',
        default_args=default_args,
        schedule_interval='@hourly',
        catchup=False
    )
    
    # ใช้ code เดียวกับ realtime แต่ส่ง param mode=batch
    run_batch_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_batch_dataflow',
        py_file='gs://dataflow-templates/hybrid_pipeline.py',
        job_name=f'batch-{domain}-{{{{ ds_nodash }}}}',
        options={
            'mode': 'batch',  # ต่างจาก realtime ตรงนี้
            'config_path': f'gs://configs/{domain}/config.yaml',
            'domain': domain,
            'batch_window_hours': 1,
            'runner': 'DataflowRunner',
            'project': '{{ var.value.gcp_project_id }}',
            'region': '{{ var.value.gcp_region }}',
            'temp_location': f'gs://dataflow-temp/{domain}/batch/{{{{ ds }}}}',
            'staging_location': f'gs://dataflow-staging/{domain}/batch/{{{{ ds }}}}',
            'setup_file': './setup.py'
        },
        dataflow_config={
            'job_name': f'batch-{domain}',
            'num_workers': 5,
            'max_num_workers': 20,
            'machine_type': 'n2-standard-4',
            'disk_size_gb': 100,
            'service_account_email': '{{ var.value.dataflow_service_account }}'
        },
        dag=dag
    )
```

### 4. **Reconciliation Pipeline** - ใช้ Federated Queries

```python
# airflow/dags/reconciliation_pipeline.py (ปรับปรุง)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

def create_reconciliation_dag(domain: str, tables: list):
    dag = DAG(
        f'reconciliation_{domain}_pipeline',
        default_args=default_args,
        schedule_interval='@daily'
    )
    
    # Step 1: Get Secrets
    get_aws_creds = SecretsManagerRetrieveSecretOperator(
        task_id='get_aws_credentials',
        secret_id='aws-s3-credentials',
        project_id='{{ var.value.gcp_project_id }}',
        dag=dag
    )
    
    # Step 2: Create External Table for S3 (Federated Query)
    for table in tables:
        create_s3_external = BigQueryInsertJobOperator(
            task_id=f'create_s3_external_{table}',
            configuration={
                'query': {
                    'query': f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{domain}_reconcile_temp.{table}_s3`
                        OPTIONS (
                            format = 'PARQUET',
                            uris = ['s3://{s3_bucket}/{domain}/{table}/*.parquet'],
                            connection_name = 'projects/{project_id}/locations/{region}/connections/aws-s3-connection'
                        )
                    """,
                    'useLegacySql': False
                }
            },
            dag=dag
        )
        
        # Step 3: Run comparison using SQL (no Dataflow needed)
        run_comparison = BigQueryInsertJobOperator(
            task_id=f'compare_{table}',
            configuration={
                'query': {
                    'query': f"""
                        WITH comparison AS (
                            SELECT 
                                COALESCE(s3.id, bq.id) as record_id,
                                CASE 
                                    WHEN s3.id IS NULL THEN 'MISSING_IN_S3'
                                    WHEN bq.id IS NULL THEN 'MISSING_IN_BQ'
                                    WHEN s3.* != bq.* THEN 'MISMATCH'
                                    ELSE 'MATCH'
                                END as status,
                                s3.* as s3_data,
                                bq.* as bq_data
                            FROM `{project_id}.{domain}_reconcile_temp.{table}_s3` s3
                            FULL OUTER JOIN `{project_id}.{domain}_raw.{table}` bq
                            ON s3.id = bq.id
                        )
                        INSERT INTO `{project_id}.{domain}_audit.reconciliation_results`
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP() as reconciliation_timestamp
                        FROM comparison
                        WHERE status != 'MATCH'
                    """,
                    'useLegacySql': False
                }
            },
            dag=dag
        )
        
        get_aws_creds >> create_s3_external >> run_comparison
```

## 🔧 Monitoring Solution - Cloud Monitoring Alerts

```yaml
# monitoring/alert_policies.yaml
apiVersion: monitoring.coreos.com/v1
kind: AlertPolicy
metadata:
  name: dataflow-window-latency
spec:
  displayName: "Dataflow Window Processing Latency"
  conditions:
  - displayName: "High window latency"
    conditionThreshold:
      filter: |
        resource.type="dataflow_job"
        AND metric.type="dataflow.googleapis.com/job/streaming/watermark_age"
      comparison: COMPARISON_GT
      thresholdValue: 300  # 5 minutes
      duration: 60s
      aggregations:
      - alignmentPeriod: 60s
        perSeriesAligner: ALIGN_MAX
  notificationChannels:
  - projects/${PROJECT_ID}/notificationChannels/${SLACK_CHANNEL}
  alertStrategy:
    autoClose: 86400s  # 24 hours
```

## 📋 Best Practices Documentation

### **1. Client Management Best Practices**

```python
# ❌ AVOID: Creating clients in DoFn
class BadDoFn(beam.DoFn):
    def process(self, element):
        client = bigquery.Client()  # ❌ Creates new client per element
        
# ✅ BETTER: Use Native I/O
ReadFromBigQuery(query="SELECT ...", use_standard_sql=True)
WriteToBigQuery(table="...", method="STORAGE_WRITE_API")

# ✅ BEST: Use Airflow Operators for orchestration
BigQueryInsertJobOperator(configuration={...})
SecretsManagerRetrieveSecretOperator(secret_id="...")
```

### **2. Secret Management Pattern**

```python
# ✅ Use Airflow Connections
from airflow.models import Connection

# Create connection in Airflow UI or CLI
conn = Connection(
    conn_id='aws_s3_connection',
    conn_type='s3',
    login='{{ task_instance.xcom_pull(task_ids="get_aws_key") }}',
    password='{{ task_instance.xcom_pull(task_ids="get_aws_secret") }}'
)
```

### **3. Pipeline Design Principles**

| Principle | Implementation |
|-----------|---------------|
| **Separation of Concerns** | Airflow = Orchestration, Dataflow = Processing |
| **Idempotency** | Use WRITE_TRUNCATE or partitioned writes |
| **Scalability** | Native I/O with auto-scaling |
| **Cost Optimization** | Use appropriate machine types and storage classes |
| **Monitoring** | Cloud Monitoring + BigQuery audit tables |

### **4. Architecture Decision Records (ADRs)**

#### ADR-001: Use Native I/O Instead of Client Libraries
- **Status**: Accepted
- **Context**: Client bloat causes memory issues in long-running pipelines
- **Decision**: Use Apache Beam Native I/O connectors
- **Consequences**: 90% less code, better performance, automatic retries

#### ADR-002: SQL-First for Data Quality
- **Status**: Accepted  
- **Context**: Complex reconciliation logic
- **Decision**: Use BigQuery SQL with Federated Queries instead of Dataflow
- **Consequences**: Simpler code, better performance, easier debugging

## 🚀 Migration Checklist

- [ ] Replace all `SecretManagerServiceClient` with `SecretsManagerRetrieveSecretOperator`
- [ ] Replace `monitoring_v3.MetricServiceClient` with Cloud Monitoring Alert Policies
- [ ] Convert BigQuery client usage to Native I/O in Dataflow
- [ ] Use BigQuery Federated Queries for S3 reconciliation
- [ ] Setup Airflow Connections for external services
- [ ] Deploy monitoring dashboards and alerts
- [ ] Test windowing configurations
- [ ] Document operational runbooks

This approach จะช่วยลด complexity และเพิ่ม reliability ของ pipeline อย่างมาก!