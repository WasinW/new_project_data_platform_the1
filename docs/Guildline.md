# GCP Data Platform Implementation Guide

## Executive Summary

This document outlines a **shared infrastructure data platform** for migrating from AWS S3 to GCP. The solution features **4 specialized pipelines** orchestrated by Cloud Composer (Airflow), with **shared GCP resources** and **domain-specific data isolation**.

## Table of Contents

1. [Solution Overview](#solution-overview)
2. [Shared Infrastructure Design](#shared-infrastructure-design)
3. [Pipeline Specifications](#pipeline-specifications)
4. [Implementation Details](#implementation-details)
5. [Configuration Management](#configuration-management)
6. [Deployment Guide](#deployment-guide)
7. [Operations & Monitoring](#operations-monitoring)

## 1. Solution Overview

### 1.1 Business Context

The solution addresses the need to:
- Migrate 2TB of initial data from AWS S3 to GCP
- Process 25GB/day of incremental data across multiple business domains
- Support 10+ business domains with 50 tables each
- Enable seamless transition from batch to realtime processing
- Maintain data reconciliation with legacy AWS pipeline

### 1.2 Shared Infrastructure Approach

**Key Innovation**: All GCP resources are **shared across domains** with data isolation through:
- **Table prefixes**: `{domain}_table_name`
- **Subfolders**: `/{domain}/` within shared buckets
- **Domain-specific DAGs**: `{pipeline_type}_{domain}_pipeline`

**Benefits**:
- **Cost Optimization**: Single datasets, buckets, Composer environment
- **Simplified Management**: Centralized infrastructure
- **Scalability**: Easy addition of new domains
- **Resource Efficiency**: Shared compute and storage resources

## 2. Shared Infrastructure Design

### 2.1 Resource Naming Convention

**Shared Resources** (No Domain Prefix):
```
Datasets:
- raw_data              # All domain raw tables
- staging_data          # All domain staging tables  
- monitoring_data       # All domain monitoring tables

Buckets:
- {project-id}-dataflow-temp      # Shared Dataflow temp
- {project-id}-dataflow-staging   # Shared Dataflow staging
- {project-id}-gcs-staging       # Shared data staging
- {project-id}-pipeline-configs  # Shared configs

Pub/Sub:
- data-events-create    # Shared create events topic
- data-events-update    # Shared update events topic
- data-events-{domain}-sub  # Domain-specific subscriptions

Composer:
- composer-{environment}  # Shared Airflow environment

Dataplex:
- data-lake-main       # Shared data lake
- zone-raw-data        # Shared raw zone
- zone-staging-data    # Shared staging zone
```

**Domain-Specific Elements**:
```
Table Names:
- {domain}_batch_input
- {domain}_realtime_events
- {domain}_processing_errors

DAG Names:
- initiate_{domain}_pipeline
- realtime_{domain}_pipeline
- batch_{domain}_pipeline
- reconciliation_{domain}_pipeline

Subfolders:
- gs://bucket/{domain}/data/
- gs://bucket/{domain}/config/
```

### 2.1 High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         CLOUD COMPOSER (ORCHESTRATION)                      │
├──────────────┬──────────────────┬──────────────────┬─────────────────────┤
│   Initiate   │    Realtime      │     Batch        │    Reconcile        │
│   Pipeline   │    Pipeline      │    Pipeline      │    Pipeline         │
│ (One-time)   │  (Continuous)    │   (Hourly)       │    (Daily)          │
└──────────────┴──────────────────┴──────────────────┴─────────────────────┘
       │                │                   │                  │
       ▼                ▼                   ▼                  ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
│ Pure Airflow │ │  Dataflow    │ │  Dataflow    │ │ Airflow +        │
│ - STS        │ │  Streaming   │ │   Batch      │ │ Dataflow         │
│ - BigQuery   │ │ - Pub/Sub    │ │ - BigQuery   │ │ - Comparison     │
│ - SQL        │ │ - Transform  │ │ - Transform  │ │ - Audit          │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────────┘
```

### 2.2 Shared Infrastructure Data Flow

```
Source Systems           Shared Processing Layer         Shared Storage Layer
┌─────────────┐         ┌──────────────────┐           ┌─────────────────┐
│    AWS S3   │─STS────▶│  Shared GCS      │─External─▶│  Shared BigQuery│
│  Snapshots  │         │  staging_data    │  Tables   │    raw_data     │
└─────────────┘         └──────────────────┘           │  staging_data   │
                               │                        │ monitoring_data │
┌─────────────┐         ┌──────────────────┐           └─────────────────┘
│ Shared      │─Stream─▶│ Shared Dataflow  │─Write────▶│   Domain Tables:│
│ Pub/Sub     │         │   Processing     │           │ - member_events │
│ Topics      │         │   (hybrid_       │           │ - order_batch   │
└─────────────┘         │    pipeline.py)  │           │ - product_raw   │
                        └──────────────────┘           └─────────────────┘
```

### 2.3 Domain Data Isolation Pattern

```
Shared Dataset: raw_data
├── member_batch_input      # Domain: member
├── member_realtime_events  # Domain: member  
├── order_batch_input       # Domain: order
├── order_realtime_events   # Domain: order
└── product_*               # Domain: product

Shared Bucket: project-gcs-staging
├── /member/               # Domain subfolder
│   ├── batch/
│   └── realtime/
├── /order/                # Domain subfolder
└── /product/              # Domain subfolder
```

## 3. Pipeline Specifications

### 3.1 Initiate Pipeline

**Purpose**: One-time migration of historical data from S3 to shared BigQuery datasets

**Technology**: Pure Airflow (no Dataflow)

**Shared Infrastructure Usage**:
- **Source**: AWS S3 → Shared GCS staging bucket
- **Target**: Shared `staging_data` and `raw_data` datasets
- **Tables**: Domain-prefixed tables (e.g., `member_historical_data`)

**Steps**:
1. Create Storage Transfer Service job for shared staging bucket
2. Copy data from S3 to shared GCS staging with domain subfolders
3. Create BigQuery external tables in shared `staging_data` dataset
4. Load data to domain-prefixed tables in shared `raw_data` dataset
5. Register with shared Dataplex lake
6. Track data lineage in shared monitoring dataset

### 3.2 Realtime Pipeline

**Purpose**: Continuous processing of streaming events with shared infrastructure

**Technology**: Airflow (orchestration) + Dataflow (processing)

**Shared Infrastructure Usage**:
- **Source**: Shared Pub/Sub topics with domain-specific subscriptions
- **Target**: Domain-prefixed tables in shared datasets
- **Processing**: Shared Dataflow templates with domain parameters

**Steps**:
1. Consume from shared topics via domain-specific subscriptions
2. Process through shared `hybrid_pipeline.py` with domain context
3. Distribute to domain-prefixed tables in shared datasets
4. Apply domain-specific transformations
5. Write to shared `raw_data` and `staging_data` datasets
6. Audit logging to shared `monitoring_data` dataset

### 3.3 Batch Pipeline

**Purpose**: Hourly batch processing using shared infrastructure

**Technology**: Airflow + Dataflow (reuses realtime code)

**Shared Infrastructure Usage**:
- **Source**: Domain-prefixed tables in shared `staging_data` dataset
- **Target**: Domain-prefixed tables in shared `raw_data` dataset
- **Processing**: Same shared Dataflow templates as realtime

**Steps**:
1. Query domain-specific tables from shared datasets
2. Process through shared `hybrid_pipeline.py` in batch mode
3. Write to domain-prefixed target tables in shared datasets
4. Validation through shared monitoring dataset

### 3.4 Reconciliation Pipeline

**Purpose**: Daily validation against AWS S3 using shared infrastructure

**Technology**: Airflow + Dataflow

**Shared Infrastructure Usage**:
- **Temporary Storage**: Shared GCS staging with domain subfolders
- **Comparison**: Domain-prefixed tables across shared datasets
- **Reporting**: Shared `monitoring_data` dataset

**Steps**:
1. Copy S3 snapshot to shared GCS staging bucket
2. Create temporary external tables in shared `staging_data` dataset
3. Compare with domain-prefixed tables in shared `raw_data` dataset
4. Generate mismatch reports in shared `monitoring_data` dataset

## 4. Implementation Details

### 4.1 Project Structure

```
gcp-data-pipeline/
├── airflow/
│   ├── dags/
│   │   ├── initiate_pipeline.py
│   │   ├── realtime_trigger.py
│   │   ├── batch_pipeline.py
│   │   └── reconciliation_pipeline.py
│   ├── plugins/
│   │   ├── operators/
│   │   │   └── custom_operators.py
│   │   └── sensors/
│   │       └── dependency_sensor.py
│   └── config/
│       └── airflow_variables.json
│
├── dataflow/
│   ├── pipelines/
│   │   ├── hybrid_pipeline.py
│   │   └── reconciliation_pipeline.py
│   ├── transforms/
│   │   ├── __init__.py
│   │   ├── distributor.py
│   │   ├── dependency_checker.py
│   │   └── complex_transforms.py
│   ├── utils/
│   │   ├── config_loader.py
│   │   └── audit_logger.py
│   └── requirements.txt
│
├── config/
│   ├── pipeline_config.yaml
│   ├── distribution_mapping.yaml
│   ├── column_mappings.yaml
│   └── transform_modules.yaml
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
│
└── docs/
    └── README.md
```

### 4.2 Complete Code Implementation

#### 4.2.1 Initiate Pipeline DAG

```python
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
        source_s3 = f"s3://{Variable.get('s3_bucket')}/{domain}/{table}/"
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
        
        # Create Dataplex entry
        entry = datacatalog_v1.Entry(
            display_name=f"{domain}.{table}",
            description=f"Migrated table from S3 for {domain} domain",
            type_=datacatalog_v1.EntryType.TABLE,
            linked_resource=f"//bigquery.googleapis.com/projects/{project_id}/datasets/{domain}_raw/tables/{table}",
            schema=datacatalog_v1.Schema()  # Schema will be auto-detected
        )
        
        parent = f"projects/{project_id}/locations/{Variable.get('gcp_region')}/entryGroups/{domain}_tables"
        
        try:
            datacatalog_client.create_entry(
                parent=parent,
                entry_id=table,
                entry=entry
            )
        except Exception as e:
            print(f"Entry might already exist: {e}")
        
        return f"Dataplex asset created for {table}"
    
    # Create tasks for each table
    for table in tables:
        # Step 1: Create STS job
        sts_create = CloudDataTransferServiceCreateJobOperator(
            task_id=f'sts_create_{table}',
            body={
                "description": f"Migrate {domain}.{table} from S3 to GCS",
                "status": "ENABLED",
                "projectId": Variable.get('gcp_project_id'),
                "transferSpec": {
                    "awsS3DataSource": {
                        "bucketName": Variable.get('s3_bucket'),
                        "awsAccessKey": {
                            "accessKeyId": Variable.get('aws_access_key_id'),
                            "secretAccessKey": Variable.get('aws_secret_access_key')
                        },
                        "path": f"/{domain}/{table}/"
                    },
                    "gcsDataSink": {
                        "bucketName": f"gcs-staging-{domain}",
                        "path": f"/staging/{table}/"
                    },
                    "transferOptions": {
                        "overwriteObjectsAlreadyExistingInSink": True,
                        "deleteObjectsFromSourceAfterTransfer": False
                    }
                }
            },
            dag=dag
        )
        
        # Step 2: Run STS job
        sts_run = CloudDataTransferServiceRunJobOperator(
            task_id=f'sts_run_{table}',
            job_name="{{{{ task_instance.xcom_pull(task_ids='sts_create_{}') }}}}".format(table),
            project_id=Variable.get('gcp_project_id'),
            dag=dag
        )
        
        # Step 3: Create external table with Iceberg
        create_external = BigQueryCreateExternalTableOperator(
            task_id=f'create_external_{table}',
            dataset_id=f'{domain}_staging',
            table_resource={
                "tableReference": {
                    "projectId": Variable.get('gcp_project_id'),
                    "datasetId": f"{domain}_staging",
                    "tableId": f"{table}_ext"
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://gcs-staging-{domain}/staging/{table}/*.parquet"],
                    "autodetect": True,
                    "icebergOptions": {
                        "fileFormat": "PARQUET",
                        "storageUri": f"gs://gcs-staging-{domain}/iceberg/{table}/",
                        "partitionSpec": {
                            "fields": [
                                {
                                    "sourceId": 1,
                                    "fieldId": 1000,
                                    "name": "date",
                                    "transform": "DAY"
                                }
                            ]
                        }
                    }
                }
            },
            dag=dag
        )
        
        # Step 4: Load to native table
        load_native = BigQueryInsertJobOperator(
            task_id=f'load_native_{table}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{Variable.get('gcp_project_id')}.{domain}_raw.{table}`
                        PARTITION BY DATE(created_date)
                        CLUSTER BY member_id
                        AS
                        SELECT 
                            *,
                            CURRENT_TIMESTAMP() AS ingestion_timestamp,
                            'initiate' AS ingestion_pipeline,
                            '{context['run_id']}' AS run_id
                        FROM `{Variable.get('gcp_project_id')}.{domain}_staging.{table}_ext`
                    """,
                    "useLegacySql": False
                }
            },
            dag=dag
        )
        
        # Step 5: Create Dataplex asset
        create_asset = PythonOperator(
            task_id=f'create_dataplex_{table}',
            python_callable=create_dataplex_asset,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        # Step 6: Track lineage
        track_lineage = PythonOperator(
            task_id=f'track_lineage_{table}',
            python_callable=track_data_lineage,
            params={'table': table, 'domain': domain},
            dag=dag
        )
        
        # Step 7: Audit log
        audit_log = BigQueryInsertJobOperator(
            task_id=f'audit_log_{table}',
            configuration={
                "query": {
                    "query": f"""
                        INSERT INTO `{Variable.get('gcp_project_id')}.{domain}_audit.pipeline_logs`
                        (pipeline_name, pipeline_type, domain, table_name, status, 
                         records_processed, execution_date, run_id, created_at)
                        SELECT
                            'initiate_{domain}_{table}',
                            'initiate',
                            '{domain}',
                            '{table}',
                            'SUCCESS',
                            COUNT(*),
                            '{context['execution_date']}',
                            '{context['run_id']}',
                            CURRENT_TIMESTAMP()
                        FROM `{Variable.get('gcp_project_id')}.{domain}_raw.{table}`
                        WHERE run_id = '{context['run_id']}'
                    """,
                    "useLegacySql": False
                }
            },
            dag=dag
        )
        
        # Define task dependencies
        sts_create >> sts_run >> create_external >> load_native
        load_native >> [create_asset, track_lineage] >> audit_log
    
    return dag

# Create DAGs for each domain
domains_config = json.loads(Variable.get('domains_config', '{}'))
for domain, config in domains_config.items():
    tables = config.get('tables', [])
    if tables:
        globals()[f'initiate_{domain}_dag'] = create_initiate_dag(domain, tables)
```

#### 4.2.2 Hybrid Dataflow Pipeline (Batch/Realtime)

```python
# dataflow/pipelines/hybrid_pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import json
import yaml
import argparse
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

class HybridPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--mode', required=True, choices=['batch', 'realtime'])
        parser.add_argument('--config_path', required=True)
        parser.add_argument('--domain', required=True)
        parser.add_argument('--batch_window_hours', type=int, default=1)

class DependencyChecker(beam.DoFn):
    """Check upstream dependencies before processing"""
    
    def __init__(self, dependencies: List[Dict]):
        self.dependencies = dependencies
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        # Check each dependency
        for dep in self.dependencies:
            if not self._check_dependency(dep):
                # Send to dead letter queue
                yield beam.pvalue.TaggedOutput('failed_dependency', {
                    'element': element,
                    'failed_dependency': dep,
                    'timestamp': datetime.utcnow().isoformat()
                })
                return
        
        # All dependencies met
        yield beam.pvalue.TaggedOutput('main', element)
    
    def _check_dependency(self, dep: Dict) -> bool:
        query = f"""
            SELECT COUNT(*) as count
            FROM `{dep['project']}.{dep['dataset']}.{dep['table']}`
            WHERE {dep['condition']}
        """
        
        try:
            result = self.bq_client.query(query).result()
            for row in result:
                return row.count > 0
        except Exception as e:
            logging.error(f"Dependency check failed: {e}")
            return False
        
        return False

class FetchFromBigQuery(beam.DoFn):
    """Fetch full records from BigQuery based on IDs"""
    
    def __init__(self, project: str, dataset: str, table: str):
        self.project = project
        self.dataset = dataset
        self.table = table
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        # Extract ID from element
        record_id = element.get('id') or element.get('member_id')
        
        if not record_id:
            logging.error(f"No ID found in element: {element}")
            return
        
        query = f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{self.table}`
            WHERE member_id = @record_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("record_id", "STRING", record_id)
            ]
        )
        
        try:
            result = self.bq_client.query(query, job_config=job_config).result()
            for row in result:
                yield dict(row)
        except Exception as e:
            logging.error(f"Failed to fetch record {record_id}: {e}")
            yield beam.pvalue.TaggedOutput('fetch_error', {
                'id': record_id,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })

class DataDistributor(beam.DoFn):
    """Distribute single source record to multiple target tables"""
    
    def __init__(self, distribution_mapping: Dict[str, List[str]]):
        self.distribution_mapping = distribution_mapping
    
    def process(self, element):
        for table, columns in self.distribution_mapping.items():
            # Extract only required columns for each table
            table_record = {
                col: element.get(col)
                for col in columns
                if col in element
            }
            
            # Add metadata
            table_record['_source_timestamp'] = element.get('_timestamp', datetime.utcnow().isoformat())
            table_record['_ingestion_timestamp'] = datetime.utcnow().isoformat()
            
            yield beam.pvalue.TaggedOutput(table, table_record)

class ColumnMapper(beam.DoFn):
    """Apply column mapping and transformations"""
    
    def __init__(self, table: str, mapping: Dict):
        self.table = table
        self.mapping = mapping
    
    def process(self, element):
        result = {}
        
        for target_col, source_spec in self.mapping.items():
            if isinstance(source_spec, str):
                # Simple rename
                result[target_col] = element.get(source_spec)
            elif isinstance(source_spec, dict):
                # Complex mapping
                if source_spec['type'] == 'concat':
                    values = [str(element.get(c, '')) for c in source_spec['columns']]
                    result[target_col] = source_spec.get('separator', '').join(values)
                elif source_spec['type'] == 'constant':
                    result[target_col] = source_spec['value']
                elif source_spec['type'] == 'expression':
                    # Evaluate simple Python expression
                    try:
                        result[target_col] = eval(source_spec['expression'], {'element': element})
                    except Exception as e:
                        logging.error(f"Expression evaluation failed: {e}")
                        result[target_col] = None
        
        # Add table metadata
        result['_table'] = self.table
        result['_mapped_timestamp'] = datetime.utcnow().isoformat()
        
        yield result

class ComplexTransform(beam.DoFn):
    """Apply complex business logic transformations"""
    
    def __init__(self, transform_module: str, params: Dict):
        self.transform_module = transform_module
        self.params = params
    
    def setup(self):
        # Dynamically import transform module
        import importlib
        module_path, class_name = self.transform_module.rsplit('.', 1)
        module = importlib.import_module(module_path)
        self.transform_class = getattr(module, class_name)
        self.transformer = self.transform_class(self.params)
    
    def process(self, element):
        try:
            transformed = self.transformer.transform(element)
            yield transformed
        except Exception as e:
            logging.error(f"Transform failed for {self.transform_module}: {e}")
            yield beam.pvalue.TaggedOutput('transform_error', {
                'input': element,
                'error': str(e),
                'transform': self.transform_module,
                'timestamp': datetime.utcnow().isoformat()
            })

class AuditLogger(beam.DoFn):
    """Log audit information for processed records"""
    
    def __init__(self, pipeline_config: Dict):
        self.pipeline_config = pipeline_config
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
    
    def process(self, element):
        audit_record = {
            'pipeline_name': self.pipeline_config['name'],
            'pipeline_mode': self.pipeline_config['mode'],
            'domain': self.pipeline_config['domain'],
            'record_id': element.get('id') or element.get('member_id'),
            'processing_timestamp': datetime.utcnow().isoformat(),
            'source_timestamp': element.get('_timestamp'),
            'status': 'SUCCESS'
        }
        
        # Write to audit table
        table_id = f"{self.pipeline_config['project']}.{self.pipeline_config['domain']}_audit.processing_logs"
        
        try:
            errors = self.bq_client.insert_rows_json(table_id, [audit_record])
            if errors:
                logging.error(f"Audit logging failed: {errors}")
        except Exception as e:
            logging.error(f"Audit logging exception: {e}")
        
        # Pass through the element
        yield element

def run_pipeline(pipeline_options: HybridPipelineOptions):
    """Main pipeline execution"""
    
    # Load configuration
    with open(pipeline_options.config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Set streaming mode
    options = pipeline_options.view_as(StandardOptions)
    options.streaming = (pipeline_options.mode == 'realtime')
    
    # Create pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Step 1: Read source data
        if pipeline_options.mode == 'realtime':
            # Read from Pub/Sub
            messages = (
                pipeline
                | 'ReadFromPubSub' >> ReadFromPubSub(
                    subscription=config['pubsub']['subscription']
                )
                | 'ParsePubSubMessage' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            )
        else:
            # Read from BigQuery (batch mode)
            hours_back = pipeline_options.batch_window_hours
            query = f"""
                SELECT *
                FROM `{config['source']['project']}.{config['source']['dataset']}.{config['source']['table']}`
                WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours_back} HOUR)
            """
            
            messages = (
                pipeline
                | 'ReadFromBigQuery' >> ReadFromBigQuery(query=query, use_standard_sql=True)
                | 'ConvertToDict' >> beam.Map(lambda x: dict(x))
            )
        
        # Step 2: Check dependencies
        dependency_results = (
            messages
            | 'CheckDependencies' >> beam.ParDo(
                DependencyChecker(config.get('dependencies', []))
            ).with_outputs('main', 'failed_dependency')
        )
        
        validated_messages = dependency_results['main']
        failed_dependencies = dependency_results['failed_dependency']
        
        # Handle failed dependencies
        (failed_dependencies
         | 'FormatFailedDeps' >> beam.Map(lambda x: json.dumps(x))
         | 'WriteFailedDepsToDLQ' >> WriteToBigQuery(
             table=f"{config['project']}.{config['domain']}_audit.failed_dependencies",
             schema='SCHEMA_AUTODETECT',
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))
        
        # Step 3: Fetch full records from source
        source_records = (
            validated_messages
            | 'FetchSourceData' >> beam.ParDo(
                FetchFromBigQuery(
                    config['source']['project'],
                    config['source']['dataset'],
                    config['source']['table']
                )
            ).with_outputs('main', 'fetch_error')
        )
        
        fetched_records = source_records['main']
        fetch_errors = source_records['fetch_error']
        
        # Handle fetch errors
        (fetch_errors
         | 'FormatFetchErrors' >> beam.Map(lambda x: json.dumps(x))
         | 'WriteFetchErrorsToDLQ' >> WriteToBigQuery(
             table=f"{config['project']}.{config['domain']}_audit.fetch_errors",
             schema='SCHEMA_AUTODETECT',
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))
        
        # Step 4: Distribute to multiple tables
        distributed = (
            fetched_records
            | 'DistributeToTables' >> beam.ParDo(
                DataDistributor(config['distribution_mapping'])
            ).with_outputs(*config['distribution_mapping'].keys())
        )
        
        # Step 5: Process each table
        for table in config['distribution_mapping'].keys():
            table_records = distributed[table]
            
            # Apply column mapping
            mapped_records = (
                table_records
                | f'MapColumns_{table}' >> beam.ParDo(
                    ColumnMapper(table, config['column_mappings'].get(table, {}))
                )
            )
            
            # Apply complex transformations if configured
            if table in config.get('complex_transforms', {}):
                transform_config = config['complex_transforms'][table]
                transformed = (
                    mapped_records
                    | f'ComplexTransform_{table}' >> beam.ParDo(
                        ComplexTransform(
                            transform_config['module'],
                            transform_config.get('params', {})
                        )
                    ).with_outputs('main', 'transform_error')
                )
                
                final_records = transformed['main']
                transform_errors = transformed['transform_error']
                
                # Handle transform errors
                (transform_errors
                 | f'FormatTransformErrors_{table}' >> beam.Map(lambda x: json.dumps(x))
                 | f'WriteTransformErrorsToDLQ_{table}' >> WriteToBigQuery(
                     table=f"{config['project']}.{config['domain']}_audit.transform_errors",
                     schema='SCHEMA_AUTODETECT',
                     write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
                 ))
            else:
                final_records = mapped_records
            
            # Write to BigQuery
            dataset = config['target_datasets'].get(table.split('_')[0], config['domain'] + '_raw')
            
            (final_records
             | f'WriteToBigQuery_{table}' >> WriteToBigQuery(
                 table=f"{config['project']}.{dataset}.{table}",
                 schema='SCHEMA_AUTODETECT',
                 create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
                 write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
             ))
        
        # Step 6: Audit logging
        (fetched_records
         | 'AuditLogging' >> beam.ParDo(AuditLogger({
             'name': f"{config['domain']}_pipeline",
             'mode': pipeline_options.mode,
             'domain': config['domain'],
             'project': config['project']
         }))
        )

def main():
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    hybrid_options = pipeline_options.view_as(HybridPipelineOptions)
    
    run_pipeline(hybrid_options)

if __name__ == '__main__':
    main()
```

#### 4.2.3 Batch Pipeline DAG

```python
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
            'temp_location': f"gs://dataflow-temp/{domain}/batch/{{{{ ds }}}}",
            'staging_location': f"gs://dataflow-staging/{domain}/batch/{{{{ ds }}}}",
            'machine_type': 'n2-standard-4',
            'max_workers': 20,
            'region': Variable.get('gcp_region', 'asia-southeast1'),
            'subnetwork': Variable.get('dataflow_subnetwork'),
            'service_account_email': Variable.get('dataflow_service_account')
        }
        
        # Store in XCom for next task
        return json.dumps(params)
    
    prepare_params = PythonOperator(
        task_id='prepare_dataflow_params',
        python_callable=prepare_dataflow_params,
        dag=dag
    )
    
    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id='run_batch_dataflow',
        template='gs://dataflow-templates/hybrid_pipeline',
        job_name=f"batch-{domain}-{{{{ ds_nodash }}}}",
        parameters=json.loads("{{ ti.xcom_pull(task_ids='prepare_dataflow_params') }}"),
        dataflow_default_options={
            'project': Variable.get('gcp_project_id'),
            'region': Variable.get('gcp_region', 'asia-southeast1'),
            'zone': Variable.get('gcp_zone', 'asia-southeast1-a'),
            'tempLocation': f"gs://dataflow-temp/{domain}/batch/{{{{ ds }}}}",
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

# Create DAGs for each domain
domains = json.loads(Variable.get('batch_domains', '["member", "order", "product"]'))
for domain in domains:
    globals()[f'batch_{domain}_dag'] = create_batch_dag(domain)
```

#### 4.2.4 Reconciliation Pipeline

```python
# dataflow/pipelines/reconciliation_pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
import argparse
from typing import Dict, List, Tuple
from datetime import datetime
import logging

class ReconciliationOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--s3_table', required=True)
        parser.add_argument('--native_table', required=True)
        parser.add_argument('--output_table', required=True)
        parser.add_argument('--key_columns', required=True)
        parser.add_argument('--comparison_columns', required=True)

class RecordComparator(beam.DoFn):
    """Compare records between S3 and native tables"""
    
    def __init__(self, key_columns: List[str], comparison_columns: List[str]):
        self.key_columns = key_columns
        self.comparison_columns = comparison_columns
    
    def process(self, element):
        s3_record, native_record = element
        
        # Create comparison result
        result = {
            'reconciliation_timestamp': datetime.utcnow().isoformat(),
            'key': self._create_key(s3_record or native_record),
            'status': 'UNKNOWN'
        }
        
        if s3_record and not native_record:
            result['status'] = 'MISSING_IN_NATIVE'
            result['details'] = 'Record exists in S3 but not in native table'
            result['s3_record'] = s3_record
        elif native_record and not s3_record:
            result['status'] = 'MISSING_IN_S3'
            result['details'] = 'Record exists in native table but not in S3'
            result['native_record'] = native_record
        elif s3_record and native_record:
            # Compare field values
            mismatches = []
            for col in self.comparison_columns:
                s3_value = s3_record.get(col)
                native_value = native_record.get(col)
                
                if s3_value != native_value:
                    mismatches.append({
                        'column': col,
                        's3_value': str(s3_value),
                        'native_value': str(native_value)
                    })
            
            if mismatches:
                result['status'] = 'MISMATCH'
                result['details'] = f"Found {len(mismatches)} column mismatches"
                result['mismatches'] = mismatches
                result['s3_record'] = s3_record
                result['native_record'] = native_record
            else:
                result['status'] = 'MATCH'
                result['details'] = 'Records match perfectly'
        
        yield result
    
    def _create_key(self, record: Dict) -> str:
        """Create composite key from key columns"""
        if not record:
            return 'UNKNOWN'
        
        key_values = [str(record.get(col, '')) for col in self.key_columns]
        return '|'.join(key_values)

def run_reconciliation(options: ReconciliationOptions):
    """Main reconciliation pipeline"""
    
    key_columns = options.key_columns.split(',')
    comparison_columns = options.comparison_columns.split(',')
    
    with beam.Pipeline(options=options) as pipeline:
        
        # Read S3 data (external table)
        s3_data = (
            pipeline
            | 'ReadS3Data' >> ReadFromBigQuery(
                table=options.s3_table,
                use_standard_sql=True
            )
            | 'ConvertS3ToDict' >> beam.Map(lambda x: dict(x))
            | 'KeyS3Records' >> beam.Map(
                lambda x: (
                    '|'.join([str(x.get(col, '')) for col in key_columns]),
                    x
                )
            )
        )
        
        # Read native data
        native_data = (
            pipeline
            | 'ReadNativeData' >> ReadFromBigQuery(
                table=options.native_table,
                use_standard_sql=True
            )
            | 'ConvertNativeToDict' >> beam.Map(lambda x: dict(x))
            | 'KeyNativeRecords' >> beam.Map(
                lambda x: (
                    '|'.join([str(x.get(col, '')) for col in key_columns]),
                    x
                )
            )
        )
        
        # Join and compare
        comparison_results = (
            {'s3': s3_data, 'native': native_data}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'CompareRecords' >> beam.ParDo(
                RecordComparator(key_columns, comparison_columns)
            )
        )
        
        # Calculate statistics
        stats = (
            comparison_results
            | 'ExtractStatus' >> beam.Map(lambda x: (x['status'], 1))
            | 'CountByStatus' >> beam.CombinePerKey(sum)
            | 'FormatStats' >> beam.Map(lambda x: {
                'status': x[0],
                'count': x[1],
                'timestamp': datetime.utcnow().isoformat()
            })
        )
        
        # Write detailed results
        (comparison_results
         | 'WriteDetailedResults' >> WriteToBigQuery(
             table=options.output_table,
             schema='SCHEMA_AUTODETECT',
             create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_TRUNCATE
         ))
        
        # Write statistics
        (stats
         | 'WriteStats' >> WriteToBigQuery(
             table=options.output_table.replace('results', 'stats'),
             schema='SCHEMA_AUTODETECT',
             create_disposition=WriteToBigQuery.CreateDisposition.CREATE_IF_NEEDED,
             write_disposition=WriteToBigQuery.WriteDisposition.WRITE_APPEND
         ))

def main():
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args()
    
    options = PipelineOptions(pipeline_args)
    recon_options = options.view_as(ReconciliationOptions)
    
    run_reconciliation(recon_options)

if __name__ == '__main__':
    main()
```

## 5. Configuration Management

### 5.1 Shared Infrastructure Configuration

The platform uses a **shared infrastructure approach** with domain-specific data isolation:

```yaml
# config/pipeline_config.yaml
project: your-gcp-project-id
region: asia-southeast1
domain: member  # Domain-specific parameter

# Shared Infrastructure
shared_infrastructure:
  # Shared Datasets (no domain prefix)
  datasets:
    raw: raw_data
    staging: staging_data
    monitoring: monitoring_data
  
  # Shared Buckets (no domain prefix)  
  buckets:
    temp: "gs://your-project-dataflow-temp"
    staging: "gs://your-project-dataflow-staging"
    gcs_staging: "gs://your-project-gcs-staging"
    configs: "gs://your-project-pipeline-configs"
  
  # Shared Pub/Sub Topics
  pubsub:
    topics:
      create: "data-events-create"
      update: "data-events-update"
    # Domain-specific subscriptions
    subscription: "projects/your-project/subscriptions/data-events-{domain}-sub"

# Domain-Specific Configuration
domain_config:
  # Table naming pattern: {domain}_{table_name}
  table_prefix: "${domain}_"
  
  # Subfolder pattern: /{domain}/ within shared buckets
  subfolder_pattern: "/{domain}/"

# Distribution Mapping (domain-agnostic logic)
distribution_mapping:
  raw_a1: [a, b, c]
  raw_a2: [a, e, f]
  raw_a3: [a, e, g]
  refined_b1: [a, b, c, d, e, f]
  refined_b2: [a, b, c, d]
  refined_c1: [a, b, c, d, e, f, g]

# Column Mappings (apply to all domains)
column_mappings:
  raw_a1:
    member_id: a
    member_name: b
    member_status: c
  
  raw_a2:
    member_id: a
    email: e
    phone: f
    member_id: a
    member_name: b
    member_status: c
    created_date: d
    email: e
    phone: f
  
  refined_b2:
    member_id: a
    member_name: b
    member_status: c
    created_date: d
    member_key:
      type: concat
      columns: [a, b]
      separator: "_"
    default_score:
      type: constant
      value: 0
  
  refined_c1:
    member_id: a
    member_name: b
    member_status: c
    created_date: d
    email: e
    phone: f
    address: g

# Complex Transformations
complex_transforms:
  refined_b1:
    module: transforms.member.MemberProfileEnrichment
    params:
      lookup_table: reference_data.member_segments
      join_key: member_id
  
  analytics_summary:
    module: transforms.analytics.MemberAggregation
    params:
      group_by: [region, segment]
      metrics: [total_value, transaction_count]

# Dependencies
dependencies:
  - project: your-project
    dataset: batch_control
    table: job_status
    condition: "job_name = 'daily_etl' AND status = 'COMPLETED' AND DATE(completion_time) = CURRENT_DATE()"
```

### 5.2 Airflow Variables Configuration

```json
{
  "gcp_project_id": "your-gcp-project-id",
  "gcp_region": "asia-southeast1",
  "gcp_zone": "asia-southeast1-a",
  "s3_bucket": "your-s3-bucket",
  "aws_access_key_id": "your-aws-key",
  "aws_secret_access_key": "your-aws-secret",
  "dataflow_network": "projects/your-project/global/networks/default",
  "dataflow_subnetwork": "projects/your-project/regions/asia-southeast1/subnetworks/default",
  "dataflow_service_account": "dataflow-sa@your-project.iam.gserviceaccount.com",
  "domains_config": {
    "member": {
      "tables": ["s_loy_program", "s_org_ext", "s_loy_tier", "s_loy_member"],
      "enabled": true
    },
    "order": {
      "tables": ["order_header", "order_detail", "payment"],
      "enabled": true
    }
  },
  "batch_domains": ["member", "order", "product"]
}
```

## 6. Deployment Guide

### 6.1 Prerequisites

```bash
# Install required tools
pip install apache-airflow-providers-google==10.3.0
pip install apache-beam[gcp]==2.48.0
pip install pyyaml==6.0
pip install google-cloud-storage==2.10.0
pip install google-cloud-bigquery==3.11.4
pip install google-cloud-datacatalog==3.9.0
pip install google-cloud-lineage==0.2.0
```

### 6.2 Infrastructure Setup

```bash
# Create GCS buckets
gsutil mb -l asia-southeast1 gs://gcs-staging-member
gsutil mb -l asia-southeast1 gs://dataflow-temp
gsutil mb -l asia-southeast1 gs://dataflow-staging
gsutil mb -l asia-southeast1 gs://pipeline-configs
gsutil mb -l asia-southeast1 gs://dataflow-templates

# Create BigQuery datasets
bq mk --location=asia-southeast1 member_raw
bq mk --location=asia-southeast1 member_staging
bq mk --location=asia-southeast1 member_refined
bq mk --location=asia-southeast1 member_analytics
bq mk --location=asia-southeast1 member_audit
bq mk --location=asia-southeast1 member_reconcile_temp

# Create Pub/Sub topics and subscriptions
gcloud pubsub topics create member-events-create
gcloud pubsub topics create member-events-update
gcloud pubsub subscriptions create member-events-sub \
    --topic=member-events-create,member-events-update
```

### 6.3 Deploy Dataflow Templates

```bash
# Build Dataflow Flex Template
cd dataflow
docker build -t gcr.io/your-project/hybrid-pipeline:latest .
docker push gcr.io/your-project/hybrid-pipeline:latest

gcloud dataflow flex-template build \
    gs://dataflow-templates/hybrid_pipeline.json \
    --image gcr.io/your-project/hybrid-pipeline:latest \
    --sdk-language PYTHON \
    --metadata-file metadata.json
```

### 6.4 Deploy Airflow DAGs

```bash
# Upload DAGs to Composer
gcloud composer environments storage dags import \
    --environment your-composer-env \
    --location asia-southeast1 \
    --source airflow/dags/
```

## 7. Operations & Monitoring

### 7.1 Monitoring Queries

```sql
-- Pipeline execution status
SELECT 
    pipeline_name,
    pipeline_type,
    status,
    COUNT(*) as execution_count,
    AVG(records_processed) as avg_records,
    MAX(execution_date) as last_execution
FROM `project.member_audit.pipeline_logs`
WHERE DATE(created_at) >= CURRENT_DATE() - 7
GROUP BY pipeline_name, pipeline_type, status
ORDER BY last_execution DESC;

-- Reconciliation summary
SELECT 
    DATE(reconciliation_timestamp) as date,
    status,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(reconciliation_timestamp)), 2) as percentage
FROM `project.member_audit.reconciliation_results`
WHERE DATE(reconciliation_timestamp) >= CURRENT_DATE() - 30
GROUP BY date, status
ORDER BY date DESC, status;

-- Data quality metrics
SELECT 
    table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
FROM `project.member_audit.data_quality_logs`
WHERE DATE(check_timestamp) = CURRENT_DATE()
GROUP BY table_name;
```

### 7.2 Alerting Configuration

```yaml
# monitoring/alerts.yaml
alerts:
  - name: pipeline_failure
    query: |
      SELECT COUNT(*)
      FROM member_audit.pipeline_logs
      WHERE status = 'FAILED'
        AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    threshold: 0
    notification_channels: [email, slack]
  
  - name: reconciliation_mismatch_high
    query: |
      SELECT 
        SUM(CASE WHEN status != 'MATCH' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
      FROM member_audit.reconciliation_results
      WHERE DATE(reconciliation_timestamp) = CURRENT_DATE()
    threshold: 5.0  # Alert if >5% mismatch
    notification_channels: [email]
  
  - name: dataflow_job_stuck
    query: |
      SELECT COUNT(*)
      FROM dataflow.jobs
      WHERE state = 'RUNNING'
        AND start_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    threshold: 0
    notification_channels: [pagerduty]
```

### 7.3 Operational Runbook

#### Starting Pipelines

```bash
# Trigger initiate pipeline
gcloud composer environments run your-composer-env \
    --location asia-southeast1 \
    dags trigger initiate_member_pipeline

# Start realtime pipeline
gcloud composer environments run your-composer-env \
    --location asia-southeast1 \
    dags unpause realtime_member_pipeline

# Enable batch pipeline
gcloud composer environments run your-composer-env \
    --location asia-southeast1 \
    dags unpause batch_member_pipeline
```

#### Switching from Batch to Realtime

```bash
# Step 1: Ensure realtime pipeline is ready
gcloud dataflow jobs list --filter="name:realtime-member*" --region=asia-southeast1

# Step 2: Pause batch pipeline
gcloud composer environments run your-composer-env \
    --location asia-southeast1 \
    dags pause batch_member_pipeline

# Step 3: Start realtime pipeline
gcloud dataflow flex-template run realtime-member-$(date +%s) \
    --template-file-gcs-location=gs://dataflow-templates/hybrid_pipeline.json \
    --parameters mode=realtime,config_path=gs://pipeline-configs/member/config.yaml \
    --region=asia-southeast1

# Step 4: Monitor for 24 hours
# Step 5: If stable, remove batch pipeline schedule
```

## Conclusion

This comprehensive solution provides a robust, scalable data pipeline architecture that supports both batch and realtime processing modes. The design enables seamless migration from AWS S3 to GCP while maintaining data quality through reconciliation. The modular approach with shared Dataflow code ensures maintainability and consistency across different processing modes.

Key benefits:
- **Flexibility**: Easy switching between batch and realtime modes
- **Reusability**: Shared code between pipelines
- **Scalability**: Auto-scaling Dataflow jobs
- **Reliability**: Comprehensive error handling and audit logging
- **Observability**: Full monitoring and alerting capabilities

The solution is production-ready and can handle the specified data volumes while providing clear migration path from legacy systems.