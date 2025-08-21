# GCP Data Platform - Comprehensive Data Pipeline Solution

## Overview

This project implements a comprehensive data pipeline solution for migrating from AWS S3 batch processing to GCP hybrid batch/realtime system. The solution consists of four distinct pipelines orchestrated by Cloud Composer (Airflow), utilizing both pure Airflow operations and Python-based Dataflow jobs.

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         CLOUD COMPOSER (ORCHESTRATION)                      │
├──────────────┬──────────────────┬──────────────────┬─────────────────────┤
│   Initiate   │    Realtime      │     Batch        │    Reconcile        │
│   Pipeline   │    Pipeline      │    Pipeline      │    Pipeline         │
│ (One-time)   │  (Continuous)    │   (Hourly)       │    (Daily)          │
└──────────────┴──────────────────┴──────────────────┴─────────────────────┘
```

## Key Features

- **Hybrid Processing**: Supports both batch and realtime data processing
- **Flexible Configuration**: YAML-based configuration with environment overrides
- **Data Quality**: Built-in validation and quality scoring
- **Comprehensive Auditing**: Full data lineage and processing metrics
- **Error Handling**: Robust error handling with dead letter queues
- **Windowing Support**: Advanced windowing for streaming data
- **Secret Management**: Secure credential storage using Google Secret Manager
- **Data Lineage**: Automated data lineage tracking with Dataplex
- **Monitoring**: Comprehensive monitoring and alerting

## Project Structure

```
gcp-data-pipeline/
├── airflow/                    # Airflow DAGs and configurations
│   ├── dags/                  # Pipeline DAGs
│   ├── plugins/               # Custom operators and sensors
│   └── config/                # Airflow variables and configurations
├── dataflow/                  # Dataflow pipeline code
│   ├── pipelines/             # Main pipeline implementations
│   ├── transforms/            # Data transformation modules
│   └── utils/                 # Utility functions
├── config/                    # Pipeline configurations
├── terraform/                 # Infrastructure as code
├── monitoring/                # Monitoring and alerting configurations
├── scripts/                   # Setup and utility scripts
└── docs/                      # Documentation
```

## Prerequisites

- Google Cloud Platform account with billing enabled
- Terraform >= 1.0
- Docker
- gcloud CLI
- Python 3.8+

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd gcp-data-pipeline
   ```

2. **Set environment variables**
   ```bash
   export PROJECT_ID="your-gcp-project-id"
   export REGION="asia-southeast1"
   export ENVIRONMENT="dev"
   export DOMAIN="member"
   ```

3. **Run setup script**
   ```bash
   chmod +x scripts/setup.sh
   ./scripts/setup.sh
   ```

4. **Deploy infrastructure**
   ```bash
   cd terraform
   terraform init
   terraform plan -var="project_id=$PROJECT_ID"
   terraform apply -var="project_id=$PROJECT_ID"
   ```

## Pipeline Types

### 1. Initiate Pipeline
- **Purpose**: One-time migration of historical data from S3 to BigQuery
- **Technology**: Pure Airflow (no Dataflow)
- **Trigger**: Manual
- **Steps**: STS → GCS → External Tables → Native BigQuery → Audit

### 2. Realtime Pipeline
- **Purpose**: Continuous processing of streaming events
- **Technology**: Airflow + Dataflow Streaming
- **Trigger**: Continuous
- **Source**: Pub/Sub topics

### 3. Batch Pipeline
- **Purpose**: Hourly batch processing (temporary until realtime is ready)
- **Technology**: Airflow + Dataflow Batch
- **Trigger**: Hourly schedule
- **Source**: BigQuery
- **Windowing**: Optional windowing for large dataset processing

### 4. Reconciliation Pipeline
- **Purpose**: Daily validation against AWS S3 reference data
- **Technology**: Airflow + Dataflow
- **Trigger**: Daily schedule
- **Output**: Comparison reports

## Windowing Configuration

### Overview
Enhanced windowing support allows for flexible data processing patterns across both batch and realtime modes. Windows help manage data flow, handle late arrivals, and optimize throughput.

### Configuration (`config/pipeline_config.yaml`)
```yaml
windowing:
  enabled: true  # Global windowing toggle
  
  # Default window configuration
  default:
    type: fixed  # fixed, sliding, session
    duration_seconds: 30
    allowed_lateness_seconds: 60
    accumulation_mode: discarding  # discarding, accumulating
    trigger:
      type: after_watermark
      early_firings: 1
      late_firings: 1
  
  # Step-specific configurations
  steps:
    message_ingestion:
      enabled: true
      type: fixed
      duration_seconds: 10
      accumulation_mode: accumulating
      
    dependency_check:
      enabled: true  
      type: fixed
      duration_seconds: 30
      allowed_lateness_seconds: 120
      
    fetch_source:
      enabled: true
      type: sliding
      duration_seconds: 60
      period_seconds: 30
      
    distribution:
      enabled: true
      type: fixed
      duration_seconds: 30
      
    transformation:
      enabled: true
      type: session
      gap_duration_seconds: 10
      
    aggregation:
      enabled: true
      type: fixed
      duration_seconds: 60
      trigger:
        type: repeatedly
        after_count: 100
  
  # Batch mode windowing
  batch_windowing:
    enabled: true
    type: fixed
    duration_seconds: 300  # 5 minute windows
    max_elements: 10000   # Process in chunks
```

### Window Types

1. **Fixed Windows**: Process data in fixed time intervals
   - Best for: Regular batch processing, consistent aggregations
   - Example: 30-second windows for realtime ingestion

2. **Sliding Windows**: Overlapping time windows
   - Best for: Moving averages, trend analysis
   - Example: 60-second windows sliding every 30 seconds

3. **Session Windows**: Dynamic windows based on data gaps
   - Best for: User session analysis, activity-based grouping
   - Example: 10-second gap for session detection

### Windowing Benefits

- **Late Data Handling**: Configure allowed lateness for out-of-order data
- **Throughput Control**: Manage processing load with window sizing
- **Memory Management**: Batch mode windowing prevents memory overflow

## Secret Management

This project uses Google Cloud Secret Manager to securely store and manage credentials and sensitive configuration data.

### Configured Secrets

1. **AWS S3 Credentials** (for initiate and reconciliation pipelines)
   - `aws-s3-access-key-id`: AWS access key ID
   - `aws-s3-secret-access-key`: AWS secret access key
   - `aws-s3-bucket-name`: Source S3 bucket name

2. **BigQuery Service Account** (for external table access)
   - `bq-service-account-key`: Service account JSON key

### Secret Usage in Pipelines

**Initiate Pipeline:**
```python
# Automatically retrieves S3 credentials from Secret Manager
secrets = get_secrets_from_config(config, project_id)
s3_creds = secrets['s3_credentials']

# Uses credentials for Storage Transfer Service
sts_config = {
    'aws_access_key': s3_creds['aws_access_key_id'],
    'aws_secret_key': s3_creds['aws_secret_access_key'],
    'bucket_name': s3_creds['s3_bucket_name']
}
```

**Reconciliation Pipeline:**
```python
# Uses same Secret Manager integration
secrets = get_secrets_from_config(config, project_id)
# Credentials automatically injected into Dataflow job
```

### Creating Secrets

```bash
# Create S3 credentials
gcloud secrets create aws-s3-access-key-id --data-file=<path-to-key-file>
gcloud secrets create aws-s3-secret-access-key --data-file=<path-to-secret-file>
gcloud secrets create aws-s3-bucket-name --data-file=<path-to-bucket-file>

# Create BigQuery service account key
gcloud secrets create bq-service-account-key --data-file=service-account.json
```

## Data Lineage with Dataplex

This project uses Google Cloud Dataplex for comprehensive data lineage tracking and data discovery across all pipelines.

### Dataplex Infrastructure

**Data Lake Structure:**
```
{domain}-data-lake/
├── {domain}-raw-zone/          # Raw data from sources
│   ├── {domain}_raw (BigQuery)
│   └── gcs-staging-{domain} (GCS)
├── {domain}-refined-zone/      # Processed/transformed data
│   └── {domain}_refined (BigQuery)
└── {domain}-analytics-zone/    # Analytics-ready data
    └── {domain}_analytics (BigQuery)
```

**Asset Types:**
- **BigQuery Datasets**: Native tables in raw, refined, analytics layers
- **GCS Buckets**: Staging area for external table data
- **External Tables**: S3 data accessed via BigQuery external tables

### Automated Lineage Tracking

**Initiate Pipeline:**
- Tracks S3 → GCS → BigQuery External → BigQuery Native lineage
- One-time setup during initial migration
- Full end-to-end data movement tracking

**Realtime/Batch Pipelines:**
- Automatic lineage tracking for each processed record
- Source → Target table relationships
- Transformation step tracking
- Window-level lineage for streaming data

**Lineage Creation:**
```python
# Automatically tracks lineage for each target table
pipeline_info = {
    'name': f"{domain}_{pipeline_mode}_pipeline",
    'type': pipeline_mode,  # 'realtime', 'batch', 'initiate'
    'domain': domain
}

dataplex_manager.track_pipeline_lineage(
    pipeline_info, source_info, target_info
)
```

### Dataplex Benefits

1. **Automatic Discovery**: Assets are automatically discovered and cataloged
2. **Data Governance**: Central view of all data assets and their relationships
3. **Compliance**: Audit trail for data movement and transformations
4. **Impact Analysis**: Understanding downstream effects of data changes
5. **Data Quality**: Integration with data quality monitoring

### Setup Status

✅ **Dataplex Infrastructure Enabled**
- Data lakes, zones, and assets are automatically created via Terraform
- No manual Dataplex configuration required
- Lineage tracking is enabled for all pipeline modes
- Assets are automatically registered during pipeline execution

**Note**: Dataplex infrastructure is created once during initial deployment and reused across all pipeline executions. No additional lineage tracking steps are required in individual pipeline runs.
- **Monitoring**: Window-level metrics and alerting
- **Watermark Management**: Automatic progress tracking

## Configuration

### Pipeline Configuration (`config/pipeline_config.yaml`)

The main configuration file supports:
- Source and target specifications
- Column mapping and transformations
- Data validation rules
- Error handling settings
- Environment-specific overrides
- **Windowing configuration** (new)

Example:
```yaml
project: your-gcp-project-id
domain: member
source:
  project: source-project
  dataset: refined_data
  table: personas
distribution_mapping:
  raw_a1: [a, b, c]
  refined_b1: [a, b, c, d, e, f]
windowing:
  enabled: true
  default:
    type: fixed
    duration_seconds: 30
```

### Airflow Variables (`airflow/config/airflow_variables.json`)

Contains environment-specific settings:
- GCP project and region settings
- Service account configurations
- **Windowing control variables** (new)
- Storage bucket locations
- Domain configurations

## Data Transformations

### Simple Transformations
- Column renaming
- Data type conversions
- Constant value assignment
- String concatenation

### Complex Transformations
- Custom business logic modules
- External API enrichment
- Data quality validation
- Aggregation and windowing

Example transformation configuration:
```yaml
complex_transforms:
  refined_b1:
    module: transforms.member.MemberProfileEnrichment
    params:
      lookup_table: reference_data.member_segments
      join_key: member_id
```

## Windowing Configuration

Enhanced windowing support for realtime processing:

```yaml
windowing:
  enabled: true
  default:
    type: fixed
    duration_seconds: 30
  steps:
    message_ingestion:
      type: fixed
      duration_seconds: 10
    aggregation:
      type: sliding
      duration_seconds: 60
      period_seconds: 30
```

## Monitoring and Alerting

### Built-in Metrics
- Pipeline success rates
- Data volume trends
- Error distribution
- Data quality scores
- Processing latency
- **Window processing metrics** (new)
- **Window latency analysis** (new)
- **Throughput per window** (new)

### Window-Specific Monitoring

#### Windowing Views (`monitoring/windowing_monitoring.sql`)
- `window_processing_stats`: Window processing statistics and trends
- `window_latency_analysis`: Latency analysis with percentiles
- `window_throughput_analysis`: Throughput and efficiency metrics
- `window_error_analysis`: Window-related error tracking
- `windowing_alerts`: Real-time alerting for window issues

#### Key Windowing Metrics
```sql
-- Window latency monitoring
SELECT 
  pipeline_name,
  AVG(latency_seconds) as avg_latency,
  PERCENTILE_CONT(latency_seconds, 0.95) as p95_latency
FROM window_summaries
WHERE processing_date = CURRENT_DATE()
GROUP BY pipeline_name;

-- Window throughput analysis
SELECT 
  hour_of_day,
  SUM(record_count) as total_records,
  COUNT(DISTINCT window_start) as window_count,
  SUM(record_count) / COUNT(DISTINCT window_start) as avg_records_per_window
FROM window_summaries
WHERE processing_date = CURRENT_DATE()
GROUP BY hour_of_day;
```

### Alert Configuration (`monitoring/alerts.yaml`)

New windowing-specific alerts:
```yaml
alerts:
  - name: window_high_latency
    query: "Check for windows with >5min latency"
    threshold: 5
    notification_channels: [email, slack]
    
  - name: empty_windows
    query: "Alert on windows with zero records"
    threshold: 10
    notification_channels: [email, slack]
    
  - name: window_processing_stopped
    query: "Alert when no windows processed for 20+ minutes"
    threshold: 20
    notification_channels: [pagerduty, slack]
```

## Data Quality

### Validation Rules
- Required field checks
- Format validation
- Range validation
- Custom business rules

### Quality Scoring
- Completeness checks
- Format consistency
- Outlier detection
- Custom quality metrics

## Security

- Service account-based authentication
- VPC networking for secure communication
- KMS encryption for sensitive data
- IAM roles and permissions
- Private Google Access

## Operations

### Starting Pipelines with Windowing

```bash
# Trigger initiate pipeline
gcloud composer environments run composer-env \
    --location asia-southeast1 \
    dags trigger initiate_member_pipeline

# Start realtime pipeline with windowing enabled
gcloud composer environments run composer-env \
    --location asia-southeast1 \
    dags unpause realtime_member_pipeline

# Control windowing via Airflow variables
gcloud composer environments run composer-env \
    --location asia-southeast1 \
    variables set member_windowing_enabled true

# Start batch pipeline with windowing for large datasets
python -m dataflow.pipelines.hybrid_pipeline \
    --mode=batch \
    --config_path=gs://bucket/config.yaml \
    --enable_windowing=true \
    --batch_window_hours=24 \
    --runner=DataflowRunner
```

### Windowing Control Commands

```bash
# Enable/disable windowing per domain
airflow variables set member_windowing_enabled true
airflow variables set order_windowing_enabled false

# Check window processing status
bq query --use_legacy_sql=false \
  "SELECT * FROM \`project.member_audit.windowing_alerts\` 
   WHERE generated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"

# Monitor window latency
bq query --use_legacy_sql=false \
  "SELECT pipeline_name, avg_window_latency_seconds, p95_latency_seconds 
   FROM \`project.member_audit.window_latency_analysis\` 
   WHERE processing_date = CURRENT_DATE() 
   ORDER BY avg_window_latency_seconds DESC"
```

### Monitoring

Access monitoring dashboards through:
- Cloud Console Dataflow page
- Cloud Monitoring dashboards
- BigQuery audit tables (including new windowing views)
- Custom Grafana dashboards
- **Window-specific metrics** (new)

### Troubleshooting

Common issues and solutions:

1. **Pipeline failures**: Check audit tables for error details
2. **Data quality issues**: Review validation logs
3. **Window latency issues**: Check `windowing_alerts` view for high latency windows
4. **Empty windows**: Verify upstream data availability and dependency checks
5. **Window processing stopped**: Check pipeline health and resource availability
3. **Performance problems**: Monitor resource usage and adjust scaling
4. **Network issues**: Verify VPC and firewall configurations

## Migration Strategy

### Phase 1: Infrastructure Setup
1. Deploy GCP infrastructure
2. Set up networking and security
3. Configure monitoring

### Phase 2: Initial Data Migration
1. Run initiate pipeline for historical data
2. Validate data completeness and quality
3. Set up reconciliation

### Phase 3: Hybrid Processing
1. Enable batch pipeline for regular processing
2. Gradually transition to realtime
3. Monitor and optimize

### Phase 4: Full Realtime
1. Switch to realtime pipeline
2. Disable batch pipeline
3. Continuous monitoring and optimization

## Performance Tuning

### Dataflow Optimization
- Worker scaling configuration
- Machine type selection
- Disk and memory optimization
- Network configuration

### BigQuery Optimization
- Table partitioning and clustering
- Query optimization
- Slot management
- Cost optimization

## Troubleshooting Guide

### Common Issues

1. **Authentication Errors**
   - Verify service account permissions
   - Check IAM roles
   - Validate key configurations

2. **Network Connectivity**
   - Check VPC configuration
   - Verify firewall rules
   - Test private Google access

3. **Data Quality Issues**
   - Review validation rules
   - Check source data quality
   - Analyze error logs

4. **Performance Problems**
   - Monitor resource usage
   - Adjust worker scaling
   - Optimize queries

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests and documentation
5. Submit a pull request

## Support

For support and questions:
- Create an issue in the repository
- Contact the data platform team
- Check the troubleshooting guide

## License

This project is licensed under the MIT License - see the LICENSE file for details.