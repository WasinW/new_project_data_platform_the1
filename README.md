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

### 4. Reconciliation Pipeline
- **Purpose**: Daily validation against AWS S3 reference data
- **Technology**: Airflow + Dataflow
- **Trigger**: Daily schedule
- **Output**: Comparison reports

## Configuration

### Pipeline Configuration (`config/pipeline_config.yaml`)

The main configuration file supports:
- Source and target specifications
- Column mapping and transformations
- Data validation rules
- Error handling settings
- Environment-specific overrides

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
```

### Airflow Variables (`airflow/config/airflow_variables.json`)

Contains environment-specific settings:
- GCP project and region settings
- Service account configurations
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

### Alert Configuration (`monitoring/alerts.yaml`)
```yaml
alerts:
  - name: pipeline_failure
    query: "SELECT COUNT(*) FROM audit.processing_logs WHERE status = 'FAILED'"
    threshold: 0
    notification_channels: [email, slack]
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

### Starting Pipelines

```bash
# Trigger initiate pipeline
gcloud composer environments run composer-env \
    --location asia-southeast1 \
    dags trigger initiate_member_pipeline

# Start realtime pipeline
gcloud composer environments run composer-env \
    --location asia-southeast1 \
    dags unpause realtime_member_pipeline
```

### Monitoring

Access monitoring dashboards through:
- Cloud Console Dataflow page
- Cloud Monitoring dashboards
- BigQuery audit tables
- Custom Grafana dashboards

### Troubleshooting

Common issues and solutions:

1. **Pipeline failures**: Check audit tables for error details
2. **Data quality issues**: Review validation logs
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