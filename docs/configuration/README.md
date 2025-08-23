# Shared Infrastructure Configuration Guide

## 📖 Overview
This guide explains how to configure the **shared infrastructure data platform** using centralized parameters. The platform uses **shared GCP resources** with **domain-specific data isolation** through table prefixes and subfolders.

---

## �️ Shared Infrastructure Philosophy

### **Shared Resources** (Cost-Optimized)
- **Single datasets**: `raw_data`, `staging_data`, `monitoring_data`
- **Single buckets**: `{project}-dataflow-temp`, `{project}-gcs-staging`
- **Single Composer**: `composer-{environment}`
- **Shared topics**: `data-events-create`, `data-events-update`

### **Domain Isolation** (Data Separation)
- **Table prefixes**: `{domain}_table_name`
- **Subfolders**: `/{domain}/` within shared buckets
- **Domain subscriptions**: `data-events-{domain}-sub`
- **Domain DAGs**: `{pipeline_type}_{domain}_pipeline`

### Parameter Naming Convention
```bash
# Shared Infrastructure (no domain prefix)
PROJECT_ID="your-project-id"
BIGQUERY_DATASET_RAW="raw_data"
STORAGE_BUCKET_STAGING="your-project-gcs-staging"
PUBSUB_TOPIC_EVENTS="data-events-create"

# Domain-Specific Elements
DOMAIN="member"
TABLE_PREFIX="${DOMAIN}_"
SUBFOLDER_PATTERN="/${DOMAIN}/"
```

---

## 🔧 Configuration Process

### 1. **Setup Shared Infrastructure Parameters**
```bash
# Copy and edit shared parameters
cp docs/configuration/parameters.conf.example docs/configuration/parameters.conf

# Configure shared resources
PROJECT_ID="your-gcp-project-id"
REGION="asia-southeast1"

# Shared datasets (no domain prefix)
BIGQUERY_DATASET_RAW="raw_data"
BIGQUERY_DATASET_STAGING="staging_data"  
BIGQUERY_DATASET_MONITORING="monitoring_data"

# Shared buckets (no domain prefix)
STORAGE_BUCKET_DATAFLOW_TEMP="${PROJECT_ID}-dataflow-temp"
STORAGE_BUCKET_GCS_STAGING="${PROJECT_ID}-gcs-staging"
STORAGE_BUCKET_CONFIGS="${PROJECT_ID}-pipeline-configs"

# Shared Composer (no domain prefix)
COMPOSER_ENVIRONMENT_NAME="composer-${ENVIRONMENT}"
```

### 2. **Configure Domain-Specific Elements**
```bash
# Domain configuration
SUPPORTED_DOMAINS="member,order,product"
DOMAIN="member"  # Current domain being configured

# Domain-specific naming patterns
TABLE_PREFIX="${DOMAIN}_"
SUBFOLDER_PATTERN="/${DOMAIN}/"

# Domain-specific subscriptions
PUBSUB_SUBSCRIPTION_PATTERN="data-events-${DOMAIN}-sub"
```
DATA_LAKE_BUCKET_NAME="your-project-data-lake"
RAW_DATASET_ID="raw_data"
INPUT_TOPIC_ID="input-data-topic"
```

### 3. **Run Parameter Replacement**
```bash
# Use your local parameters file
export PARAMS_FILE="docs/configuration/parameters.local.conf"
bash scripts/replace_parameters.sh
```

### 4. **Validate Configuration**
```bash
# Check for unreplaced parameters
grep -r '\${[A-Z_][A-Z0-9_]*}' . --include="*.py" --include="*.yaml" --include="*.tf"

# Validate Terraform configuration
cd terraform
terraform validate
```

---

## 📊 Parameter Categories

### **Core Project Settings**
```bash
# Basic project information
PROJECT_ID="your-gcp-project-id"           # GCP project ID
PROJECT_NAME="Your Data Platform"          # Human-readable name
PROJECT_NUMBER="123456789012"               # GCP project number
ORGANIZATION_ID="your-organization-id"     # GCP organization ID
DOMAIN="your-company.com"                   # Company domain
```

### **Regional Configuration**
```bash
# Primary regions
REGION="us-central1"                       # Main GCP region
ZONE="us-central1-a"                       # Specific zone
BIGQUERY_REGION="US"                       # BigQuery location
STORAGE_REGION="us-central1"               # Storage region
BACKUP_REGION="us-east1"                   # Backup region (different from primary)
```

### **Service Accounts**
```bash
# Dataflow service account
DATAFLOW_SERVICE_ACCOUNT_NAME="dataflow-worker-sa"
DATAFLOW_SERVICE_ACCOUNT_EMAIL="${DATAFLOW_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Airflow service account
AIRFLOW_SERVICE_ACCOUNT_NAME="airflow-scheduler-sa" 
AIRFLOW_SERVICE_ACCOUNT_EMAIL="${AIRFLOW_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

### **BigQuery Configuration**
```bash
# Datasets
RAW_DATASET_ID="raw_data"                  # Raw ingested data
PROCESSED_DATASET_ID="processed_data"      # Transformed data
ANALYTICS_DATASET_ID="analytics"           # Analytics-ready data
AUDIT_DATASET_ID="audit_logs"              # Audit and logging

# Tables
PIPELINE_METADATA_TABLE="pipeline_metadata"
DATA_QUALITY_TABLE="data_quality_results"
RAW_TABLE="raw_events"
PROCESSED_TABLE="processed_events"
```

### **Pub/Sub Configuration**
```bash
# Topics
INPUT_TOPIC_ID="input-data-topic"          # Main input stream
ERROR_TOPIC_ID="error-topic"               # Dead letter topic
AUDIT_TOPIC_ID="audit-topic"               # Audit events
OUTPUT_TOPIC_ID="output-data-topic"        # Processed output

# Subscriptions
PROCESSING_SUBSCRIPTION_ID="processing-subscription"
MONITORING_SUBSCRIPTION_ID="monitoring-subscription"
ERROR_SUBSCRIPTION_ID="error-subscription"
```

### **Storage Configuration**
```bash
# Buckets
DATA_LAKE_BUCKET_NAME="${PROJECT_ID}-data-lake"      # Primary data lake
TEMP_BUCKET_NAME="${PROJECT_ID}-temp-processing"     # Temporary processing
BACKUP_BUCKET_NAME="${PROJECT_ID}-backup-storage"    # Backup storage
CONFIG_BUCKET_NAME="${PROJECT_ID}-config"            # Configuration files

# Paths
RAW_DATA_PATH="raw"
PROCESSED_DATA_PATH="processed"
ENRICHED_DATA_PATH="enriched"
ARCHIVE_DATA_PATH="archive"
```

---

## 🔧 Environment-Specific Configuration

### **Development Environment**
```bash
ENVIRONMENT="development"
DEV_PROJECT_ID="${PROJECT_ID}-dev"
DEV_DATAFLOW_NUM_WORKERS="1"
DEV_DATAFLOW_MAX_NUM_WORKERS="5"
DEV_COMPOSER_NODE_COUNT="1"
```

### **Staging Environment**
```bash
ENVIRONMENT="staging"
STAGING_PROJECT_ID="${PROJECT_ID}-staging"
STAGING_DATAFLOW_NUM_WORKERS="2"
STAGING_DATAFLOW_MAX_NUM_WORKERS="10"
STAGING_COMPOSER_NODE_COUNT="2"
```

### **Production Environment**
```bash
ENVIRONMENT="production"
DATAFLOW_NUM_WORKERS="5"
DATAFLOW_MAX_NUM_WORKERS="50"
COMPOSER_NODE_COUNT="3"
```

---

## 🚀 Pipeline-Specific Configuration

### **Dataflow Configuration**
```bash
# Pipeline names
HYBRID_PIPELINE_NAME="hybrid-data-pipeline"
RECONCILIATION_PIPELINE_NAME="reconciliation-pipeline"
BATCH_PIPELINE_NAME="batch-processing-pipeline"

# Worker configuration
DATAFLOW_NUM_WORKERS="5"                   # Initial worker count
DATAFLOW_MAX_NUM_WORKERS="50"              # Maximum workers
DATAFLOW_WORKER_MACHINE_TYPE="n1-standard-4"  # Machine type
DATAFLOW_DISK_SIZE_GB="100"                # Worker disk size

# Performance settings
DATAFLOW_USE_RUNNER_V2="true"
DATAFLOW_SDK_WORKER_PARALLELISM="1"
DATAFLOW_AUTOSCALING_ALGORITHM="THROUGHPUT_BASED"
```

### **Airflow Configuration**
```bash
# Environment settings
COMPOSER_ENVIRONMENT_NAME="data-platform-composer"
COMPOSER_LOCATION="${REGION}"
COMPOSER_NODE_COUNT="3"
COMPOSER_MACHINE_TYPE="n1-standard-2"

# DAG configuration
DAG_OWNER="data-platform-team"
DAG_EMAIL="data-platform@${DOMAIN}"
DAG_RETRIES="2"
DAG_RETRY_DELAY_MINUTES="5"
DAG_SCHEDULE_INTERVAL="@daily"

# DAG names
BATCH_DAG_ID="batch_processing_pipeline"
REALTIME_DAG_ID="realtime_trigger_pipeline"
RECONCILIATION_DAG_ID="reconciliation_pipeline"
INITIATE_DAG_ID="initiate_pipeline"
```

---

## 📊 Monitoring & Alerting Configuration

### **Monitoring Setup**
```bash
# Workspace configuration
MONITORING_WORKSPACE_ID="${PROJECT_ID}"
LOG_SINK_NAME="audit-log-sink"

# Notification channels
EMAIL_NOTIFICATION_CHANNEL="email-alerts"
SLACK_NOTIFICATION_CHANNEL="slack-alerts"
ALERT_EMAIL_ADDRESS="alerts@${DOMAIN}"
```

### **Alert Thresholds**
```bash
# Performance thresholds
DATAFLOW_LATENCY_THRESHOLD_SECONDS="300"       # 5 minutes
BIGQUERY_WRITE_THRESHOLD_SECONDS="10"          # 10 seconds
PUBSUB_BACKLOG_THRESHOLD="1000"                # 1000 messages
PIPELINE_ERROR_RATE_THRESHOLD="0.05"           # 5% error rate
SECRET_MANAGER_FAILURE_THRESHOLD="5"           # 5 failures

# Data quality thresholds
DATA_QUALITY_THRESHOLD="0.95"                  # 95% quality score
DQ_ALERT_THRESHOLD="0.90"                      # Alert below 90%
```

---

## 🔐 Security Configuration

### **Secret Management**
```bash
# Secret names
DATABASE_PASSWORD_SECRET="database-password"
EXTERNAL_API_KEY_SECRET="external-api-key"
SERVICE_ACCOUNT_KEY_SECRET="service-account-key"
ENCRYPTION_KEY_SECRET="data-encryption-key"
SLACK_WEBHOOK_SECRET="slack-webhook-url"
```

### **Access Control**
```bash
# User groups
DATA_ENGINEER_GROUP="data-engineers"
DATA_ANALYST_GROUP="data-analysts"

# Custom roles
CUSTOM_PIPELINE_ROLE_NAME="data_pipeline_operator"

# Network security
ENABLE_PRIVATE_GOOGLE_ACCESS="true"
ENABLE_VPC_FLOW_LOGS="true"
ENABLE_CLOUD_NAT="false"
```

---

## 🎛️ Feature Flags

### **Core Features**
```bash
# Pipeline features
ENABLE_REAL_TIME_PROCESSING="true"
ENABLE_BATCH_PROCESSING="true"
ENABLE_DATA_QUALITY_CHECKS="true"
ENABLE_AUDIT_LOGGING="true"

# Advanced features
ENABLE_COST_OPTIMIZATION="true"
ENABLE_ADVANCED_MONITORING="true"
ENABLE_DATAPLEX_INTEGRATION="true"
ENABLE_COMPOSER_INTEGRATION="true"
```

### **Integration Toggles**
```bash
# External integrations
ENABLE_SECRET_MANAGER="true"
ENABLE_PRIVATE_NETWORKING="false"

# Development features
ENABLE_DEBUG_LOGGING="false"
ENABLE_PERFORMANCE_PROFILING="false"
```

---

## ✅ Configuration Validation

### **Required Parameter Check**
```bash
# Essential parameters that must be set
required_params=(
    "PROJECT_ID"
    "REGION"
    "BIGQUERY_REGION"
    "RAW_DATASET_ID"
    "PROCESSED_DATASET_ID"
    "DATA_LAKE_BUCKET_NAME"
    "INPUT_TOPIC_ID"
    "PROCESSING_SUBSCRIPTION_ID"
    "DATAFLOW_SERVICE_ACCOUNT_NAME"
    "AIRFLOW_SERVICE_ACCOUNT_NAME"
)
```

### **Validation Script**
```bash
#!/bin/bash
# Validate all required parameters are set
source docs/configuration/parameters.conf

for param in "${required_params[@]}"; do
    if [[ -z "${!param:-}" ]]; then
        echo "❌ Missing required parameter: $param"
        exit 1
    fi
done

echo "✅ All required parameters are configured"
```

---

## 🔄 Configuration Management

### **Version Control**
```bash
# Add to .gitignore
docs/configuration/parameters.conf         # Don't commit actual values
docs/configuration/parameters.local.conf  # Local configuration
.config/                                   # Backup directory
```

### **Backup and Recovery**
```bash
# Automatic backup during replacement
backup_dir="$PROJECT_ROOT/.config"
mkdir -p "$backup_dir"
cp "$PARAMS_FILE" "$backup_dir/parameters.conf.backup"
```

### **Environment Promotion**
```bash
# Copy configuration between environments
cp environments/dev/parameters.conf environments/staging/parameters.conf
# Edit staging-specific values
# Test and validate
# Promote to production
```

---

## 🛠️ Troubleshooting

### **Common Issues**

#### **Parameter Not Replaced**
```bash
# Check parameter syntax
grep '\${PARAM_NAME}' file.yaml

# Verify parameter is exported
echo $PARAM_NAME

# Re-run replacement script
bash scripts/replace_parameters.sh
```

#### **Invalid Parameter Values**
```bash
# Validate GCP resource names
gcloud projects describe $PROJECT_ID
gcloud storage buckets describe gs://$DATA_LAKE_BUCKET_NAME

# Check service account exists
gcloud iam service-accounts describe $DATAFLOW_SERVICE_ACCOUNT_EMAIL
```

#### **Permission Errors**
```bash
# Verify service account permissions
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$DATAFLOW_SERVICE_ACCOUNT_EMAIL"
```

### **Validation Commands**
```bash
# Check all files for unreplaced parameters
find . -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.tf" \) \
  -exec grep -l '\${[A-Z_][A-Z0-9_]*}' {} \;

# Validate Terraform configuration
terraform validate

# Test Airflow DAG syntax
python -m py_compile airflow/dags/*.py

# Validate YAML files
yamllint config/ monitoring/
```

---

## 📚 Best Practices

### **Parameter Naming**
- Use UPPERCASE with underscores
- Include category prefix (BIGQUERY_, DATAFLOW_, etc.)
- Be descriptive but concise
- Use consistent suffixes (_ID, _NAME, _BUCKET, etc.)

### **Value Guidelines**
- Use consistent naming patterns across resources
- Include project ID in resource names for uniqueness
- Use lowercase with hyphens for GCP resource names
- Include environment prefix for multi-env setups

### **Security Considerations**
- Never commit actual parameter values to version control
- Use Secret Manager for sensitive values
- Rotate credentials regularly
- Use least-privilege access for service accounts

### **Maintenance**
- Document parameter changes in commit messages
- Test configuration changes in development first
- Keep parameter documentation up to date
- Regular backup of configuration files

---

*This guide ensures consistent, secure, and maintainable configuration management across the entire data platform project.*
