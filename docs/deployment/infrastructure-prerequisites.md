# Infrastructure Prerequisites & Setup Guide

## 🏗️ Overview
This document lists all required Google Cloud services, infrastructure components, and configurations that must be prepared before initializing the data platform project.

---

## 📋 Table of Contents
1. [Google Cloud Project Setup](#google-cloud-project-setup)
2. [IAM & Service Accounts](#iam--service-accounts)
3. [BigQuery Resources](#bigquery-resources)
4. [Pub/Sub Resources](#pubsub-resources)
5. [Cloud Storage Resources](#cloud-storage-resources)
6. [Secret Manager Setup](#secret-manager-setup)
7. [Dataplex Configuration](#dataplex-configuration)
8. [Monitoring & Alerting](#monitoring--alerting)
9. [Networking & Security](#networking--security)
10. [Terraform State Management](#terraform-state-management)

---

## ☁️ Google Cloud Project Setup

### 1. **Google Cloud Project**
- **Project ID**: `${PROJECT_ID}`
- **Project Name**: `${PROJECT_NAME}`
- **Billing Account**: Must be enabled
- **Organization**: `${ORGANIZATION_ID}` (if applicable)

### 2. **Required APIs** (Enable via Cloud Console or gcloud)
```bash
gcloud services enable \
  bigquery.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  storage-api.googleapis.com \
  storage-component.googleapis.com \
  secretmanager.googleapis.com \
  dataplex.googleapis.com \
  composer.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

---

## 🔐 IAM & Service Accounts

### 1. **Service Accounts**

#### **Dataflow Service Account**
- **Name**: `${DATAFLOW_SERVICE_ACCOUNT_NAME}`
- **Email**: `${DATAFLOW_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com`
- **Required Roles**:
  - `roles/dataflow.worker`
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`
  - `roles/storage.objectViewer`
  - `roles/storage.objectCreator`
  - `roles/pubsub.subscriber`
  - `roles/pubsub.viewer`
  - `roles/secretmanager.secretAccessor`

#### **Airflow/Composer Service Account**
- **Name**: `${AIRFLOW_SERVICE_ACCOUNT_NAME}`
- **Email**: `${AIRFLOW_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com`
- **Required Roles**:
  - `roles/composer.worker`
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`
  - `roles/dataflow.admin`
  - `roles/pubsub.publisher`
  - `roles/pubsub.subscriber`
  - `roles/storage.objectAdmin`
  - `roles/secretmanager.secretAccessor`
  - `roles/dataplex.viewer`

#### **Monitoring Service Account**
- **Name**: `${MONITORING_SERVICE_ACCOUNT_NAME}`
- **Email**: `${MONITORING_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com`
- **Required Roles**:
  - `roles/monitoring.alertPolicyViewer`
  - `roles/monitoring.dashboardViewer`
  - `roles/logging.viewer`

### 2. **Custom IAM Roles**

#### **Data Pipeline Operator Role**
- **Name**: `${CUSTOM_PIPELINE_ROLE_NAME}`
- **Permissions**:
  ```yaml
  - bigquery.datasets.get
  - bigquery.tables.create
  - bigquery.tables.updateData
  - bigquery.tables.getData
  - storage.objects.create
  - storage.objects.get
  - pubsub.messages.publish
  - secretmanager.versions.access
  ```

### 3. **User IAM Bindings**
- **Data Engineers**: `${DATA_ENGINEER_GROUP}@${DOMAIN}`
  - `roles/bigquery.dataEditor`
  - `roles/composer.user`
  - `roles/dataflow.developer`
- **Data Analysts**: `${DATA_ANALYST_GROUP}@${DOMAIN}`
  - `roles/bigquery.dataViewer`
  - `roles/bigquery.jobUser`

---

## 🗄️ BigQuery Resources

### 1. **Datasets**

#### **Raw Data Dataset**
- **Dataset ID**: `${RAW_DATASET_ID}`
- **Location**: `${BIGQUERY_REGION}`
- **Default Table Expiration**: None
- **Description**: "Raw ingested data from various sources"

#### **Processed Data Dataset**
- **Dataset ID**: `${PROCESSED_DATASET_ID}`
- **Location**: `${BIGQUERY_REGION}`
- **Default Table Expiration**: None
- **Description**: "Processed and transformed data"

#### **Analytics Dataset**
- **Dataset ID**: `${ANALYTICS_DATASET_ID}`
- **Location**: `${BIGQUERY_REGION}`
- **Default Table Expiration**: None
- **Description**: "Analytics-ready data for reporting"

#### **Audit Dataset**
- **Dataset ID**: `${AUDIT_DATASET_ID}`
- **Location**: `${BIGQUERY_REGION}`
- **Default Table Expiration**: 2555 days (7 years)
- **Description**: "Pipeline audit logs and metadata"

### 2. **Required Tables**

#### **Pipeline Metadata Table**
- **Table ID**: `${PIPELINE_METADATA_TABLE}`
- **Dataset**: `${AUDIT_DATASET_ID}`
- **Schema**:
  ```sql
  CREATE TABLE `${PROJECT_ID}.${AUDIT_DATASET_ID}.${PIPELINE_METADATA_TABLE}` (
    pipeline_id STRING,
    execution_date TIMESTAMP,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INT64,
    error_message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
  )
  PARTITION BY DATE(execution_date)
  CLUSTER BY pipeline_id, status;
  ```

#### **Data Quality Results Table**
- **Table ID**: `${DATA_QUALITY_TABLE}`
- **Dataset**: `${AUDIT_DATASET_ID}`
- **Schema**:
  ```sql
  CREATE TABLE `${PROJECT_ID}.${AUDIT_DATASET_ID}.${DATA_QUALITY_TABLE}` (
    table_name STRING,
    column_name STRING,
    rule_name STRING,
    rule_result BOOLEAN,
    validation_timestamp TIMESTAMP,
    error_details STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
  )
  PARTITION BY DATE(validation_timestamp)
  CLUSTER BY table_name, rule_name;
  ```

---

## 📡 Pub/Sub Resources

### 1. **Topics**

#### **Input Data Topic**
- **Topic ID**: `${INPUT_TOPIC_ID}`
- **Message Retention**: 7 days
- **Description**: "Primary input topic for real-time data ingestion"

#### **Error Topic**
- **Topic ID**: `${ERROR_TOPIC_ID}`
- **Message Retention**: 30 days
- **Description**: "Dead letter topic for failed messages"

#### **Audit Topic**
- **Topic ID**: `${AUDIT_TOPIC_ID}`
- **Message Retention**: 7 days
- **Description**: "Audit events and pipeline status updates"

### 2. **Subscriptions**

#### **Processing Subscription**
- **Subscription ID**: `${PROCESSING_SUBSCRIPTION_ID}`
- **Topic**: `${INPUT_TOPIC_ID}`
- **Ack Deadline**: 600 seconds
- **Message Retention**: 7 days
- **Dead Letter Topic**: `${ERROR_TOPIC_ID}`
- **Max Delivery Attempts**: 5

#### **Monitoring Subscription**
- **Subscription ID**: `${MONITORING_SUBSCRIPTION_ID}`
- **Topic**: `${AUDIT_TOPIC_ID}`
- **Ack Deadline**: 60 seconds
- **Message Retention**: 7 days

---

## 🗂️ Cloud Storage Resources

### 1. **Data Lake Bucket**
- **Bucket Name**: `${DATA_LAKE_BUCKET_NAME}`
- **Location**: `${STORAGE_REGION}`
- **Storage Class**: Standard
- **Lifecycle Rules**:
  - Move to Nearline after 30 days
  - Move to Coldline after 90 days
  - Delete after 2555 days (7 years)
- **Versioning**: Enabled
- **Directory Structure**:
  ```
  gs://${DATA_LAKE_BUCKET_NAME}/
  ├── raw/
  │   ├── year=YYYY/month=MM/day=DD/
  │   └── source=${SOURCE_NAME}/
  ├── processed/
  │   ├── year=YYYY/month=MM/day=DD/
  │   └── pipeline=${PIPELINE_NAME}/
  ├── enriched/
  │   └── table=${TABLE_NAME}/
  └── archive/
      └── year=YYYY/
  ```

### 2. **Temporary Processing Bucket**
- **Bucket Name**: `${TEMP_BUCKET_NAME}`
- **Location**: `${STORAGE_REGION}`
- **Storage Class**: Standard
- **Lifecycle Rules**:
  - Delete objects after 7 days
- **Purpose**: Dataflow temporary files and staging

### 3. **Backup Bucket**
- **Bucket Name**: `${BACKUP_BUCKET_NAME}`
- **Location**: `${BACKUP_REGION}` (different region for DR)
- **Storage Class**: Coldline
- **Lifecycle Rules**:
  - Delete after 2555 days (7 years)
- **Purpose**: Cross-region backup and disaster recovery

### 4. **Configuration Bucket**
- **Bucket Name**: `${CONFIG_BUCKET_NAME}`
- **Location**: `${STORAGE_REGION}`
- **Storage Class**: Standard
- **Versioning**: Enabled
- **Purpose**: Pipeline configurations and templates

---

## 🔐 Secret Manager Setup

### 1. **Database Credentials**
- **Secret ID**: `${DATABASE_PASSWORD_SECRET}`
- **Description**: "Database connection password"

### 2. **API Keys**
- **Secret ID**: `${EXTERNAL_API_KEY_SECRET}`
- **Description**: "External API authentication key"

### 3. **Service Account Keys** (if needed)
- **Secret ID**: `${SERVICE_ACCOUNT_KEY_SECRET}`
- **Description**: "Service account JSON key for external integrations"

### 4. **Encryption Keys**
- **Secret ID**: `${ENCRYPTION_KEY_SECRET}`
- **Description**: "Data encryption key for sensitive data"

---

## 🏞️ Dataplex Configuration

### 1. **Data Lake**
- **Lake ID**: `${DATAPLEX_LAKE_ID}`
- **Location**: `${DATAPLEX_REGION}`
- **Description**: "Primary data lake for the platform"

### 2. **Zones**

#### **Raw Data Zone**
- **Zone ID**: `${RAW_ZONE_ID}`
- **Type**: RAW
- **Resource Spec**:
  - Location Type: SINGLE_REGION
  - Location: `${STORAGE_REGION}`

#### **Curated Data Zone**
- **Zone ID**: `${CURATED_ZONE_ID}`
- **Type**: CURATED
- **Resource Spec**:
  - Location Type: SINGLE_REGION
  - Location: `${STORAGE_REGION}`

### 3. **Assets**

#### **Raw Storage Asset**
- **Asset ID**: `${RAW_STORAGE_ASSET_ID}`
- **Zone**: `${RAW_ZONE_ID}`
- **Resource Spec**:
  - Type: STORAGE_BUCKET
  - Name: `${DATA_LAKE_BUCKET_NAME}`

#### **BigQuery Asset**
- **Asset ID**: `${BIGQUERY_ASSET_ID}`
- **Zone**: `${CURATED_ZONE_ID}`
- **Resource Spec**:
  - Type: BIGQUERY_DATASET
  - Name**: `${PROCESSED_DATASET_ID}`

---

## 📊 Monitoring & Alerting

### 1. **Cloud Monitoring Workspace**
- **Workspace ID**: `${MONITORING_WORKSPACE_ID}`
- **Project**: `${PROJECT_ID}`

### 2. **Notification Channels**

#### **Email Channel**
- **Channel ID**: `${EMAIL_NOTIFICATION_CHANNEL}`
- **Type**: email
- **Address**: `${ALERT_EMAIL_ADDRESS}`

#### **Slack Channel** (optional)
- **Channel ID**: `${SLACK_NOTIFICATION_CHANNEL}`
- **Type**: slack
- **Webhook URL**: Store in Secret Manager

### 3. **Log Sinks**
- **Sink Name**: `${AUDIT_LOG_SINK_NAME}`
- **Destination**: `${AUDIT_DATASET_ID}.audit_logs`
- **Filter**: 
  ```
  resource.type="dataflow_job" OR 
  resource.type="pubsub_topic" OR 
  resource.type="bigquery_dataset"
  ```

---

## 🌐 Networking & Security

### 1. **VPC Network** (if using private networking)
- **Network Name**: `${VPC_NETWORK_NAME}`
- **Subnet Name**: `${SUBNET_NAME}`
- **Region**: `${NETWORK_REGION}`
- **IP Range**: `${SUBNET_CIDR_RANGE}`

### 2. **Firewall Rules**
- **Rule Name**: `${DATAFLOW_FIREWALL_RULE}`
- **Direction**: INGRESS
- **Targets**: Service Account `${DATAFLOW_SERVICE_ACCOUNT_NAME}`
- **Source Ranges**: `${DATAFLOW_SUBNET_RANGE}`
- **Allowed Ports**: tcp:12345-12346

### 3. **Private Google Access**
- **Subnet**: `${SUBNET_NAME}`
- **Private Google Access**: Enabled

---

## 🗃️ Terraform State Management

### 1. **State Bucket**
- **Bucket Name**: `${TERRAFORM_STATE_BUCKET}`
- **Location**: `${STORAGE_REGION}`
- **Versioning**: Enabled
- **Encryption**: Google-managed keys

### 2. **State Lock Table** (if using DynamoDB-like solution)
- **Table Name**: `${TERRAFORM_LOCK_TABLE}`
- **Key Schema**: LockID (String)

---

## ✅ Pre-Deployment Checklist

### Google Cloud Setup
- [ ] Project created with billing enabled
- [ ] All required APIs enabled
- [ ] IAM roles and service accounts configured
- [ ] Service account keys downloaded (if needed)

### Infrastructure Resources
- [ ] BigQuery datasets and tables created
- [ ] Pub/Sub topics and subscriptions configured
- [ ] Cloud Storage buckets created with proper lifecycle policies
- [ ] Secret Manager secrets populated
- [ ] Dataplex lake and zones configured

### Security & Monitoring
- [ ] IAM permissions tested
- [ ] Network security rules configured
- [ ] Monitoring workspace and alerts configured
- [ ] Notification channels tested

### Development Environment
- [ ] Terraform state backend configured
- [ ] Local development tools installed
- [ ] Service account credentials configured for local development

---

## 🚀 Next Steps

After completing all prerequisites:

1. **Clone the repository**
2. **Configure environment variables** using the parameter file
3. **Run Terraform initialization**
4. **Deploy infrastructure** using Terraform
5. **Test pipeline deployment** in development environment
6. **Configure monitoring and alerting**
7. **Deploy to production**

For detailed deployment instructions, see the deployment guide in `docs/deployment/`.
