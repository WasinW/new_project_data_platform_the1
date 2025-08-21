#!/bin/bash

# scripts/setup.sh
# Setup script for GCP Data Platform

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-your-gcp-project-id}"
REGION="${REGION:-asia-southeast1}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
DOMAIN="${DOMAIN:-member}"

echo "Setting up GCP Data Platform..."
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Environment: $ENVIRONMENT"
echo "Domain: $DOMAIN"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI is not installed"
    exit 1
fi

# Check if authenticated
if ! gcloud auth list --filter="status:ACTIVE" --format="value(account)" | grep -q .; then
    echo "Error: Not authenticated with gcloud"
    echo "Run: gcloud auth login"
    exit 1
fi

# Set project
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "Enabling required APIs..."
gcloud services enable \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    pubsub.googleapis.com \
    composer.googleapis.com \
    datacatalog.googleapis.com \
    dlp.googleapis.com \
    storagetransfer.googleapis.com

# Deploy infrastructure with Terraform
echo "Deploying infrastructure with Terraform..."
cd terraform

terraform init
terraform plan -var="project_id=$PROJECT_ID" -var="region=$REGION" -var="environment=$ENVIRONMENT" -var="primary_domain=$DOMAIN"
terraform apply -auto-approve -var="project_id=$PROJECT_ID" -var="region=$REGION" -var="environment=$ENVIRONMENT" -var="primary_domain=$DOMAIN"

cd ..

# Get Terraform outputs
DATAFLOW_SA=$(terraform -chdir=terraform output -raw dataflow_service_account)
CONFIG_BUCKET=$(terraform -chdir=terraform output -raw storage_buckets | jq -r '.["pipeline-configs"]')
TEMPLATE_BUCKET=$(terraform -chdir=terraform output -raw storage_buckets | jq -r '.["dataflow-templates"]')
COMPOSER_ENV=$(terraform -chdir=terraform output -raw composer_environment_name)

echo "Infrastructure deployed successfully!"
echo "Dataflow Service Account: $DATAFLOW_SA"
echo "Config Bucket: $CONFIG_BUCKET"
echo "Template Bucket: $TEMPLATE_BUCKET"
echo "Composer Environment: $COMPOSER_ENV"

# Upload configuration files
echo "Uploading configuration files..."
gsutil -m cp config/*.yaml gs://$CONFIG_BUCKET/

# Build and upload Dataflow templates
echo "Building Dataflow templates..."
cd dataflow

# Build Docker image
docker build -t gcr.io/$PROJECT_ID/hybrid-pipeline:latest .
docker push gcr.io/$PROJECT_ID/hybrid-pipeline:latest

# Create Flex Template
gcloud dataflow flex-template build \
    gs://$TEMPLATE_BUCKET/hybrid_pipeline.json \
    --image gcr.io/$PROJECT_ID/hybrid-pipeline:latest \
    --sdk-language PYTHON \
    --metadata-file metadata.json

cd ..

# Create metadata file for templates
cat > dataflow/metadata.json << EOF
{
  "name": "Hybrid Data Pipeline",
  "description": "Flexible pipeline for both batch and streaming data processing",
  "parameters": [
    {
      "name": "mode",
      "label": "Processing Mode",
      "helpText": "Choose between 'batch' or 'realtime' processing mode",
      "is_optional": false,
      "regexes": ["^(batch|realtime)$"]
    },
    {
      "name": "config_path", 
      "label": "Configuration Path",
      "helpText": "GCS path to the pipeline configuration file",
      "is_optional": false
    },
    {
      "name": "domain",
      "label": "Data Domain",
      "helpText": "Business domain for the data (e.g., member, order, product)",
      "is_optional": false
    },
    {
      "name": "batch_window_hours",
      "label": "Batch Window Hours",
      "helpText": "Number of hours to look back for batch processing",
      "is_optional": true
    }
  ]
}
EOF

# Upload Airflow DAGs and configurations
echo "Uploading Airflow DAGs..."
gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV \
    --location $REGION \
    --source airflow/dags

# Import Airflow variables
echo "Importing Airflow variables..."
gcloud composer environments storage data import \
    --environment $COMPOSER_ENV \
    --location $REGION \
    --source airflow/config/airflow_variables.json

# Create BigQuery schemas directory if not exists
mkdir -p config/schemas

# Create sample BigQuery table schemas
echo "Creating BigQuery table schemas..."

# Processing logs schema
cat > config/schemas/processing_logs_schema.json << EOF
[
  {"name": "pipeline_name", "type": "STRING", "mode": "REQUIRED"},
  {"name": "pipeline_mode", "type": "STRING", "mode": "REQUIRED"},
  {"name": "domain", "type": "STRING", "mode": "REQUIRED"},
  {"name": "record_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "source_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "target_table", "type": "STRING", "mode": "NULLABLE"},
  {"name": "transform_module", "type": "STRING", "mode": "NULLABLE"},
  {"name": "status", "type": "STRING", "mode": "REQUIRED"},
  {"name": "data_size_bytes", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "field_count", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "validation_status", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "processing_duration_ms", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "pipeline_version", "type": "STRING", "mode": "NULLABLE"},
  {"name": "environment", "type": "STRING", "mode": "NULLABLE"},
  {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
EOF

# Data lineage schema
cat > config/schemas/data_lineage_schema.json << EOF
[
  {"name": "record_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "source_system", "type": "STRING", "mode": "NULLABLE"},
  {"name": "source_table", "type": "STRING", "mode": "NULLABLE"},
  {"name": "target_table", "type": "STRING", "mode": "NULLABLE"},
  {"name": "transformation_applied", "type": "STRING", "mode": "NULLABLE"},
  {"name": "lineage_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "pipeline_run_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "data_classification", "type": "STRING", "mode": "NULLABLE"},
  {"name": "processing_stage", "type": "STRING", "mode": "NULLABLE"},
  {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
EOF

# Upload schemas to config bucket
gsutil -m cp config/schemas/*.json gs://$CONFIG_BUCKET/schemas/

echo "Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Review and customize configuration files in gs://$CONFIG_BUCKET/"
echo "2. Access Composer environment: $(terraform -chdir=terraform output -raw composer_uri)"
echo "3. Monitor pipeline execution in Cloud Console"
echo "4. Set up alerting and monitoring as needed"
echo ""
echo "To start pipelines:"
echo "- Trigger initiate pipeline for one-time migration"
echo "- Enable realtime pipeline for streaming processing"
echo "- Enable batch pipeline for scheduled processing"
