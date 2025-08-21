#!/bin/bash

# setup_secrets.sh - Script to create and populate Secret Manager secrets

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check required environment variables
check_env_vars() {
    print_status "Checking required environment variables..."
    
    required_vars=("PROJECT_ID" "REGION")
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            print_error "Environment variable $var is not set"
            exit 1
        fi
    done
    
    print_status "Environment variables check passed"
}

# Check if gcloud is authenticated
check_auth() {
    print_status "Checking Google Cloud authentication..."
    
    if ! gcloud auth list --filter="status:ACTIVE" --format="value(account)" | grep -q "@"; then
        print_error "No active gcloud authentication found"
        print_error "Please run: gcloud auth login"
        exit 1
    fi
    
    print_status "Authentication check passed"
}

# Set the project
set_project() {
    print_status "Setting project to $PROJECT_ID..."
    gcloud config set project "$PROJECT_ID"
}

# Enable required APIs
enable_apis() {
    print_status "Enabling required APIs..."
    
    apis=(
        "secretmanager.googleapis.com"
        "dataplex.googleapis.com"
        "datacatalog.googleapis.com"
        "datalineage.googleapis.com"
    )
    
    for api in "${apis[@]}"; do
        print_status "Enabling $api..."
        gcloud services enable "$api" --quiet
    done
    
    print_status "APIs enabled successfully"
}

# Create Secret Manager secrets with default values
create_secrets() {
    print_status "Creating Secret Manager secrets..."
    
    # AWS S3 Access Key ID
    if ! gcloud secrets describe aws-s3-access-key-id >/dev/null 2>&1; then
        print_status "Creating secret: aws-s3-access-key-id"
        echo "YOUR_AWS_ACCESS_KEY_ID" | gcloud secrets create aws-s3-access-key-id \
            --data-file=- \
            --labels="environment=${ENVIRONMENT:-dev},component=data-pipeline,type=aws-credential"
        print_warning "Please update the secret value with your actual AWS access key ID"
    else
        print_status "Secret aws-s3-access-key-id already exists"
    fi
    
    # AWS S3 Secret Access Key
    if ! gcloud secrets describe aws-s3-secret-access-key >/dev/null 2>&1; then
        print_status "Creating secret: aws-s3-secret-access-key"
        echo "YOUR_AWS_SECRET_ACCESS_KEY" | gcloud secrets create aws-s3-secret-access-key \
            --data-file=- \
            --labels="environment=${ENVIRONMENT:-dev},component=data-pipeline,type=aws-credential"
        print_warning "Please update the secret value with your actual AWS secret access key"
    else
        print_status "Secret aws-s3-secret-access-key already exists"
    fi
    
    # AWS S3 Bucket Name
    if ! gcloud secrets describe aws-s3-bucket-name >/dev/null 2>&1; then
        print_status "Creating secret: aws-s3-bucket-name"
        echo "your-aws-s3-bucket-name" | gcloud secrets create aws-s3-bucket-name \
            --data-file=- \
            --labels="environment=${ENVIRONMENT:-dev},component=data-pipeline,type=aws-resource"
        print_warning "Please update the secret value with your actual S3 bucket name"
    else
        print_status "Secret aws-s3-bucket-name already exists"
    fi
    
    # BigQuery Service Account Key
    if ! gcloud secrets describe bq-service-account-key >/dev/null 2>&1; then
        print_status "Creating secret: bq-service-account-key"
        echo '{"type": "service_account", "project_id": "placeholder"}' | gcloud secrets create bq-service-account-key \
            --data-file=- \
            --labels="environment=${ENVIRONMENT:-dev},component=data-pipeline,type=service-account"
        print_warning "Please update the secret value with your actual BigQuery service account JSON key"
    else
        print_status "Secret bq-service-account-key already exists"
    fi
    
    print_status "Secret creation completed"
}

# Grant IAM permissions
grant_iam_permissions() {
    print_status "Setting up IAM permissions..."
    
    # Get Composer and Dataflow service accounts
    COMPOSER_SA="${COMPOSER_SA:-dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com}"
    DATAFLOW_SA="${DATAFLOW_SA:-dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com}"
    
    secrets=("aws-s3-access-key-id" "aws-s3-secret-access-key" "aws-s3-bucket-name" "bq-service-account-key")
    
    for secret in "${secrets[@]}"; do
        print_status "Granting access to secret: $secret"
        
        # Grant access to Composer service account
        gcloud secrets add-iam-policy-binding "$secret" \
            --member="serviceAccount:$COMPOSER_SA" \
            --role="roles/secretmanager.secretAccessor" \
            --quiet
        
        # Grant access to Dataflow service account
        gcloud secrets add-iam-policy-binding "$secret" \
            --member="serviceAccount:$DATAFLOW_SA" \
            --role="roles/secretmanager.secretAccessor" \
            --quiet
    done
    
    print_status "IAM permissions configured"
}

# Update secrets with actual values
update_secrets() {
    print_status "Instructions for updating secrets with actual values:"
    print_status ""
    
    print_status "1. Update AWS S3 Access Key ID:"
    echo "   gcloud secrets versions add aws-s3-access-key-id --data-file=<path-to-access-key-file>"
    print_status ""
    
    print_status "2. Update AWS S3 Secret Access Key:"
    echo "   gcloud secrets versions add aws-s3-secret-access-key --data-file=<path-to-secret-key-file>"
    print_status ""
    
    print_status "3. Update AWS S3 Bucket Name:"
    echo "   echo 'your-actual-bucket-name' | gcloud secrets versions add aws-s3-bucket-name --data-file=-"
    print_status ""
    
    print_status "4. Update BigQuery Service Account Key:"
    echo "   gcloud secrets versions add bq-service-account-key --data-file=service-account-key.json"
    print_status ""
    
    print_warning "Remember to update these secrets with actual values before running the pipelines!"
}

# Verify Dataplex setup
verify_dataplex() {
    print_status "Verifying Dataplex setup..."
    
    # Check if Dataplex resources exist (will be created by Terraform)
    print_status "Dataplex resources will be created automatically by Terraform deployment"
    print_status "Run 'terraform apply' to create Dataplex lakes, zones, and assets"
}

# Main function
main() {
    print_status "Starting Secret Manager and Dataplex setup..."
    print_status "Project: $PROJECT_ID"
    print_status "Region: ${REGION:-asia-southeast1}"
    print_status "Environment: ${ENVIRONMENT:-dev}"
    print_status ""
    
    check_env_vars
    check_auth
    set_project
    enable_apis
    create_secrets
    grant_iam_permissions
    update_secrets
    verify_dataplex
    
    print_status ""
    print_status "✅ Secret Manager setup completed successfully!"
    print_status ""
    print_status "Next steps:"
    print_status "1. Update the secret values with your actual credentials"
    print_status "2. Run 'terraform apply' to create Dataplex infrastructure"
    print_status "3. Deploy your Airflow DAGs to Cloud Composer"
    print_status "4. Test the pipelines with your data"
}

# Help function
show_help() {
    echo "Setup script for Secret Manager secrets and Dataplex infrastructure"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Required environment variables:"
    echo "  PROJECT_ID      - GCP Project ID"
    echo "  REGION          - GCP Region (default: asia-southeast1)"
    echo ""
    echo "Optional environment variables:"
    echo "  ENVIRONMENT     - Environment name (default: dev)"
    echo "  COMPOSER_SA     - Composer service account email"
    echo "  DATAFLOW_SA     - Dataflow service account email"
    echo ""
    echo "Options:"
    echo "  -h, --help      - Show this help message"
    echo ""
    echo "Example:"
    echo "  export PROJECT_ID='your-project-id'"
    echo "  export REGION='asia-southeast1'"
    echo "  export ENVIRONMENT='dev'"
    echo "  $0"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Run main function
main
