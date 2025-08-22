#!/bin/bash

# Parameter Replacement Utility Script
# This script replaces all parameter placeholders in configuration files, 
# pipeline scripts, DAGs, and Dataflow pipelines with actual values

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PARAMS_FILE="${PROJECT_ROOT}/docs/configuration/parameters.conf"

print_info "Starting parameter replacement process..."
print_info "Project root: $PROJECT_ROOT"

# Check if parameters file exists
if [[ ! -f "$PARAMS_FILE" ]]; then
    print_error "Parameters file not found: $PARAMS_FILE"
    print_info "Please copy and configure docs/configuration/parameters.conf first"
    exit 1
fi

# Source the parameters file
print_info "Loading parameters from: $PARAMS_FILE"
source "$PARAMS_FILE"

# Validate required parameters
print_info "Validating required parameters..."

required_params=(
    "PROJECT_ID"
    "REGION"
    "BIGQUERY_REGION"
    "RAW_DATASET_ID"
    "PROCESSED_DATASET_ID"
    "DATA_LAKE_BUCKET_NAME"
    "INPUT_TOPIC_ID"
    "PROCESSING_SUBSCRIPTION_ID"
)

missing_params=()
for param in "${required_params[@]}"; do
    if [[ -z "${!param:-}" ]]; then
        missing_params+=("$param")
    fi
done

if [[ ${#missing_params[@]} -gt 0 ]]; then
    print_error "Missing required parameters:"
    for param in "${missing_params[@]}"; do
        echo "  - $param"
    done
    exit 1
fi

print_success "All required parameters are set"

# Function to replace parameters in a file
replace_parameters_in_file() {
    local file_path="$1"
    local temp_file="${file_path}.tmp"
    
    if [[ ! -f "$file_path" ]]; then
        print_warning "File not found: $file_path"
        return 1
    fi
    
    print_info "Processing: $file_path"
    
    # Create a copy of the original file
    cp "$file_path" "$temp_file"
    
    # Replace parameters using environment variables
    # This reads all exported variables and replaces ${VAR_NAME} patterns
    envsubst < "$file_path" > "$temp_file"
    
    # If the file was modified, replace the original
    if ! diff -q "$file_path" "$temp_file" > /dev/null 2>&1; then
        mv "$temp_file" "$file_path"
        print_success "Updated: $file_path"
        return 0
    else
        rm "$temp_file"
        print_info "No changes needed: $file_path"
        return 0
    fi
}

# Function to replace parameters in all files matching a pattern
replace_parameters_in_pattern() {
    local pattern="$1"
    local description="$2"
    
    print_info "Processing $description..."
    
    local files_found=0
    while IFS= read -r -d '' file; do
        replace_parameters_in_file "$file"
        ((files_found++))
    done < <(find "$PROJECT_ROOT" -name "$pattern" -type f -print0 2>/dev/null)
    
    if [[ $files_found -eq 0 ]]; then
        print_warning "No files found matching pattern: $pattern"
    else
        print_success "Processed $files_found file(s) for $description"
    fi
}

# Export all variables so envsubst can use them
print_info "Exporting parameters as environment variables..."
set -a  # Automatically export all variables
source "$PARAMS_FILE"
set +a  # Stop auto-exporting

# Process different file types
print_info "=== PROCESSING CONFIGURATION FILES ==="

# YAML configuration files
replace_parameters_in_pattern "*.yaml" "YAML configuration files"
replace_parameters_in_pattern "*.yml" "YML configuration files"

# JSON configuration files
replace_parameters_in_pattern "*.json" "JSON configuration files"

# Terraform files
print_info "=== PROCESSING TERRAFORM FILES ==="
replace_parameters_in_pattern "*.tf" "Terraform files"
replace_parameters_in_pattern "*.tfvars" "Terraform variable files"

# Airflow DAGs
print_info "=== PROCESSING AIRFLOW DAGS ==="
if [[ -d "$PROJECT_ROOT/airflow/dags" ]]; then
    while IFS= read -r -d '' file; do
        replace_parameters_in_file "$file"
    done < <(find "$PROJECT_ROOT/airflow/dags" -name "*.py" -type f -print0)
    print_success "Processed Airflow DAG files"
else
    print_warning "Airflow DAGs directory not found"
fi

# Dataflow pipelines
print_info "=== PROCESSING DATAFLOW PIPELINES ==="
if [[ -d "$PROJECT_ROOT/dataflow" ]]; then
    while IFS= read -r -d '' file; do
        replace_parameters_in_file "$file"
    done < <(find "$PROJECT_ROOT/dataflow" -name "*.py" -type f -print0)
    print_success "Processed Dataflow pipeline files"
else
    print_warning "Dataflow directory not found"
fi

# Shell scripts
print_info "=== PROCESSING SHELL SCRIPTS ==="
replace_parameters_in_pattern "*.sh" "Shell scripts"

# Documentation files
print_info "=== PROCESSING DOCUMENTATION ==="
replace_parameters_in_pattern "*.md" "Markdown documentation files"

# SQL files
print_info "=== PROCESSING SQL FILES ==="
replace_parameters_in_pattern "*.sql" "SQL files"

# Special handling for specific files
print_info "=== PROCESSING SPECIAL FILES ==="

# Process pipeline config
if [[ -f "$PROJECT_ROOT/config/pipeline_config.yaml" ]]; then
    replace_parameters_in_file "$PROJECT_ROOT/config/pipeline_config.yaml"
fi

# Process Airflow variables
if [[ -f "$PROJECT_ROOT/airflow/config/airflow_variables.json" ]]; then
    replace_parameters_in_file "$PROJECT_ROOT/airflow/config/airflow_variables.json"
fi

# Process monitoring configs
if [[ -d "$PROJECT_ROOT/monitoring" ]]; then
    while IFS= read -r -d '' file; do
        replace_parameters_in_file "$file"
    done < <(find "$PROJECT_ROOT/monitoring" -type f \( -name "*.yaml" -o -name "*.yml" -o -name "*.sh" -o -name "*.sql" \) -print0)
    print_success "Processed monitoring configuration files"
fi

# Generate environment-specific files
print_info "=== GENERATING ENVIRONMENT-SPECIFIC FILES ==="

# Create environment-specific Terraform variable files
if [[ -d "$PROJECT_ROOT/terraform" ]]; then
    for env in development staging production; do
        env_file="$PROJECT_ROOT/terraform/environments/${env}.tfvars"
        
        if [[ ! -f "$env_file" ]]; then
            mkdir -p "$(dirname "$env_file")"
            
            cat > "$env_file" << EOF
# Environment-specific variables for $env
project_id = "${PROJECT_ID}"
environment = "$env"
region = "${REGION}"
bigquery_region = "${BIGQUERY_REGION}"

# Scaling configuration for $env
EOF
            
            case $env in
                development)
                    cat >> "$env_file" << EOF
dataflow_num_workers = 1
dataflow_max_num_workers = 5
composer_node_count = 1
EOF
                    ;;
                staging)
                    cat >> "$env_file" << EOF
dataflow_num_workers = 2
dataflow_max_num_workers = 10
composer_node_count = 2
EOF
                    ;;
                production)
                    cat >> "$env_file" << EOF
dataflow_num_workers = ${DATAFLOW_NUM_WORKERS}
dataflow_max_num_workers = ${DATAFLOW_MAX_NUM_WORKERS}
composer_node_count = ${COMPOSER_NODE_COUNT}
EOF
                    ;;
            esac
            
            print_success "Created environment file: $env_file"
        else
            print_info "Environment file already exists: $env_file"
        fi
    done
fi

# Validation
print_info "=== VALIDATING REPLACEMENTS ==="

# Check for remaining unreplaced variables
unreplaced_files=()
while IFS= read -r -d '' file; do
    if grep -l '\${[A-Z_][A-Z0-9_]*}' "$file" > /dev/null 2>&1; then
        unreplaced_files+=("$file")
    fi
done < <(find "$PROJECT_ROOT" -type f \( -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.json" -o -name "*.tf" -o -name "*.sh" -o -name "*.sql" -o -name "*.md" \) -print0 2>/dev/null)

if [[ ${#unreplaced_files[@]} -gt 0 ]]; then
    print_warning "Files with unreplaced variables found:"
    for file in "${unreplaced_files[@]}"; do
        echo "  - $file"
        # Show which variables are unreplaced
        grep -n '\${[A-Z_][A-Z0-9_]*}' "$file" | head -3
    done
    print_warning "Please check these files and ensure all required parameters are defined"
else
    print_success "All parameter placeholders have been replaced"
fi

# Create a backup of the parameters file in the git-ignored area
backup_dir="$PROJECT_ROOT/.config"
mkdir -p "$backup_dir"
cp "$PARAMS_FILE" "$backup_dir/parameters.conf.backup"
print_success "Created backup of parameters file"

# Summary
print_info "=== REPLACEMENT SUMMARY ==="
echo "✅ Configuration files processed"
echo "✅ Terraform files processed" 
echo "✅ Airflow DAGs processed"
echo "✅ Dataflow pipelines processed"
echo "✅ Shell scripts processed"
echo "✅ Documentation processed"
echo "✅ Monitoring configs processed"

print_success "Parameter replacement completed successfully!"
print_info ""
print_info "Next steps:"
echo "1. Review the modified files to ensure correctness"
echo "2. Test the configuration in a development environment"
echo "3. Commit the changes to version control (excluding sensitive parameters)"
echo "4. Deploy using: terraform plan && terraform apply"

print_warning "Important: Keep the parameters.conf file secure and do not commit it to version control!"

# Create a gitignore entry for the parameters file if it doesn't exist
gitignore_file="$PROJECT_ROOT/.gitignore"
if [[ -f "$gitignore_file" ]]; then
    if ! grep -q "docs/configuration/parameters.conf" "$gitignore_file"; then
        echo "" >> "$gitignore_file"
        echo "# Sensitive configuration files" >> "$gitignore_file"
        echo "docs/configuration/parameters.conf" >> "$gitignore_file"
        echo ".config/" >> "$gitignore_file"
        print_success "Added parameters.conf to .gitignore"
    fi
else
    cat > "$gitignore_file" << EOF
# Sensitive configuration files
docs/configuration/parameters.conf
.config/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so

# Terraform
*.tfstate
*.tfstate.*
.terraform/
.terraform.lock.hcl

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
EOF
    print_success "Created .gitignore file"
fi

print_info "✨ All done! Your project is now configured with your parameters."
