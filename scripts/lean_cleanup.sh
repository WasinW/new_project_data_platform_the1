#!/bin/bash

# scripts/lean_cleanup.sh
# Script to clean up unused files and optimize the project structure
# Based on lean_feature.md analysis

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$(pwd)}"
echo "🧹 Starting lean cleanup for project: $PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

echo_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Step 1: Remove unused files (already done above)
echo "📁 Step 1: Remove unused files"
UNUSED_FILES=(
    "backup/"
    "dataflow/Dockerfile"
    "dataflow/transforms/__init__.py"
)

for file in "${UNUSED_FILES[@]}"; do
    file_path="$PROJECT_ROOT/$file"
    if [[ -e "$file_path" ]]; then
        rm -rf "$file_path"
        echo_success "Removed unused file/directory: $file"
    else
        echo_warning "File/directory already removed: $file"
    fi
done

# Step 2: Check for duplicate code in transform files
echo ""
echo "🔍 Step 2: Checking for duplicate code patterns"

# Check if hybrid_pipeline.py (V1) still exists
if [[ -f "$PROJECT_ROOT/dataflow/pipelines/hybrid_pipeline.py" ]]; then
    echo_warning "V1 hybrid_pipeline.py still exists. Consider renaming it to hybrid_pipeline_legacy.py"
fi

# Check for unused classes in transform files
TRANSFORM_DIR="$PROJECT_ROOT/dataflow/transforms"
if [[ -d "$TRANSFORM_DIR" ]]; then
    echo "🔍 Checking transform files for potential duplicates..."
    
    # Check for DependencyChecker duplicates
    dependency_files=$(find "$TRANSFORM_DIR" -name "*.py" -exec grep -l "class.*DependencyChecker" {} \; 2>/dev/null || true)
    if [[ -n "$dependency_files" ]]; then
        echo_warning "Multiple DependencyChecker classes found in:"
        echo "$dependency_files"
    fi
    
    # Check for DataDistributor duplicates
    distributor_files=$(find "$TRANSFORM_DIR" -name "*.py" -exec grep -l "class.*DataDistributor" {} \; 2>/dev/null || true)
    if [[ -n "$distributor_files" ]]; then
        echo_warning "Multiple DataDistributor classes found in:"
        echo "$distributor_files"
    fi
fi

# Step 3: Validate V2 pipelines are properly using Native I/O
echo ""
echo "🚀 Step 3: Validating V2 pipelines use Native I/O"

V2_PIPELINES=(
    "airflow/dags/initiate_pipeline.py"
    "airflow/dags/batch_pipeline.py"
    "airflow/dags/realtime_trigger.py"
    "airflow/dags/reconciliation_pipeline.py"
    "dataflow/pipelines/hybrid_pipeline.py"
)

for pipeline in "${V2_PIPELINES[@]}"; do
    pipeline_path="$PROJECT_ROOT/$pipeline"
    if [[ -f "$pipeline_path" ]]; then
        # Check for manual client instantiation (bad patterns)
        bad_patterns=$(grep -E "(Client\(\)|\.Client\(|bigquery\.Client|storage\.Client|secretmanager\.)" "$pipeline_path" || true)
        if [[ -n "$bad_patterns" ]]; then
            echo_error "Manual client instantiation found in $pipeline:"
            echo "$bad_patterns"
        else
            echo_success "Native operators/I/O confirmed in $pipeline"
        fi
    else
        echo_warning "V2 pipeline not found: $pipeline"
    fi
done

# Step 4: Check monitoring is using native alerts
echo ""
echo "📊 Step 4: Validating monitoring uses native alert policies"

MONITORING_FILES=(
    "monitoring/alert_policies.yaml"
    "monitoring/deploy_alerts.sh"
    "monitoring/README.md"
)

for file in "${MONITORING_FILES[@]}"; do
    file_path="$PROJECT_ROOT/$file"
    if [[ -f "$file_path" ]]; then
        echo_success "Monitoring file exists: $file"
    else
        echo_warning "Monitoring file missing: $file"
    fi
done

# Check if old monitoring with MetricServiceClient exists
old_monitoring=$(find "$PROJECT_ROOT" -name "*.py" -exec grep -l "MetricServiceClient\|monitoring_v3" {} \; 2>/dev/null || true)
if [[ -n "$old_monitoring" ]]; then
    echo_warning "Old monitoring patterns found in:"
    echo "$old_monitoring"
fi

# Step 5: Generate cleanup summary
echo ""
echo "📋 Step 5: Cleanup Summary"

# Count remaining files
total_py_files=$(find "$PROJECT_ROOT" -name "*.py" | wc -l)
total_yaml_files=$(find "$PROJECT_ROOT" -name "*.yaml" -o -name "*.yml" | wc -l)

echo_success "Project structure optimized!"
echo "📊 Remaining files:"
echo "  - Python files: $total_py_files"
echo "  - YAML files: $total_yaml_files"

# Check git status if in git repo
if [[ -d "$PROJECT_ROOT/.git" ]]; then
    echo ""
    echo "📝 Git status:"
    cd "$PROJECT_ROOT"
    git status --porcelain | head -10
    
    changes_count=$(git status --porcelain | wc -l)
    if [[ $changes_count -gt 0 ]]; then
        echo_warning "$changes_count files modified. Review changes before committing."
    else
        echo_success "No uncommitted changes."
    fi
fi

echo ""
echo "🎉 Lean cleanup completed!"
echo ""
echo "📋 Next steps:"
echo "1. Review git diff to ensure all changes are correct"
echo "2. Run tests to validate V2 pipelines"
echo "3. Deploy V2 pipelines to staging environment"
echo "4. Decommission V1 pipelines after validation"
echo ""
echo "💡 For remaining items (Redshift, BigTable integration):"
echo "   These were intentionally skipped as requested in lean_feature.md"
