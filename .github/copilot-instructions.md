# AI Coding Agent Instructions for GCP Data Platform

## Project Architecture Overview

This is a **hybrid batch/realtime data platform** migrating from AWS S3 to GCP, orchestrated by **Cloud Composer (Airflow)** with **four distinct pipeline types**:

1. **Initiate Pipeline** (`airflow/dags/initiate_pipeline.py`) - One-time S3→BigQuery migration (pure Airflow, no Dataflow)
2. **Realtime Pipeline** (`airflow/dags/realtime_trigger.py`) - Continuous Pub/Sub processing via Dataflow
3. **Batch Pipeline** (`airflow/dags/batch_pipeline.py`) - Hourly batch processing (transitional)
4. **Reconciliation Pipeline** (`airflow/dags/reconciliation_pipeline.py`) - Daily validation against S3

Key insight: **DAGs are auto-generated** by factory functions (`create_initiate_dag(domain, tables)`) - never hardcode domain/table names.

## Core Configuration Pattern

**YAML-first approach** with environment variable substitution:
- Main config: `config/pipeline_config.yaml` with `${VAR:default}` syntax
- Airflow variables: `airflow/config/airflow_variables.json`
- Environment overrides supported via `ConfigLoader` class

```yaml
# Critical pattern: distribution_mapping defines data flow
distribution_mapping:
  raw_a1: [a, b, c]           # raw_a1 table gets columns a,b,c
  refined_b1: [a, b, c, d, e, f]  # refined_b1 gets a,b,c,d,e,f
```

**Never modify distribution mapping without understanding downstream impact** - it controls which columns flow to which BigQuery tables.

## Data Flow Architecture

**Source → Distributor → Multiple Tables Pattern:**
```
Single Record → DataDistributor → [raw_a1, raw_a2, refined_b1, etc.]
```

Key files:
- `dataflow/transforms/distributor.py` - Core data distribution logic
- `dataflow/transforms/dependency_checker.py` - Upstream table validation
- `dataflow/utils/config_loader.py` - Configuration management with GCS support

**Critical:** Every record gets `_source_timestamp`, `_ingestion_timestamp`, `_element_id`, `_target_table` metadata.

## Dataflow Pipeline Patterns

**Hybrid Pipeline** (`dataflow/pipelines/hybrid_pipeline.py`) supports both modes:
```python
# Mode switching pattern
--mode=batch|realtime --domain=member --config_path=gs://bucket/config.yaml
```

**Dependency checking is mandatory** - use `DependencyChecker` DoFn before processing:
```python
# Pattern: Always check dependencies first
dependency_results = (elements | beam.ParDo(DependencyChecker(deps)).with_outputs('main', 'failed_dependency'))
```

## Terraform Infrastructure Pattern

**Domain-agnostic infrastructure** in `terraform/`:
- Use variables for all domain/environment-specific values
- BigQuery datasets follow `${domain}_raw`, `${domain}_refined` naming
- **Always enable required APIs first** (see `google_project_service` resources)

## Development Workflows

**Setup sequence:**
```bash
export PROJECT_ID="your-project" REGION="asia-southeast1" DOMAIN="member"
./scripts/setup.sh  # Handles gcloud auth, Terraform, and deployment
```

**Testing pattern:**
```bash
# Local Dataflow testing
python -m dataflow.pipelines.hybrid_pipeline --mode=batch --config_path=config/pipeline_config.yaml --runner=DirectRunner

# Deploy to GCP
python -m dataflow.pipelines.hybrid_pipeline --mode=realtime --config_path=gs://config-bucket/config.yaml --runner=DataflowRunner
```

## Critical Debugging Points

**Airflow DAG Factory Pattern:** DAGs are created dynamically - check `Variable.get()` calls for missing Airflow variables.

**Data Distribution Debugging:** Check `_target_table` metadata field to trace which table a record was destined for.

**Configuration Issues:** `ConfigLoader` falls back to default config - check logs for "Falling back to default config" messages.

**Dependency Failures:** Records go to `failed_dependency` output - always handle this in your pipeline.

## Integration Boundaries

- **Airflow ↔ Dataflow:** Via `DataflowTemplatedJobStartOperator` with `--config_path` parameter
- **Pub/Sub ↔ Dataflow:** Subscription names follow `${domain}-events-{create|update}-sub` pattern
- **BigQuery:** External tables for S3 data, native tables for processed data
- **Monitoring:** Custom metrics via `audit_logger.py` to BigQuery audit tables

## Project-Specific Conventions

- **File naming:** `{domain}_{layer}_{number}` (e.g., `member_raw_a1`, `member_refined_b1`)
- **Timestamp fields:** Always UTC ISO format, use `datetime.utcnow().isoformat()`
- **Error handling:** Dead letter queues for failed records, comprehensive audit logging
- **Configuration:** Environment variables override YAML defaults, GCS for production configs

When modifying this codebase, always consider the **domain-agnostic design** - changes should work for any domain (member, product, etc.) without hardcoding.
