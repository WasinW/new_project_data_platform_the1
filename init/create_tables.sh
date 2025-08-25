#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

echo "== Create BigLake external tables for RAW/STAGING (with labels) =="
bq query --use_legacy_sql=false <<SQL
CREATE EXTERNAL TABLE IF NOT EXISTS \`${PROJECT_ID}.${BQ_DS_RAW}.sample_raw\`
WITH CONNECTION \`${PROJECT_ID}.${BQ_LOCATION}.${BQ_BIGLAKE_CONN_ID}\`
OPTIONS (
  uris = ['${BUCKET_RAW}/sample_raw/*'],
  format = 'PARQUET',
  object_metadata = 'DIRECTORY',
  labels = [("project","demo-the1")]
);
SQL

echo "== Create native tables for REFINED/ANALYTICS (with labels) =="
bq query --use_legacy_sql=false <<'SQL'
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_DS_REFINED}.member_profile` (
  member_id STRING, email STRING, updated_at TIMESTAMP
) PARTITION BY DATE(updated_at)
OPTIONS (labels=[("project","demo-the1")]);

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_DS_ANALYTICS}.member_kpi` (
  snapshot_date DATE, active_members INT64, churn_rate NUMERIC
)
OPTIONS (labels=[("project","demo-the1")]);
SQL

echo "== DONE: create_tables =="
