#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

# ---- Tag/Label Defaults ----
: "${RESOURCE_LABEL_PROJECT:=project=demo-the1}"

echo "== Copy helper scripts/templates to GCS artifacts bucket =="
gsutil -m rsync -r "./scripts" "${BUCKET_ARTIFACTS}/scripts"

echo "== Create Audit tables (BigQuery) with labels =="
# หมายเหตุ: ถ้าต้องการให้ตัวแปรใน DDL ถูกแทนค่า ให้ใช้ <<SQL (ไม่ใส่ single quote)
bq query --use_legacy_sql=false <<'SQL'
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit`
OPTIONS (
  labels=[("project","demo-the1")]
);

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit.job_run` (
  job_id STRING, pipeline STRING, params JSON, status STRING,
  started_at TIMESTAMP, ended_at TIMESTAMP, error STRING
)
OPTIONS (labels=[("project","demo-the1")]);

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit.lineage_log` (
  table_fqn STRING, upstream STRING, process STRING, run_id STRING, logged_at TIMESTAMP
)
OPTIONS (labels=[("project","demo-the1")]);

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit.event_log` (
  event_time TIMESTAMP, event_type STRING, detail JSON
)
OPTIONS (labels=[("project","demo-the1")]);
SQL

echo "== (Optional) Build Dataflow artifacts =="
if [ -f "./dataflow/pom.xml" ]; then
  (cd dataflow && mvn -q -DskipTests package)
  JAR_PATH=$(ls dataflow/target/*.jar | head -n1)
  gsutil cp "$JAR_PATH" "${BUCKET_ARTIFACTS}/dataflow/"
fi

if [ -d "./dataflow/python" ]; then
  gsutil -m rsync -r "./dataflow/python" "${BUCKET_ARTIFACTS}/dataflow/python"
fi

echo "== DONE: deploy_framework =="
