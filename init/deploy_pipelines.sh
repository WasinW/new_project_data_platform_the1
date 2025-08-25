#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

# ---- Tag/Label Defaults ----
: "${RESOURCE_LABEL_PROJECT:=project=demo-the1}"
RESOURCE_LABEL_EQ="${RESOURCE_LABEL_PROJECT}"
RESOURCE_LABEL_COLON="${RESOURCE_LABEL_EQ//=/:}"

echo "== Resolve Composer DAGs bucket prefix =="
DAGS_PREFIX=$(gcloud composer environments describe "$COMPOSER_ENV_NAME" --location "$COMPOSER_REGION" --format="get(config.dagGcsPrefix)")
echo "DAGs GCS prefix: $DAGS_PREFIX"

# ensure Composer env has label
gcloud composer environments update "$COMPOSER_ENV_NAME" \
  --location "$COMPOSER_REGION" \
  --update-labels="$RESOURCE_LABEL_EQ" || true

# label the Composer DAG bucket
DAGS_BUCKET="$(echo "$DAGS_PREFIX" | sed -E 's#gs://([^/]+)/.*#\1#')"
gsutil label ch -l "${RESOURCE_LABEL_COLON}" "gs://${DAGS_BUCKET}" || true

echo "== Import DAGs (directory) =="
gcloud composer environments storage dags import \
  --environment "$COMPOSER_ENV_NAME" \
  --location "$COMPOSER_REGION" \
  --source "./airflow/dags"

echo "== Import Plugins (if any) =="
if [ -d "./airflow/plugins" ]; then
  gcloud composer environments storage plugins import \
    --environment "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --source "./airflow/plugins"
fi

echo "== Upload & import Airflow Variables (JSON) =="
if [ -f "./config/airflow_variables.json" ]; then
  gcloud composer environments storage data import \
    --environment "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --source "./config/airflow_variables.json" \
    --destination "${BUCKET_DAGS_DATA_PREFIX}/airflow_variables.json"
  gcloud composer environments run "$COMPOSER_ENV_NAME" --location "$COMPOSER_REGION" variables \
    -- --import "/home/airflow/gcs/data/airflow_variables.json"
fi

echo "== DONE: deploy_pipelines =="
