#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

# ---- Tag/Label Defaults ----
: "${RESOURCE_LABEL_PROJECT:=project=demo-the1}"
RESOURCE_LABEL_EQ="${RESOURCE_LABEL_PROJECT}"            # project=demo-the1
RESOURCE_LABEL_COLON="${RESOURCE_LABEL_EQ//=/:}"        # project:demo-the1

echo "== Enable required APIs =="
gcloud services enable \
  iam.googleapis.com iamcredentials.googleapis.com serviceusage.googleapis.com \
  storage.googleapis.com bigquery.googleapis.com dataplex.googleapis.com datalineage.googleapis.com \
  pubsub.googleapis.com composer.googleapis.com dataflow.googleapis.com secretmanager.googleapis.com \
  storagetransfer.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

echo "== Create Service Accounts (if not exists) =="
gcloud iam service-accounts create "$SA_COMPOSER_NAME" \
  --display-name="$SA_COMPOSER_NAME" \
  --description="$RESOURCE_LABEL_EQ" || true
gcloud iam service-accounts create "$SA_DATAFLOW_NAME" \
  --display-name="$SA_DATAFLOW_NAME" \
  --description="$RESOURCE_LABEL_EQ" || true
# ensure descriptions
gcloud iam service-accounts update "$SA_COMPOSER" --description="$RESOURCE_LABEL_EQ" || true
gcloud iam service-accounts update "$SA_DATAFLOW" --description="$RESOURCE_LABEL_EQ" || true

echo "== Grant roles to SA (Composer Orchestrator) =="
for role in \
  roles/composer.worker \
  roles/dataflow.developer \
  roles/pubsub.editor \
  roles/secretmanager.secretAccessor \
  roles/storagetransfer.admin
do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_COMPOSER" --role="$role" --quiet
done
# ให้สิทธิ์เรียกใช้ SA ของ Dataflow (ถ้าจะรันงานด้วย SA_DATAFLOW)
gcloud iam service-accounts add-iam-policy-binding "$SA_DATAFLOW" \
  --member="serviceAccount:$SA_COMPOSER" \
  --role="roles/iam.serviceAccountUser" --quiet

echo "== Grant roles to SA (Dataflow Worker) =="
for role in \
  roles/dataflow.worker \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/storage.objectAdmin \
  roles/pubsub.subscriber \
  roles/pubsub.publisher \
  roles/secretmanager.secretAccessor
do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_DATAFLOW" --role="$role" --quiet
done

echo "== Create GCS Buckets (and label) =="
for b in "$BUCKET_RAW" "$BUCKET_STAGING" "$BUCKET_REFINED" "$BUCKET_ANALYTICS" "$BUCKET_ARTIFACTS"; do
  gsutil ls "$b" >/dev/null 2>&1 || gsutil mb -l "$REGION" "$b"
  # Add/ensure label on bucket
  gsutil label ch -l "${RESOURCE_LABEL_COLON}" "$b" || gsutil label set -l "${RESOURCE_LABEL_COLON}" "$b"
done

echo "== Create BigQuery Datasets (and label) =="
for ds in "$BQ_DS_STAGING" "$BQ_DS_RAW" "$BQ_DS_STRUCTURE" "$BQ_DS_REFINED" "$BQ_DS_ANALYTICS"; do
  bq --location="$BQ_LOCATION" mk --dataset --description="$ds dataset" --label "${RESOURCE_LABEL_COLON}" "$PROJECT_ID:$ds" || true
  # ensure label when exists
  bq update --set_label "${RESOURCE_LABEL_COLON}" "$PROJECT_ID:$ds" || true
done

echo "== Create BigLake Connection for external tables =="
bq mk --connection --location="$BQ_LOCATION" --connection_type=CLOUD_RESOURCE "$BQ_BIGLAKE_CONN_ID" || true
CONN_SA=$(bq show --format='value(connection.cloudResource.serviceAccountId)' --connection "${PROJECT_ID}.${BQ_LOCATION}.${BQ_BIGLAKE_CONN_ID}")
# ให้สิทธิ์อ่าน object บน RAW/STAGING
gsutil iam ch "serviceAccount:${CONN_SA}:roles/storage.objectViewer" "${BUCKET_RAW}" || true
gsutil iam ch "serviceAccount:${CONN_SA}:roles/storage.objectViewer" "${BUCKET_STAGING}" || true

echo "== Create Pub/Sub topics & subscriptions (with labels) =="
gcloud pubsub topics create "$PS_TOPIC_CREATE" --labels="$RESOURCE_LABEL_EQ" \
  || gcloud pubsub topics update "$PS_TOPIC_CREATE" --update-labels="$RESOURCE_LABEL_EQ"
gcloud pubsub topics create "$PS_TOPIC_UPDATE" --labels="$RESOURCE_LABEL_EQ" \
  || gcloud pubsub topics update "$PS_TOPIC_UPDATE" --update-labels="$RESOURCE_LABEL_EQ"
gcloud pubsub subscriptions create "$PS_SUB_CREATE" --topic="$PS_TOPIC_CREATE" --labels="$RESOURCE_LABEL_EQ" \
  || gcloud pubsub subscriptions update "$PS_SUB_CREATE" --update-labels="$RESOURCE_LABEL_EQ"
gcloud pubsub subscriptions create "$PS_SUB_UPDATE" --topic="$PS_TOPIC_UPDATE" --labels="$RESOURCE_LABEL_EQ" \
  || gcloud pubsub subscriptions update "$PS_SUB_UPDATE" --update-labels="$RESOURCE_LABEL_EQ"

echo "== Prepare Secret Manager values (create if missing + label) =="
for sec in "$SEC_AWS_ACCESS_KEY" "$SEC_AWS_SECRET_KEY" "$SEC_REDSHIFT_JDBC_URL" "$SEC_REDSHIFT_USER" "$SEC_REDSHIFT_PASSWORD"; do
  gcloud secrets describe "$sec" >/dev/null 2>&1 || gcloud secrets create "$sec" --labels="$RESOURCE_LABEL_EQ"
  # ensure label when exists
  gcloud secrets update "$sec" --update-labels="$RESOURCE_LABEL_EQ" >/dev/null 2>&1 || true
done
# ตัวอย่าง: เพิ่มเวอร์ชันค่าได้ตามต้องการ
# echo -n "<value>" | gcloud secrets versions add "$SEC_AWS_ACCESS_KEY" --data-file=-

echo "== Generate STS creds.json from secrets (local file for CLI) =="
AWS_KEY=$(gcloud secrets versions access latest --secret="$SEC_AWS_ACCESS_KEY" || true)
AWS_SEC=$(gcloud secrets versions access latest --secret="$SEC_AWS_SECRET_KEY" || true)
cat > "$STS_CREDS_FILE" <<EOF
{ "accessKeyId": "${AWS_KEY}", "secretAccessKey": "${AWS_SEC}" }
EOF

echo "== Create/Update Cloud Composer environment (with labels) =="
if ! gcloud composer environments describe "$COMPOSER_ENV_NAME" --location "$COMPOSER_REGION" >/dev/null 2>&1; then
  gcloud composer environments create "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --image-version "$COMPOSER_IMAGE_VERSION" \
    --service-account "$SA_COMPOSER" \
    --labels="$RESOURCE_LABEL_EQ"
else
  gcloud composer environments update "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --update-labels="$RESOURCE_LABEL_EQ" || true
fi

echo "== DONE: deploy_env =="

# (Optional) Create STS jobs per table with description for tagging
while IFS=',' read -r zone table s3p gcsp; do
  [ -z "$zone" ] && continue
  NAME_INIT="${zone}_${table}_s3_gcs_init"
  NAME_RECO="${zone}_${table}_s3_gcs_reco"
  # Initiate (one-time, do-not-run)
  gcloud transfer jobs create "s3://${s3p}" "${BUCKET_RAW}/${gcsp}/" \
    --source-creds-file="${STS_CREDS_FILE}" \
    --name="$NAME_INIT" \
    --do-not-run \
    --description="$RESOURCE_LABEL_EQ"
  # Reconciled (schedule hourly example)
  gcloud transfer jobs create "s3://${s3p}" "${BUCKET_RAW}/${gcsp}/" \
    --source-creds-file="${STS_CREDS_FILE}" \
    --name="$NAME_RECO" \
    --schedule-repeats-every=3600s \
    --description="$RESOURCE_LABEL_EQ"
done < "$STS_TABLES_CSV"
