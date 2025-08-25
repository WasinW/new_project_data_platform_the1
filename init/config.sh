#!/usr/bin/env bash
# -------- Global / Project --------
export PROJECT_ID="ntt-test-data-bq-looker"
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export REGION="asia-southeast1"        # Dataflow/Composer region
export BQ_LOCATION="$REGION"           # BigQuery region (ควรตรงกับ bucket ถ้าใช้ BigLake)
export ENV="dev"
export DOMAIN="the1"

# -------- Service Accounts --------
export SA_COMPOSER_NAME="sa-composer-orchestrator"
export SA_DATAFLOW_NAME="sa-dataflow-runner"

export SA_COMPOSER="${SA_COMPOSER_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export SA_DATAFLOW="${SA_DATAFLOW_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# -------- Cloud Storage (GCS) --------
export BUCKET_RAW="gs://${PROJECT_ID}-${ENV}-${DOMAIN}-raw"
export BUCKET_STAGING="gs://${PROJECT_ID}-${ENV}-${DOMAIN}-staging"
export BUCKET_REFINED="gs://${PROJECT_ID}-${ENV}-${DOMAIN}-refined"
export BUCKET_ANALYTICS="gs://${PROJECT_ID}-${ENV}-${DOMAIN}-analytics"
export BUCKET_ARTIFACTS="gs://${PROJECT_ID}-${ENV}-${DOMAIN}-artifacts"  # เก็บ templates/jars
export BUCKET_DAGS_DATA_PREFIX="data"    # โฟลเดอร์ data/ ใน Composer bucket

# -------- BigQuery --------
export BQ_DS_STAGING="${DOMAIN}_${ENV}_staging"
export BQ_DS_RAW="${DOMAIN}_${ENV}_raw"
export BQ_DS_STRUCTURE="${DOMAIN}_${ENV}_structure"
export BQ_DS_REFINED="${DOMAIN}_${ENV}_refined"
export BQ_DS_ANALYTICS="${DOMAIN}_${ENV}_analytics"

# BigLake Connection (สำหรับ external tables)
export BQ_BIGLAKE_CONN_ID="${DOMAIN}_${ENV}_cr"  # cloud resource connection id (CLOUD_RESOURCE)

# -------- Pub/Sub --------
export PS_TOPIC_CREATE="${DOMAIN}-${ENV}-create"
export PS_TOPIC_UPDATE="${DOMAIN}-${ENV}-update"
export PS_SUB_CREATE="${PS_TOPIC_CREATE}-sub"
export PS_SUB_UPDATE="${PS_TOPIC_UPDATE}-sub"

# -------- Secret Manager --------
# ชื่อ secrets ที่เก็บ credentials ต่าง ๆ (ตาม Pipeline Details)
export SEC_AWS_ACCESS_KEY="aws_s3_access_key"
export SEC_AWS_SECRET_KEY="aws_s3_secret_key"
export SEC_REDSHIFT_JDBC_URL="redshift_jdbc_url"
export SEC_REDSHIFT_USER="redshift_user"
export SEC_REDSHIFT_PASSWORD="redshift_password"
export SEC_BQ_SVC_JSON="bq_service_json"          # ถ้าจำเป็น
# คุณใช้ "internal_sa" / "external_sa" ไว้: สามารถสร้างเพิ่มตรงนี้ได้

# -------- Storage Transfer Service (S3 -> GCS) --------
export STS_CREDS_FILE="./aws_creds.json"   # จะ generate จาก secrets ตอน deploy_env
# ตัวอย่าง table list ไฟล์ CSV สำหรับสร้าง jobs (zone,table,s3_prefix,gcs_prefix)
export STS_TABLES_CSV="./config/sts_tables.csv"

# -------- Cloud Composer --------
export COMPOSER_ENV_NAME="${DOMAIN}-${ENV}-composer"
export COMPOSER_REGION="$REGION"
export COMPOSER_IMAGE_VERSION="composer-2-airflow-2"  # ใช้ alias ล่าสุดใน Composer 2 สำหรับ Airflow 2 (แก้เป็นคอนกรีตได้) :contentReference[oaicite:24]{index=24}

# -------- Dataflow --------
# เลือกได้ทั้ง Classic template/JAR (Java) หรือ Python direct/Flex template
export DF_STAGING_LOCATION="${BUCKET_ARTIFACTS}/df-staging"
export DF_TEMP_LOCATION="${BUCKET_ARTIFACTS}/df-tmp"

# -------- Dataplex / Lineage --------
export DATAPLEX_LAKE_ID="${DOMAIN}-${ENV}-lake"
export DATAPLEX_ZONE_STAGING="${DOMAIN}-${ENV}-staging"
export DATAPLEX_ZONE_RAW="${DOMAIN}-${ENV}-raw"
export DATAPLEX_ZONE_REFINED="${DOMAIN}-${ENV}-refined"
export DATAPLEX_ZONE_ANALYTICS="${DOMAIN}-${ENV}-analytics"
