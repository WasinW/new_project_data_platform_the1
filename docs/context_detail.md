เยี่ยมมากครับ ภาพรวม requirement จาก **List\_service** และ **Pipeline Details** ที่คุณแนบมาเคลียร์ดีแล้ว และเหมาะกับวิธี “ง่ายก่อน” ที่ต้องการใช้ shell/CLI แทน CI/CD เต็มรูปแบบในระยะแรก ผมสรุปคำตอบให้ครบ 4 ส่วนหลักตามที่ถาม โดยผูกกับสิ่งที่อยู่ใน Git + เอกสารของคุณ และแนบสคริปต์ตัวอย่างพร้อมแก้ไขตัวแปรจากไฟล์เดียว

> แหล่งอ้างอิงจากไฟล์ของคุณ: รายการบริการและโซน/เลเยอร์ข้อมูลอยู่ใน **List\_service.md** และลอจิกของ 4 pipeline (initiate/realtime/batch/reconciled) + การใช้ Secret/STS อยู่ใน **Pipeline Details.md** ครับ &#x20;

---

## 1) รายการ **Services / Service accounts / Permissions** ที่ต้องใช้ (แบบละเอียด)

ด้านล่างผมให้ 2 ตัวเลือก: **Simple (บัญชีเดียว)** ตามที่คุณตั้งใจ “รวมทุก service ใน account เดียว” และ **Better (แยกหน้าที่)** เพื่อรองรับ least privilege/scale-ups ภายหลัง

### 1.1 APIs ที่ต้องเปิดใช้งาน (ทุกแบบ)

* `iam.googleapis.com`, `iamcredentials.googleapis.com`
* `serviceusage.googleapis.com`
* `storage.googleapis.com`
* `bigquery.googleapis.com`
* `dataplex.googleapis.com`
* `datalineage.googleapis.com` (เพื่อให้ Dataplex Universal Catalog แสดง lineage + ใช้ API หากต้องบันทึก lineage เอง) ([Google Cloud][1])
* `pubsub.googleapis.com`
* `composer.googleapis.com`
* `dataflow.googleapis.com`
* `secretmanager.googleapis.com` ([Google Cloud][2])
* `storagetransfer.googleapis.com` (Storage Transfer Service – STS) ([Google Cloud][3])
* (ถ้าใช้ Artifact Registry/Docker image สำหรับ Flex templates) `artifactregistry.googleapis.com`, `cloudbuild.googleapis.com`

### 1.2 บัญชีบริการ (Service Accounts)

**Simple (ตามเอกสารคุณ)**

* `sa-internal` (orchestrator) — ใช้กับ Cloud Composer/Airflow เพื่อ:

  * trigger Dataflow, STS, Pub/Sub, BigQuery, GCS, Dataplex/Lineage, Secret Manager
  * Roles แนะนำ (ระดับโปรเจกต์ เว้นแต่ระบุถึงทรัพยากรเฉพาะ):

    * `roles/composer.worker` (จำเป็นเมื่อใช้เป็น SA ของ Composer env) ([Google Cloud][4])
    * `roles/iam.serviceAccountUser` บน `sa-dataflow` (ถ้าจะสั่ง Dataflow ให้รันด้วย SA อื่น) ([Google Cloud][5])
    * `roles/dataflow.developer` (สิทธิ์ client เรียกสร้าง/จัดการงาน Dataflow) ([Google Cloud][5])
    * `roles/bigquery.jobUser`, `roles/bigquery.dataEditor` (งานที่แก้/สร้างตารางได้) ([Google Cloud][5])
    * `roles/storage.admin` (ง่ายสุดช่วงแรก หากอยากลดให้เป็น `storage.objectAdmin` ก็ได้) ([Google Cloud][6])
    * `roles/pubsub.editor` (สร้าง topic/sub และ publish/subscribe เบื้องต้น) ([Google Cloud][7])
    * `roles/secretmanager.secretAccessor` (อ่าน secret) และถ้าจะสร้าง/อัปเวอร์ชัน secret ให้ใช้ `roles/secretmanager.admin` ในช่วง bootstrap เท่านั้น แล้วค่อยลดเหลือ accessor ภายหลัง ([Google Cloud][2])
    * `roles/storagetransfer.admin` (สร้าง/รัน STS jobs) ([Google Cloud][3])
    * (ถ้าต้องเขียน lineage เองผ่าน API) `roles/datalineage.admin` หรืออย่างน้อย editor ของ Data Lineage API; แต่ถ้าดูอย่างเดียว `roles/datalineage.viewer` พอ ([Google Cloud][8])

* `sa-external` (ฝั่ง AWS/Redshift) — ตามเอกสารคุณ จะเก็บคีย์/เครเดนเชียลไว้ใน Secret Manager (บน GCP) สำหรับ STS + JDBC/ODBC ไป Redshift; **บน AWS** ให้สร้าง IAM user/role ตาม best practice ของ STS S3-to-GCS (access key/secret key อย่างน้อย read objects) ([Google Cloud][9])

**Better (Least privilege / แยกหน้าที่)**

* `sa-composer-orchestrator` — roles: `composer.worker`, `dataflow.developer`, `pubsub.editor`, `secretmanager.secretAccessor`, `storagetransfer.admin`, `iam.serviceAccountUser` (บน `sa-dataflow-runner`)
* `sa-dataflow-runner` — ใช้เป็น **worker** ของทุก Dataflow job
  Roles ขั้นต่ำ:

  * `roles/dataflow.worker` (จำเป็นกับ worker) ([Google Cloud][5])
  * `roles/bigquery.dataEditor`, `roles/bigquery.jobUser` (อ่าน/เขียน/โหลดงาน) ([Google Cloud][7])
  * `roles/storage.objectAdmin` (อ่าน/เขียน staging/temp + raw/staging bucket) ([Google Cloud][7])
  * `roles/pubsub.subscriber` + `roles/pubsub.publisher` (กรณี streaming consume/emit) ([Google Cloud][7])
  * `roles/secretmanager.secretAccessor` (อ่าน secrets)
  * (ถ้าต้องบันทึก lineage เอง) `roles/datalineage.admin` หรือ writer ตามต้องการบันทึกผ่าน API ([Google Cloud][8])

> หมายเหตุ: Cloud Composer เองมี **service agent** และข้อกำหนดเรื่อง image/version — เวลาสร้าง environment ให้ระบุ image เวอร์ชันตามรายการที่รองรับใน region นั้น ๆ และผูกกับ SA ที่ให้ `composer.worker` แล้วจึงเพิ่ม roles อื่นสำหรับ DAG ที่ต้องแตะทรัพยากรคนละชุดภายหลัง ([Google Cloud][10])

### 1.3 Storage Transfer Service (S3 → GCS)

* CLI: `gcloud transfer jobs create s3://<S3_BUCKET>/<prefix> gs://<GCS_BUCKET>/<prefix>/ --source-creds-file=creds.json [ตัวเลือก schedule/filters]`
  (รองรับ manifest, schedule, overwrite/delete options) ([Google Cloud][3])
* ให้สิทธิ์ **Storage Transfer Service service agent**/service account ของโครงการเข้าถึง bucket ปลายทาง (GCS) ตามที่เอกสารแนะนำ (อย่างน้อย objectAdmin ที่ bucket ปลายทาง) ([Google Cloud][3])

### 1.4 BigQuery External/BigLake (โซน `staging/raw`)

* ใช้ **BigLake external table** + **BigQuery connection (CLOUD\_RESOURCE)** เพื่อ delegated access จาก BQ ไปยัง GCS — ขั้นตอน:

  1. `bq mk --connection --location=<region> --connection_type=CLOUD_RESOURCE <CONN_ID>`
  2. เอา **service account ของ connection** ที่ได้จาก `bq show --connection` ไปให้สิทธิ์ `storage.objectViewer` เฉพาะ path ที่ต้องอ่านใน GCS
  3. สร้างตารางแบบ BigLake ชี้ไป Parquet/JSON บน GCS (ใช้ `CREATE EXTERNAL TABLE ... WITH CONNECTION ... OPTIONS (uris=[...], format='PARQUET', object_metadata='DIRECTORY')`) ([Google Cloud][11])

---

## 2) **สคริปต์ Shell / CLI** (ง่าย ๆ แก้ตัวแปรที่ไฟล์เดียว)

> โครงไฟล์:
>
> ```
> deploy_env.sh          # Process 1: เตรียม environment + infra
> deploy_framework.sh    # Process 2: เตรียม framework/platform + audit tables + อัปโหลดสคริปต์
> deploy_pipelines.sh    # Process 3: อัปโหลด/ตั้งค่า Airflow DAG/Plugins/Variables
> create_tables.sh       # Process 4: สร้างตารางตามโซน
> config.sh              # รวมตัวแปรทั้งหมด แยกเป็นหมวดตาม service
> ```
>
> หมายเหตุ: ทุกสคริปต์ทำให้ “idempotent” เท่าที่ทำได้ (เช็คก่อนสร้าง, ใช้ `|| true`) และ echo ขั้นตอนชัดเจน

### 2.1 `config.sh`  (ตัวอย่าง – แยกตาม service)

```bash
#!/usr/bin/env bash
# -------- Global / Project --------
export PROJECT_ID="your-gcp-project"
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
```

### 2.2 `deploy_env.sh`  (Process 1 – เตรียม Environment/Infra)

```bash
#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

echo "== Enable required APIs =="
gcloud services enable \
  iam.googleapis.com iamcredentials.googleapis.com serviceusage.googleapis.com \
  storage.googleapis.com bigquery.googleapis.com dataplex.googleapis.com datalineage.googleapis.com \
  pubsub.googleapis.com composer.googleapis.com dataflow.googleapis.com secretmanager.googleapis.com \
  storagetransfer.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com

echo "== Create Service Accounts (if not exists) =="
gcloud iam service-accounts create "$SA_COMPOSER_NAME" --display-name="$SA_COMPOSER_NAME" || true
gcloud iam service-accounts create "$SA_DATAFLOW_NAME" --display-name="$SA_DATAFLOW_NAME" || true

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

echo "== Create GCS Buckets =="
for b in "$BUCKET_RAW" "$BUCKET_STAGING" "$BUCKET_REFINED" "$BUCKET_ANALYTICS" "$BUCKET_ARTIFACTS"; do
  gsutil ls "$b" >/dev/null 2>&1 || gsutil mb -l "$REGION" "$b"
done

echo "== Create BigQuery Datasets =="
for ds in "$BQ_DS_STAGING" "$BQ_DS_RAW" "$BQ_DS_STRUCTURE" "$BQ_DS_REFINED" "$BQ_DS_ANALYTICS"; do
  bq --location="$BQ_LOCATION" mk --dataset --description="$ds dataset" "$PROJECT_ID:$ds" || true
done

echo "== Create BigLake Connection for external tables =="
bq mk --connection --location="$BQ_LOCATION" --connection_type=CLOUD_RESOURCE "$BQ_BIGLAKE_CONN_ID" || true
CONN_SA=$(bq show --format='value(connection.cloudResource.serviceAccountId)' --connection "${PROJECT_ID}.${BQ_LOCATION}.${BQ_BIGLAKE_CONN_ID}")
# ให้สิทธิ์อ่าน object บน RAW/STAGING
gsutil iam ch "serviceAccount:${CONN_SA}:roles/storage.objectViewer" "${BUCKET_RAW}" || true
gsutil iam ch "serviceAccount:${CONN_SA}:roles/storage.objectViewer" "${BUCKET_STAGING}" || true
# เอกสาร BigLake/Connection: :contentReference[oaicite:25]{index=25}

echo "== Create Pub/Sub topics & subscriptions =="
gcloud pubsub topics create "$PS_TOPIC_CREATE" || true
gcloud pubsub topics create "$PS_TOPIC_UPDATE" || true
gcloud pubsub subscriptions create "$PS_SUB_CREATE" --topic="$PS_TOPIC_CREATE" || true
gcloud pubsub subscriptions create "$PS_SUB_UPDATE" --topic="$PS_TOPIC_UPDATE" || true

echo "== Prepare Secret Manager values (create if missing) =="
# ตัวอย่างการสร้าง secret เปล่า (เวอร์ชันยังไม่ใส่)
for sec in "$SEC_AWS_ACCESS_KEY" "$SEC_AWS_SECRET_KEY" "$SEC_REDSHIFT_JDBC_URL" "$SEC_REDSHIFT_USER" "$SEC_REDSHIFT_PASSWORD"; do
  gcloud secrets describe "$sec" >/dev/null 2>&1 || gcloud secrets create "$sec"
done
# ตัวอย่าง: ถ้าคุณมีค่าพร้อมแล้วให้เพิ่มเวอร์ชันทันที
# echo -n "<value>" | gcloud secrets versions add "$SEC_AWS_ACCESS_KEY" --data-file=-

echo "== Generate STS creds.json from secrets (local file for CLI) =="
AWS_KEY=$(gcloud secrets versions access latest --secret="$SEC_AWS_ACCESS_KEY" || true)
AWS_SEC=$(gcloud secrets versions access latest --secret="$SEC_AWS_SECRET_KEY" || true)
cat > "$STS_CREDS_FILE" <<EOF
{ "accessKeyId": "${AWS_KEY}", "secretAccessKey": "${AWS_SEC}" }
EOF

echo "== Create Cloud Composer environment (if not exists) =="
if ! gcloud composer environments describe "$COMPOSER_ENV_NAME" --location "$COMPOSER_REGION" >/dev/null 2>&1; then
  gcloud composer environments create "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --image-version "$COMPOSER_IMAGE_VERSION" \
    --service-account "$SA_COMPOSER"
  # อ้างอิงการสร้าง env และเวอร์ชัน: :contentReference[oaicite:26]{index=26}
fi

echo "== DONE: deploy_env =="
```

> ถ้าคุณต้องการ “สร้าง Storage Transfer jobs” ไว้ล่วงหน้าตามแนวคิด **initiate** กับ **reconciled** ต่อ table (2 jobs/ตาราง) สามารถเพิ่มบล็อกนี้ท้าย `deploy_env.sh` หรือแยกไฟล์ `create_sts_jobs.sh` ก็ได้:

```bash
# ตัวอย่างอ่านจาก CSV: zone,table,s3_prefix,gcs_prefix
while IFS=',' read -r zone table s3p gcsp; do
  [ -z "$zone" ] && continue
  NAME_INIT="${zone}_${table}_s3_gcs_init"
  NAME_RECO="${zone}_${table}_s3_gcs_reco"
  # Initiate (one-time, do-not-run)
  gcloud transfer jobs create "s3://${s3p}" "${BUCKET_RAW}/${gcsp}/" \
    --source-creds-file="${STS_CREDS_FILE}" \
    --name="$NAME_INIT" --do-not-run
  # Reconciled (schedule hourly example)
  gcloud transfer jobs create "s3://${s3p}" "${BUCKET_RAW}/${gcsp}/" \
    --source-creds-file="${STS_CREDS_FILE}" \
    --name="$NAME_RECO" \
    --schedule-repeats-every=3600s
done < "$STS_TABLES_CSV"
# คำสั่งอ้างอิง STS CLI: :contentReference[oaicite:27]{index=27}
```

### 2.3 `deploy_framework.sh`  (Process 2 – เตรียม Framework/Platform + Audit)

```bash
#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

echo "== Copy helper scripts/templates to GCS artifacts bucket =="
gsutil -m rsync -r "./scripts" "${BUCKET_ARTIFACTS}/scripts"

echo "== Create Audit tables (BigQuery) =="
# คุณสามารถแยกไฟล์ .sql ไว้ใน ./sql/audit/*.sql
# ตัวอย่าง DDL คร่าว ๆ:
bq query --use_legacy_sql=false <<'SQL'
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit`;
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit.job_run` (
  job_id STRING, pipeline STRING, params JSON, status STRING,
  started_at TIMESTAMP, ended_at TIMESTAMP, error STRING
);
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit.lineage_log` (
  table_fqn STRING, upstream STRING, process STRING, run_id STRING, logged_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DOMAIN}_${ENV}_audit.event_log` (
  event_time TIMESTAMP, event_type STRING, detail JSON
);
SQL

echo "== (Optional) Build Dataflow artifacts =="
# กรณี Java (Maven) -> สร้าง JAR แล้วอัปโหลด:
if [ -f "./dataflow/pom.xml" ]; then
  (cd dataflow && mvn -q -DskipTests package)
  JAR_PATH=$(ls dataflow/target/*.jar | head -n1)
  gsutil cp "$JAR_PATH" "${BUCKET_ARTIFACTS}/dataflow/"
fi

# กรณี Python (direct run หรือ Flex template): เริ่มจากอัปโหลด source ไปเก็บก่อน
if [ -d "./dataflow/python" ]; then
  gsutil -m rsync -r "./dataflow/python" "${BUCKET_ARTIFACTS}/dataflow/python"
  # ถ้าภายหลังอยากทำ Flex Template ให้เพิ่มขั้นตอน build image + "gcloud dataflow flex-template build"
  # (เกี่ยวข้อง Artifact Registry/Cloud Build)
fi

echo "== DONE: deploy_framework =="
```

### 2.4 `deploy_pipelines.sh`  (Process 3 – อัปโหลด Airflow pipelines)

```bash
#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

echo "== Resolve Composer DAGs bucket prefix =="
DAGS_PREFIX=$(gcloud composer environments describe "$COMPOSER_ENV_NAME" --location "$COMPOSER_REGION" --format="get(config.dagGcsPrefix)")
echo "DAGs GCS prefix: $DAGS_PREFIX"
# อ้างอิงวิธีดึง dag bucket + import DAG: :contentReference[oaicite:28]{index=28}

echo "== Import DAGs (directory) =="
gcloud composer environments storage dags import \
  --environment "$COMPOSER_ENV_NAME" \
  --location "$COMPOSER_REGION" \
  --source "./airflow/dags"   # รองรับอัปโหลดทั้งโฟลเดอร์

echo "== Import Plugins (if any) =="
if [ -d "./airflow/plugins" ]; then
  gcloud composer environments storage plugins import \
    --environment "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --source "./airflow/plugins"
fi
# อ้างอิง plugins import: :contentReference[oaicite:29]{index=29}

echo "== Upload & import Airflow Variables (JSON) =="
if [ -f "./config/airflow_variables.json" ]; then
  # อัปโหลดไฟล์ variables ไปที่ data/
  gcloud composer environments storage data import \
    --environment "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_REGION" \
    --source "./config/airflow_variables.json" \
    --destination "${BUCKET_DAGS_DATA_PREFIX}/airflow_variables.json"
  # สั่ง Airflow CLI ผ่าน composer เพื่อนำเข้า variables
  gcloud composer environments run "$COMPOSER_ENV_NAME" --location "$COMPOSER_REGION" variables \
    -- --import "/home/airflow/gcs/data/airflow_variables.json"
fi
# อ้างอิง command การ import variables ผ่าน gcloud composer env run: :contentReference[oaicite:30]{index=30}

echo "== DONE: deploy_pipelines =="
```

### 2.5 `create_tables.sh`  (Process 4 – สร้างตารางแต่ละโซน)

```bash
#!/usr/bin/env bash
set -euo pipefail
source "./config.sh"
gcloud config set project "$PROJECT_ID"

echo "== Create BigLake external tables for RAW/STAGING =="
# ตัวอย่าง: ชี้ไฟล์ Parquet ภายใต้ RAW bucket โดยใช้ Connection ที่สร้างไว้
bq query --use_legacy_sql=false <<SQL
CREATE EXTERNAL TABLE IF NOT EXISTS \`${PROJECT_ID}.${BQ_DS_RAW}.sample_raw\`
WITH CONNECTION \`${PROJECT_ID}.${BQ_LOCATION}.${BQ_BIGLAKE_CONN_ID}\`
OPTIONS (
  uris = ['${BUCKET_RAW}/sample_raw/*'],
  format = 'PARQUET',
  object_metadata = 'DIRECTORY'
);
SQL
# อ้างอิงการสร้าง BigLake external table + connection: :contentReference[oaicite:31]{index=31}

echo "== Create native tables for REFINED/ANALYTICS (examples) =="
bq query --use_legacy_sql=false <<'SQL'
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_DS_REFINED}.member_profile` (
  member_id STRING, email STRING, updated_at TIMESTAMP
) PARTITION BY DATE(updated_at);

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${BQ_DS_ANALYTICS}.member_kpi` (
  snapshot_date DATE, active_members INT64, churn_rate NUMERIC
);
SQL

echo "== DONE: create_tables =="
```

> **หมายเหตุสำคัญ**: ถ้าต้องอัปโหลดไฟล์ `.sql` จำนวนมาก ให้ใส่ไว้ใน `./sql/tables/<zone>/*.sql` แล้ววน `for f in ...; do bq query --use_legacy_sql=false "$(cat "$f")"; done`

---

## 3) **รีวิว Git (branch: feature/refactor\_param) + ข้อเสนอปรับปรุง**

> ผมพยายามเปิดดูโครง repo ที่ลิงก์มา แต่ด้วยข้อจำกัดการโหลดหน้า dynamic ของ GitHub ผ่านเครื่องมือ จึงยังเข้าถึงไฟล์ภายในโฟลเดอร์ย่อยไม่ได้ทั้งหมดในรอบนี้ อย่างไรก็ดี จากรายละเอียดในไฟล์ **List\_service.md**/**Pipeline Details.md** (และโครงแบบแผนที่คุณสื่อ) ผมรีวิวเชิงสถาปัตยกรรม/โครงสร้างและ expected behavior ให้ก่อน หากต้องการโค้ดรีวิวเชิงลึกไฟล์ DAG/Beam ตัวต่อตัว ให้ส่ง snapshot ของไฟล์เหล่านั้นเพิ่มเติมได้ครับ (ผมยังทำคำแนะนำเฉพาะจุดให้ไว้ด้านล่างแล้ว)

### 3.1 ความถูกต้องตามที่คาดหวัง (Expected behavior) เทียบกับเอกสาร

* **pipeline initiate**: ใช้ Composer เรียก STS (S3→GCS), สร้าง/รีครีเอต temp/external table, insert-to-target, เขียน lineage+audit — ตรงกับที่สรุปใน Pipeline Details แล้ว (แนะนำเพิ่ม idempotency และการ verify volume/rowcount หลัง transfer)&#x20;
* **pipeline realtime**: Airflow แค่ trigger Dataflow; Dataflow เปิด window, consume pub/sub create/update, ดึง secret, เขียน GCS (raw) และ BQ (refined/analytics), track lineage/audit, ตรวจ dependency (config-driven) — แนวทางนี้สอดคล้องกับ best practice ของ Dataflow streaming + IAM แยก runner SA ชัดเจน (เพิ่มข้อควรระวัง: Dataflow จะสร้าง subscription ภายในสำหรับ watermark — SA ต้องมีสิทธิ์บน Pub/Sub) ([Stack Overflow][12])&#x20;
* **pipeline batch**: ใช้ Dataflow โค้ดเดียวกับ realtime แต่ param เป็น batch; ไม่ใช้ window/notification, ดึงตรงจากแหล่ง (BQ ข้ามโปรเจกต์) — ตรงกับ design ของคุณ (ข้อเสนอ: ใช้ Airflow `Dataflow[Python|Java]Operator` พร้อม pass params ที่ระบุ batch/replay window)&#x20;
* **pipeline reconciled**: รัน STS อีกชุดเพื่อดึง snapshot มาเทียบกับ native table, สร้าง external temp, compare + audit reconciled — ตรงกับ design ที่ตั้งใจ (ข้อเสนอ: แยกชุด DDL external temp เป็นมาตรฐานเดียว และมี query compare template)&#x20;

### 3.2 โครงสร้างโฟลเดอร์ (ข้อเสนอปรับ)

แนะนำให้จัดโครงสร้างแบบ opinionated เพื่อรองรับ deploy/manual + CI/CD ในอนาคต:

```
/airflow
  /dags
  /plugins
  /include/         # schema/SQL/templates สำหรับใช้ใน DAG
  /config/          # variables-*.json, connections.json (ถ้าใช้)
/dataflow
  /java|python
  /templates        # (ถ้าทำ Flex) metadata.json, template_spec.json
  /docker           # (ถ้าทำ Flex) Dockerfile
  /tests
/config
  sts_tables.csv
  airflow_variables.json
/docs
  solution.md
  operations.md
/sql
  /audit
  /tables
    /raw
    /staging
    /refined
    /analytics
/scripts
  deploy_env.sh
  deploy_framework.sh
  deploy_pipelines.sh
  create_tables.sh
/infra
  /terraform (อนาคต)
```

ประโยชน์:

* แยก concerns ชัดเจน, ง่ายต่อ `rsync`/`import` เข้า Composer, ง่ายต่อการ build Dataflow
* วาง **variables** และ **STS table mapping** เดียว ใช้ซ้ำได้ทั้ง local/CI/CD

### 3.3 สิ่งที่น่าปรับปรุง (เชิงเทคนิค)

* **IAM scope**: เริ่มแบบ simple ได้ แต่เตรียม plan ลด permission (`storage.objectAdmin` → จำกัดเป็น bucket/prefix ที่ใช้, `pubsub.editor` → เฉพาะ publisher/subscriber) เมื่อขึ้น PROD ([Google Cloud][7])
* **Composer variables & secrets**: ค่า config runtime ให้ใช้ **Airflow Variables (JSON)** และดึง creds ผ่าน Secret Manager เสมอ (ไม่ hardcode ใน DAG) ([Medium][13])
* **BigLake external**: ใช้ BigLake + Connection เสมอสำหรับ RAW/STAGING เพื่อหลีกเลี่ยงการกระจายสิทธิ์ GCS ตรงให้ผู้ใช้ BigQuery (delegated access) ([Google Cloud][14])
* **Dataflow packaging**: ระยะสั้นใช้ **Dataflow\[Python|Java]Operator** รันจาก source ใน GCS/Artifacts; ระยะยาวพิจารณา **Flex Template** เพื่อ versioning/rollbacks ง่าย
* **Lineage**: เปิด Data Lineage API และให้ DAG/Dataflow log lineage event ไป Dataplex (ถ้าต้องการเขียนเองให้ใช้ role ด้าน write) ([Google Cloud][1])
* **Observability**: เพิ่ม Cloud Logging labels (pipeline, table, run\_id), และ alert จาก error rate/failed task
* **Reconciled template**: ทำ SQL เทียบระหว่าง external temp vs native เป็นเทมเพลตเดียว รับพารามิเตอร์ table/keys/columns
* **Idempotency & Backfill**: รองรับ rerun/backfill ที่ไม่ทำให้ duplicate (ใช้ `MERGE` เข้า refined/analytics, หรือ write-to staging + `MERGE`)
* **Naming**: มาตรฐาน `dp-<env>-<domain>-<zone>-<resource>` สำหรับ topics/subs/buckets/tables
* **Tests/Lint**: เพิ่ม unit test (Beam transforms), pylint/black (Python)/spotless (Java), DAG sanity check

---

## 3.1 (ตามที่ขอ) **Documentation – “In‑detail Solution”** (สรุปรวม)

> ไฟล์แนะนำ: `docs/solution.md`

**สถาปัตยกรรม (สรุป)**

* **Zones**: RAW/STAGING (External – BigLake), REFINED/ANALYTICS (Native BQ) ตาม **List\_service.md**&#x20;
* **Orchestration**: Cloud Composer (Airflow) เรียก STS/Dataflow/BigQuery/Dataplex/Secret Manager
* **Compute**: Dataflow (Realtime/Batch), STS (S3→GCS)
* **Governance**: Dataplex + Data Lineage API
* **Secrets**: GCP Secret Manager (AWS S3, Redshift, BQ cross‑project) ตาม **Pipeline Details**&#x20;

**Pipeline ตามประเภท**

1. **Initiate**

   * Airflow:

     1. Get secrets (AWS S3 / Redshift / BQ)
     2. Run STS (job ที่สร้างไว้ล่วงหน้า) copy S3→GCS
     3. สร้าง external temp/recreate
     4. Insert/Load เข้า target table
     5. Track lineage + Audit log
   * ใช้สิทธิ์: Orchestrator SA + STS admin + BQ data editor + Storage object admin
   * อ้างอิง STS CLI/Perms: ([Google Cloud][3])

2. **Realtime (Pub/Sub → Dataflow → RAW/REFINED/ANALYTICS)**

   * Airflow trigger Dataflow พร้อม `--params`
   * Dataflow: windowing consume topics `create/update`; ดึง secrets; เขียน RAW (GCS/Parquet) & REFINED/ANALYTICS (BQ) + lineage + audit; ตรวจ dependency ตาม config
   * SA ที่ใช้รันงาน: `sa-dataflow-runner` มี roles: dataflow\.worker, bq editor, storage object admin, pubsub sub/publish, secret accessor ([Google Cloud][7])

3. **Batch (short‑term)**

   * เหมือน realtime แต่ไม่มี window/notification; ดึงตรงจาก source (BQ ข้ามโปรเจกต์) ทุกชั่วโมง; ใช้ param แยกโหมดใน Dataflow
   * Audit+Lineage เช่นกัน&#x20;

4. **Reconciled**

   * Airflow: STS สร้าง snapshot ลง RAW, สร้าง external temp, compare กับ native table (query/spark/sql), บันทึกผล audit (diff/metrics) และ lineage
   * มี 2 STS jobs/ตาราง: **initiate** และ **reconciled** ตามรายละเอียดคุณ&#x20;

**Security/IAM**

* เปิด API ตามข้อ 1.1, จัด SA ตาม Simple/Better
* Composer env ผูก SA พร้อม `composer.worker` และเพิ่ม roles ที่ DAG ต้องใช้เท่านั้น ([Google Cloud][4])
* BigLake ใช้ Connection SA + GCS objectViewer เฉพาะ path ที่จำเป็น ([Google Cloud][11])

**Data Modeling & DDL**

* RAW/STAGING = BigLake external (Parquet)
* STRUCTURE = Views บน RAW
* REFINED/ANALYTICS = Native tables + partition/cluster + MERGE

**Operations**

* Deploy 4 ขั้นตอนด้วยสคริปต์ในข้อ 2
* Airflow variables: เก็บ config (แหล่งที่มา, target tables, dependency graph, batch interval ฯลฯ)
* Monitoring: Airflow task states, Dataflow job logs, STS job logs, custom metrics, alerting

---

## 3.2 (ตามที่ขอ) **Prompt Context – โปรเจกต์นี้** (ให้ก๊อปไปวางใช้กับ AI/Agent)

> **System / Project Context (ย่อ/เข้มข้น)**

* **Goal**: สร้าง Data Platform บน GCP รองรับ 4 pipeline (initiate/realtime/batch/reconciled) โดยเริ่มจาก deployment ง่ายด้วย shell/CLI ก่อน CI/CD
* **Zones**: RAW/STAGING (BigLake External via Connection), REFINED/ANALYTICS (Native BQ)
* **Services**: Composer(Airflow), Dataflow, Pub/Sub, BigQuery, GCS, Secret Manager, Dataplex(+Lineage), Storage Transfer Service (S3→GCS) ตาม **List\_service.md**&#x20;
* **Secrets**: ใช้ Secret Manager ทั้งหมด (AWS S3, Redshift, BQ cross‑project) ตาม **Pipeline Details**&#x20;
* **IAM**: เริ่มแบบ SA เดียว (orchestrator) ได้ แต่รองรับแยก SA สำหรับ Dataflow worker เมื่อเข้าสู่ PROD (least privilege)
* **Deploy 4 ขั้น**: `deploy_env.sh` → `deploy_framework.sh` → `deploy_pipelines.sh` → `create_tables.sh`
* **Constraints**: ใช้ CLI/gsutil/bq/gcloud, เก็บ config ที่ `config.sh`, Airflow Variables, CSV mapping STS jobs
* **Style Guide**:

  * Naming: `dp-<env>-<domain>-<zone>-<resource>`
  * Dataflow params: `mode=[realtime|batch]`, `table`, `zone`, `window_sec`, `source_project`, `target_dataset`, …
  * Logging: ทุกขั้นตอนต้อง log `run_id`, `pipeline`, `table`, `row_count`

> **When asked to generate code or changes**:

* ออกไฟล์ `.sh` idempotent, เช็ค resource exist, ใช้ `set -euo pipefail`
* DAG: ใช้ Operators ทางการ (`Dataflow*Operator`, `BigQuery*Operator`, `PubSub*`)
* หลีกเลี่ยง hardcode credentials; ดึงจาก Secret Manager
* ใส่คำสั่ง `gcloud composer environments storage ...` ที่เหมาะสมเสมอเมื่ออัปเดต DAG/variables/plugins

---

## 3.3 (ตามที่ขอ) **Prompt Context – สำหรับสิ่งที่ควรปรับทั้งหมด**

> **Objective**: วิเคราะห์และเสนอการปรับปรุงโครงสร้าง/โค้ด/ความปลอดภัย/สังเกตการณ์ ของโปรเจกต์ data platform นี้ให้พร้อม production โดยยังคงแนว “simple deploy first”.

**Scope ให้ AI/Agent**

1. ตรวจ **IAM minimization**: ลด `storage.admin` → objectAdmin หรือขึ้นกับ path; ลด `pubsub.editor` → publisher/subscriber; แยก SA orchestrator/runner
2. เสนอ **Dataflow packaging**: เลือก Classic vs Flex; ถ้า Flex ออกไฟล์ `Dockerfile`, `metadata.json`, `gcloud dataflow flex-template build/run`
3. ปรับ **Airflow structure**: แยก `include/` สำหรับ SQL/schema; variables JSON template; ตัวอย่าง `Connections` mapping (ถ้าใช้)
4. เสนอ **Lineage strategy**: จะอาศัย auto lineage จาก GCP systems + จุดไหนต้องใช้ API; ออกตัวอย่าง payload สำหรับ `datalineage` write (ถ้าต้อง) และการมาร์ก fullyQualifiedName ให้ match Dataplex Catalog ([Google Cloud][15])
5. **Reconciled** template: DDL external temp + SQL compare (key list, tolerances), เขียนผลลง `${DOMAIN}_${ENV}_audit.reconciled_log`
6. **Monitoring/Alerting**: Query failed tasks, Dataflow errors, STS job failures; ตัวอย่าง Cloud Monitoring alert policy
7. **Backfill strategy**: param `start_time/end_time` ใน batch; คำสั่ง Airflow backfill; `MERGE` ป้องกันซ้ำ
8. **Cost control**: Partition/cluster BQ, Autoclass GCS (ยกเว้น bucket DAGs ของ Composer), ลบ temp/staging ตามเวลา
9. **Docs & Runbook**: `docs/operations.md` วิธี roll-forward/back, วิธี rerun เฉพาะ table

---

### ปิดท้าย: เช็คลิสต์ความพร้อม (Quick)

* [ ] เปิด APIs ครบ
* [ ] `deploy_env.sh` ผ่าน: SA + Roles + Buckets + Datasets + BigLake connection + Pub/Sub + Secrets + Composer env
* [ ] `deploy_framework.sh` ผ่าน: อัปโหลด scripts + audit tables + (optional) build Dataflow artifacts
* [ ] `deploy_pipelines.sh` ผ่าน: อัปโหลด DAGs/plugins + import Airflow variables
* [ ] `create_tables.sh` ผ่าน: BigLake external RAW/STAGING + Native REFINED/ANALYTICS
* [ ] STS jobs สำหรับแต่ละ table (initiate/reconciled) ถูกสร้าง/ตั้ง schedule ตามต้องการ&#x20;

หากต้องการ ผมสามารถช่วยแปลงตัวอย่างด้านบนให้ตรงกับ **ชื่อไฟล์/โครงสร้างจริงใน repo** ของคุณมากขึ้น (เช่น แผนที่ table→prefix, variables JSON ตัวอย่าง, หรือ DAG ตัวอย่างสำหรับ initiate/reconciled) — ตอนนี้ผมจัดตามเอกสารที่คุณให้มา เพื่อให้คุณ “รันได้เลย” ด้วย 4 process ตามที่ตั้งใจครับ 💪

---

## แหล่งอ้างอิงสั้น ๆ

* STS S3→GCS + CLI ตัวอย่าง (`gcloud transfer jobs create`) และการกำหนด creds/schedule/manifest. ([Google Cloud][3])
* Composer service account/roles ที่ต้องมี (`composer.worker`) และการจัดการ DAGs/variables/plugins ด้วย CLI. ([Google Cloud][4], [Medium][13])
* Dataflow permissions สำหรับ runner/worker SA (BQ/Storage/PubSub/JobUser). ([Google Cloud][5])
* BigLake external tables + BigQuery Connection (CLOUD\_RESOURCE). ([Google Cloud][14])
* Dataplex/Data Lineage API (enable/view roles, แนวคิดการบันทึก lineage). ([Google Cloud][1])

> หมายเหตุ: ผมอ้างอิง “แนวปฏิบัติล่าสุด” ของ Cloud Composer 2 และ BigLake/Lineage ซึ่งมีการอัพเดตอยู่เสมอ ควรใช้เวอร์ชัน image Composer ที่รองรับใน region และตรวจสอบ release list ก่อนสร้าง/อัปเกรด environment ทุกครั้ง ([Google Cloud][10])

ถ้าพร้อมทดสอบจริง ให้ปรับ `config.sh` แล้วรัน:

```bash
bash deploy_env.sh
bash deploy_framework.sh
bash deploy_pipelines.sh
bash create_tables.sh
```

จากนั้นเข้า Airflow UI เพื่อ trigger DAG ตาม pipeline ครับ 🚀

[1]: https://cloud.google.com/dataplex/docs/use-lineage?utm_source=chatgpt.com "Use data lineage with Google Cloud systems | Dataplex ..."
[2]: https://cloud.google.com/secret-manager/docs/access-control?utm_source=chatgpt.com "Access control with IAM | Secret Manager Documentation"
[3]: https://cloud.google.com/storage-transfer/docs/create-transfers/agentless/s3 "Transfer from Amazon S3 to Cloud Storage  |  Cloud Storage Transfer Service Documentation  |  Google Cloud"
[4]: https://cloud.google.com/composer/docs/composer-2/create-environments?utm_source=chatgpt.com "Create Cloud Composer environments"
[5]: https://cloud.google.com/dataflow/docs/concepts/security-and-permissions?utm_source=chatgpt.com "Dataflow security and permissions"
[6]: https://cloud.google.com/bigquery/docs/external-data-cloud-storage?utm_source=chatgpt.com "Create Cloud Storage external tables | BigQuery"
[7]: https://cloud.google.com/dataflow/docs/tutorials/dataflow-stream-to-bigquery?utm_source=chatgpt.com "Stream from Pub/Sub to BigQuery - Dataflow"
[8]: https://cloud.google.com/iam/docs/roles-permissions/datalineage?utm_source=chatgpt.com "Data Lineage API roles and permissions - IAM"
[9]: https://cloud.google.com/storage-transfer/docs/source-amazon-s3?utm_source=chatgpt.com "Configure access to a source: Amazon S3"
[10]: https://cloud.google.com/composer/docs/composer-versions?utm_source=chatgpt.com "Cloud Composer version list"
[11]: https://cloud.google.com/bigquery/docs/create-cloud-resource-connection?utm_source=chatgpt.com "Create and set up a Cloud resource connection | BigQuery"
[12]: https://stackoverflow.com/questions/79397649/using-google-cloud-dataflow-with-a-custom-service-account-pub-sub-and-least-pr?utm_source=chatgpt.com "Using Google Cloud Dataflow with a Custom Service ..."
[13]: https://medium.com/%40inistar/google-cloud-composer-ci-cd-f469a09c9db8?utm_source=chatgpt.com "Google Cloud Composer CI/CD"
[14]: https://cloud.google.com/bigquery/docs/create-cloud-storage-table-biglake?utm_source=chatgpt.com "Create BigLake external tables for Cloud Storage | BigQuery"
[15]: https://cloud.google.com/dataplex/docs/about-data-lineage?utm_source=chatgpt.com "About data lineage | Dataplex Universal Catalog"

