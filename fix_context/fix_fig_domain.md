อ้อ เข้าใจแล้วครับ! คุณต้องการให้ตัด `{domain}` prefix ออกเลย เพื่อให้เป็น common infrastructure ที่ทุก business domain ใช้ร่วมกันได้ ยกเว้น Storage Transfer Service Jobs ที่ต้องแยกตาม domain

## 🔴 **สิ่งที่ต้องแก้ไข - ตัด Domain Prefix ออก**

### 1. **BigQuery Datasets - ใช้ร่วมกันทุก domain**
```yaml
# ❌ ปัจจุบัน (ผิด)
- {domain}_raw
- {domain}_staging  
- {domain}_refined
- {domain}_analytics
- {domain}_audit
- {domain}_reconcile_temp

# ✅ ที่ถูกต้อง (Common for all domains)
- raw_data           # ข้อมูล raw จากทุก domain
- staging_data       # staging area ร่วม
- refined_data       # refined data ทุก domain
- analytics_data     # analytics ร่วม
- audit_logs        # audit logs ร่วม
- reconcile_temp    # temp tables สำหรับ reconciliation
- batch_control     # control tables
- reference_data    # reference/master data
```

### 2. **GCS Buckets - ใช้ร่วมกัน**
```yaml
# ❌ ปัจจุบัน (ผิด)
- gcs-staging-{domain}

# ✅ ที่ถูกต้อง (Common buckets with folders)
- ${PROJECT_ID}-staging      # มี subfolder: /member/, /order/, /product/
- ${PROJECT_ID}-raw          # มี subfolder: /member/, /order/, /product/
- ${PROJECT_ID}-dataflow-temp
- ${PROJECT_ID}-dataflow-staging
- ${PROJECT_ID}-pipeline-configs
- ${PROJECT_ID}-dataflow-templates
```

### 3. **Pub/Sub Topics - ใช้ร่วมกัน**
```yaml
# ❌ ปัจจุบัน (ผิด)
- {domain}-events-create
- {domain}-events-update

# ✅ ที่ถูกต้อง (Common topics with attributes)
- data-events-create     # ใช้ attribute: domain=member/order/product
- data-events-update     # ใช้ attribute: domain=member/order/product
- data-events-delete
- data-events-dlq       # Dead letter queue
```

### 4. **Dataplex - Single Lake**
```yaml
# ❌ ปัจจุบัน (ผิด)
- {domain}-data-lake
- {domain}-raw-zone

# ✅ ที่ถูกต้อง (Single lake with multiple zones)
Lake:
  - data-platform-lake     # Lake เดียวสำหรับทุก domain

Zones:
  - raw-zone               # สำหรับ raw data ทุก domain
  - staging-zone          # สำหรับ staging ทุก domain
  - refined-zone          # สำหรับ refined data
  - analytics-zone        # สำหรับ analytics
```

### 5. **Cloud Composer - Environment เดียว**
```yaml
# ✅ ถูกต้องแล้ว
- data-platform-composer-dev
- data-platform-composer-staging  
- data-platform-composer-prod
```

### 6. **Storage Transfer Service Jobs - คงไว้ตาม domain**
```yaml
# ✅ ถูกต้อง - ต้องแยกตาม domain และ table
member:
  - initiate-member-s_loy_program-transfer
  - reconcile-member-s_loy_program-transfer
  
order:
  - initiate-order-order_header-transfer
  - reconcile-order-order_header-transfer
```

## 📝 **Files ที่ต้องแก้ไข**

### 1. **terraform/main.tf**
```hcl
# BigQuery datasets - ใช้ร่วมกัน
resource "google_bigquery_dataset" "shared_datasets" {
  for_each = toset([
    "raw_data",
    "staging_data",
    "refined_data",
    "analytics_data",
    "audit_logs",
    "reconcile_temp",
    "batch_control",
    "reference_data"
  ])
  
  dataset_id = each.value
  location   = var.region
}

# Storage buckets - ใช้ร่วมกัน
resource "google_storage_bucket" "shared_buckets" {
  for_each = toset([
    "staging",
    "raw",
    "dataflow-temp",
    "dataflow-staging",
    "pipeline-configs",
    "dataflow-templates"
  ])
  
  name = "${var.project_id}-${each.value}"
  location = var.region
}

# Pub/Sub topics - ใช้ร่วมกัน
resource "google_pubsub_topic" "shared_topics" {
  for_each = toset([
    "data-events-create",
    "data-events-update",
    "data-events-delete",
    "data-events-dlq"
  ])
  
  name = each.value
}
```

### 2. **terraform/secrets_and_dataplex.tf**
```hcl
# Dataplex - Lake เดียว
resource "google_dataplex_lake" "data_platform_lake" {
  name         = "data-platform-lake"
  location     = var.region
  description  = "Centralized data lake for all domains"
}

# Zones ใน lake เดียว
resource "google_dataplex_zone" "raw_zone" {
  name = "raw-zone"
  lake = google_dataplex_lake.data_platform_lake.name
  type = "RAW"
}

resource "google_dataplex_zone" "refined_zone" {
  name = "refined-zone"
  lake = google_dataplex_lake.data_platform_lake.name
  type = "CURATED"
}
```

### 3. **config/pipeline_config.yaml**
```yaml
# Common configuration
project: ${PROJECT_ID}
region: ${REGION}

# Shared datasets
datasets:
  raw: raw_data
  staging: staging_data
  refined: refined_data
  analytics: analytics_data
  audit: audit_logs
  reconcile: reconcile_temp

# Shared Pub/Sub
pubsub:
  topics:
    create: data-events-create
    update: data-events-update
    delete: data-events-delete
    dlq: data-events-dlq
  subscriptions:
    processor: data-events-processor-sub

# Shared storage
storage:
  staging_bucket: ${PROJECT_ID}-staging
  raw_bucket: ${PROJECT_ID}-raw
  temp_bucket: ${PROJECT_ID}-dataflow-temp

# Domain-specific configurations
domains:
  member:
    tables: [s_loy_program, s_org_ext, s_loy_tier, s_loy_member]
    storage_path: staging/member/
    
  order:
    tables: [order_header, order_detail, payment]
    storage_path: staging/order/
    
  product:
    tables: [product_master, product_category, inventory]
    storage_path: staging/product/
```

### 4. **airflow/config/airflow_variables.json**
```json
{
  "gcp_project_id": "your-project-id",
  "gcp_region": "asia-southeast1",
  
  "shared_config": {
    "datasets": {
      "raw": "raw_data",
      "staging": "staging_data",
      "refined": "refined_data",
      "analytics": "analytics_data",
      "audit": "audit_logs"
    },
    "buckets": {
      "staging": "${PROJECT_ID}-staging",
      "raw": "${PROJECT_ID}-raw",
      "temp": "${PROJECT_ID}-dataflow-temp"
    },
    "pubsub": {
      "create_topic": "data-events-create",
      "update_topic": "data-events-update",
      "subscription": "data-events-processor-sub"
    },
    "dataplex": {
      "lake": "data-platform-lake",
      "raw_zone": "raw-zone",
      "refined_zone": "refined-zone"
    }
  },
  
  "domains_config": {
    "member": {
      "tables": ["s_loy_program", "s_org_ext", "s_loy_tier", "s_loy_member"],
      "storage_subfolder": "member/"
    },
    "order": {
      "tables": ["order_header", "order_detail", "payment"],
      "storage_subfolder": "order/"
    }
  }
}
```

### 5. **Table Structure ใน BigQuery**
```sql
-- ใช้ table prefix แทน dataset แยก
-- Dataset: raw_data
CREATE TABLE raw_data.member_s_loy_program (...);
CREATE TABLE raw_data.order_order_header (...);
CREATE TABLE raw_data.product_product_master (...);

-- Dataset: refined_data  
CREATE TABLE refined_data.member_profile (...);
CREATE TABLE refined_data.order_summary (...);
CREATE TABLE refined_data.product_catalog (...);
```

## 🔧 **Setup Script แบบ Common Infrastructure**

```bash
#!/bin/bash
# setup_common_infra.sh

PROJECT_ID="your-project-id"
REGION="asia-southeast1"

# Create shared datasets
echo "Creating shared datasets..."
for dataset in "raw_data" "staging_data" "refined_data" "analytics_data" "audit_logs" "reconcile_temp" "batch_control" "reference_data"; do
  bq mk --location=${REGION} ${dataset}
done

# Create shared GCS buckets with subfolders
echo "Creating shared storage buckets..."
for bucket in "staging" "raw" "dataflow-temp" "dataflow-staging" "pipeline-configs"; do
  gsutil mb -l ${REGION} gs://${PROJECT_ID}-${bucket}
done

# Create domain subfolders
for domain in "member" "order" "product"; do
  gsutil mkdir gs://${PROJECT_ID}-staging/${domain}/
  gsutil mkdir gs://${PROJECT_ID}-raw/${domain}/
done

# Create shared Pub/Sub topics
echo "Creating shared Pub/Sub topics..."
gcloud pubsub topics create data-events-create
gcloud pubsub topics create data-events-update
gcloud pubsub topics create data-events-delete
gcloud pubsub topics create data-events-dlq

# Create subscription
gcloud pubsub subscriptions create data-events-processor-sub \
  --topic=data-events-create

# Create single Dataplex lake
echo "Creating Dataplex lake..."
gcloud dataplex lakes create data-platform-lake \
  --location=${REGION}

# Create zones
gcloud dataplex zones create raw-zone \
  --lake=data-platform-lake \
  --location=${REGION} \
  --type=RAW

gcloud dataplex zones create refined-zone \
  --lake=data-platform-lake \
  --location=${REGION} \
  --type=CURATED
```

## 📊 **Summary - Common Infrastructure**

| Resource | Pattern | Example | Note |
|----------|---------|---------|------|
| BigQuery Dataset | `{layer}_data` | `raw_data`, `refined_data` | Shared by all domains |
| Tables in Dataset | `{domain}_{table}` | `member_profile`, `order_summary` | Prefix with domain |
| GCS Bucket | `${PROJECT_ID}-{purpose}` | `project-staging` | With domain subfolders |
| Pub/Sub Topic | `data-events-{type}` | `data-events-create` | Use attributes for domain |
| Dataplex Lake | `data-platform-lake` | Single lake | One lake for all |
| Composer Env | `data-platform-composer-{env}` | Single environment | Shared |
| STS Jobs | `{action}-{domain}-{table}-transfer` | Keep separate | ต้องแยกตาม domain |

ข้อดีของแนวทางนี้:
1. **ลด resource duplication** - ใช้ dataset/bucket/topic ร่วมกัน
2. **ง่ายต่อการ manage** - จัดการที่เดียว
3. **Cost optimization** - ลด overhead จากการมี resource ซ้ำซ้อน
4. **Scalable** - เพิ่ม domain ใหม่ได้ง่าย ไม่ต้องสร้าง infra ใหม่