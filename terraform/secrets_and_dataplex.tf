# Secret Manager resources for the data pipeline

# AWS S3 Access Key ID
resource "google_secret_manager_secret" "aws_s3_access_key_id" {
  secret_id = "aws-s3-access-key-id"
  
  labels = {
    environment = var.environment
    domain      = var.domain
    component   = "data-pipeline"
  }

  replication {
    auto {
    }
  }
}

resource "google_secret_manager_secret_version" "aws_s3_access_key_id" {
  secret      = google_secret_manager_secret.aws_s3_access_key_id.id
  secret_data = var.aws_s3_access_key_id
}

# AWS S3 Secret Access Key
resource "google_secret_manager_secret" "aws_s3_secret_access_key" {
  secret_id = "aws-s3-secret-access-key"
  
  labels = {
    environment = var.environment
    domain      = var.domain
    component   = "data-pipeline"
  }

  replication {
    auto {
    }
  }
}

resource "google_secret_manager_secret_version" "aws_s3_secret_access_key" {
  secret      = google_secret_manager_secret.aws_s3_secret_access_key.id
  secret_data = var.aws_s3_secret_access_key
}

# AWS S3 Bucket Name
resource "google_secret_manager_secret" "aws_s3_bucket_name" {
  secret_id = "aws-s3-bucket-name"
  
  labels = {
    environment = var.environment
    domain      = var.domain
    component   = "data-pipeline"
  }

  replication {
    auto {
    }
  }
}

resource "google_secret_manager_secret_version" "aws_s3_bucket_name" {
  secret      = google_secret_manager_secret.aws_s3_bucket_name.id
  secret_data = var.aws_s3_bucket_name
}

# BigQuery Service Account Key
resource "google_secret_manager_secret" "bq_service_account_key" {
  secret_id = "bq-service-account-key"
  
  labels = {
    environment = var.environment
    domain      = var.domain
    component   = "data-pipeline"
  }

  replication {
    auto {
    }
  }
}

resource "google_secret_manager_secret_version" "bq_service_account_key" {
  secret      = google_secret_manager_secret.bq_service_account_key.id
  secret_data = jsonencode({
    type                        = "service_account"
    project_id                  = var.project_id
    private_key_id              = var.bq_service_account_private_key_id
    private_key                 = var.bq_service_account_private_key
    client_email                = var.bq_service_account_email
    client_id                   = var.bq_service_account_client_id
    auth_uri                    = "https://accounts.google.com/o/oauth2/auth"
    token_uri                   = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url        = "https://www.googleapis.com/robot/v1/metadata/x509/${var.bq_service_account_email}"
  })
}

# Dataplex Lake (Single shared lake for all domains)
resource "google_dataplex_lake" "data_lake" {
  location     = var.region
  name         = "data-platform-lake"
  description  = "Centralized data lake for all business domains"
  display_name = "Data Platform Lake"

  labels = {
    environment = var.environment
    created_by  = "terraform"
    purpose     = "multi-domain-data-lake"
  }
}

# Dataplex Zone - Raw (Shared)
resource "google_dataplex_zone" "raw_zone" {
  discovery_spec {
    enabled = true
  }

  lake     = google_dataplex_lake.data_lake.name
  location = var.region
  name     = "raw-zone"

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = "RAW"
  description  = "Raw data zone for all domains"
  display_name = "Raw Data Zone"

  labels = {
    environment = var.environment
    zone_type   = "raw"
    created_by  = "terraform"
  }
}

# Dataplex Zone - Refined (Shared)
resource "google_dataplex_zone" "refined_zone" {
  discovery_spec {
    enabled = true
  }

  lake     = google_dataplex_lake.data_lake.name
  location = var.region
  name     = "refined-zone"

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = "CURATED"
  description  = "Refined data zone for all domains"
  display_name = "Refined Data Zone"

  labels = {
    environment = var.environment
    zone_type   = "refined"
    created_by  = "terraform"
  }
}

# Dataplex Zone - Analytics (Shared)
resource "google_dataplex_zone" "analytics_zone" {
  discovery_spec {
    enabled = true
  }

  lake     = google_dataplex_lake.data_lake.name
  location = var.region
  name     = "analytics-zone"

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  type         = "CURATED"
  description  = "Analytics data zone for all domains"
  display_name = "Analytics Data Zone"

  labels = {
    environment = var.environment
    zone_type   = "analytics"
    created_by  = "terraform"
  }
}

# Dataplex Assets for BigQuery datasets (Shared datasets)

# Raw data asset
resource "google_dataplex_asset" "raw_dataset_asset" {
  name         = "raw-data-asset"
  location     = var.region
  
  lake = google_dataplex_lake.data_lake.name
  dataplex_zone = google_dataplex_zone.raw_zone.name

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/raw_data"
    type = "BIGQUERY_DATASET"
  }

  display_name = "Raw Data Dataset"
  description  = "Shared BigQuery dataset containing raw data for all domains"

  labels = {
    environment  = var.environment
    asset_type   = "bigquery_dataset"
    data_layer   = "raw"
    created_by   = "terraform"
  }

  depends_on = [google_bigquery_dataset.datasets]
}

# Refined data asset
resource "google_dataplex_asset" "refined_dataset_asset" {
  name         = "refined-data-asset"
  location     = var.region
  
  lake = google_dataplex_lake.data_lake.name
  dataplex_zone = google_dataplex_zone.refined_zone.name

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/refined_data"
    type = "BIGQUERY_DATASET"
  }

  display_name = "Refined Data Dataset"
  description  = "Shared BigQuery dataset containing refined data for all domains"

  labels = {
    environment  = var.environment
    asset_type   = "bigquery_dataset"
    data_layer   = "refined"
    created_by   = "terraform"
  }

  depends_on = [google_bigquery_dataset.datasets]
}

# Analytics data asset
resource "google_dataplex_asset" "analytics_dataset_asset" {
  name         = "${var.domain}-analytics-dataset"
  location     = var.region
  
  lake = google_dataplex_lake.data_lake.name
  dataplex_zone = google_dataplex_zone.analytics_zone.name

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/datasets/${var.domain}_analytics"
    type = "BIGQUERY_DATASET"
  }

  display_name = "${title(var.domain)} Analytics Dataset"
  description  = "BigQuery dataset containing analytics data for ${var.domain}"

  labels = {
    environment  = var.environment
    domain       = var.domain
    asset_type   = "bigquery_dataset"
    data_layer   = "analytics"
    created_by   = "terraform"
  }

  depends_on = [google_bigquery_dataset.analytics_dataset]
}

# GCS bucket asset for staging data
resource "google_dataplex_asset" "staging_bucket_asset" {
  name         = "${var.domain}-staging-bucket"
  location     = var.region
  
  lake = google_dataplex_lake.data_lake.name
  dataplex_zone = google_dataplex_zone.raw_zone.name

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/buckets/gcs-staging-${var.domain}"
    type = "STORAGE_BUCKET"
  }

  display_name = "${title(var.domain)} Staging Bucket"
  description  = "GCS bucket for staging data from S3 for ${var.domain}"

  labels = {
    environment  = var.environment
    domain       = var.domain
    asset_type   = "storage_bucket"
    data_layer   = "staging"
    created_by   = "terraform"
  }

  depends_on = [google_storage_bucket.staging_bucket]
}

# IAM bindings for Secret Manager access

# Grant Dataflow service account access to secrets
resource "google_secret_manager_secret_iam_binding" "dataflow_secret_access" {
  for_each = toset([
    google_secret_manager_secret.aws_s3_access_key_id.secret_id,
    google_secret_manager_secret.aws_s3_secret_access_key.secret_id,
    google_secret_manager_secret.aws_s3_bucket_name.secret_id,
    google_secret_manager_secret.bq_service_account_key.secret_id
  ])

  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  members = [
    "serviceAccount:${var.dataflow_service_account_email}",
    "serviceAccount:${var.composer_service_account_email}"
  ]
}

# Output values for reference
output "secret_manager_secrets" {
  description = "Secret Manager secret names"
  value = {
    aws_s3_access_key_id     = google_secret_manager_secret.aws_s3_access_key_id.secret_id
    aws_s3_secret_access_key = google_secret_manager_secret.aws_s3_secret_access_key.secret_id
    aws_s3_bucket_name       = google_secret_manager_secret.aws_s3_bucket_name.secret_id
    bq_service_account_key   = google_secret_manager_secret.bq_service_account_key.secret_id
  }
}

output "dataplex_resources" {
  description = "Dataplex resource names"
  value = {
    lake_name         = google_dataplex_lake.data_lake.name
    raw_zone_name     = google_dataplex_zone.raw_zone.name
    refined_zone_name = google_dataplex_zone.refined_zone.name
    analytics_zone_name = google_dataplex_zone.analytics_zone.name
    assets = {
      raw_dataset        = google_dataplex_asset.raw_dataset_asset.name
      refined_dataset    = google_dataplex_asset.refined_dataset_asset.name
      analytics_dataset  = google_dataplex_asset.analytics_dataset_asset.name
      staging_bucket     = google_dataplex_asset.staging_bucket_asset.name
    }
  }
}
