# terraform/main.tf
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.84.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.84.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "dataflow.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "composer.googleapis.com",
    "datacatalog.googleapis.com",
    "dlp.googleapis.com",
    "storagetransfer.googleapis.com"
  ])
  
  service = each.value
  
  disable_dependent_services = true
}

# Storage buckets for pipeline
resource "google_storage_bucket" "pipeline_buckets" {
  for_each = toset([
    "dataflow-temp",
    "dataflow-staging", 
    "pipeline-configs",
    "dataflow-templates",
    "gcs-staging-${var.primary_domain}"
  ])
  
  name          = "${var.project_id}-${each.value}"
  location      = var.region
  force_destroy = var.environment != "prod"
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = var.environment == "prod"
  }
  
  lifecycle_rule {
    condition {
      age = var.environment == "prod" ? 90 : 30
    }
    action {
      type = "Delete"
    }
  }
  
  depends_on = [google_project_service.apis]
}

# BigQuery datasets
resource "google_bigquery_dataset" "datasets" {
  for_each = toset([
    "${var.primary_domain}_raw",
    "${var.primary_domain}_staging", 
    "${var.primary_domain}_refined",
    "${var.primary_domain}_analytics",
    "${var.primary_domain}_audit",
    "${var.primary_domain}_reconcile_temp",
    "batch_control",
    "reference_data"
  ])
  
  dataset_id    = each.value
  friendly_name = "Dataset for ${each.value}"
  description   = "Dataset for ${var.primary_domain} domain - ${each.value} layer"
  location      = var.region
  
  default_table_expiration_ms = var.environment == "prod" ? null : 7776000000 # 90 days for non-prod
  
  access {
    role          = "OWNER"
    user_by_email = var.data_admin_email
  }
  
  access {
    role         = "READER"
    special_group = "projectReaders"
  }
  
  access {
    role         = "WRITER"
    special_group = "projectWriters"
  }
  
  depends_on = [google_project_service.apis]
}

# Pub/Sub topics and subscriptions
resource "google_pubsub_topic" "pipeline_topics" {
  for_each = toset([
    "${var.primary_domain}-events-create",
    "${var.primary_domain}-events-update",
    "${var.primary_domain}-events-delete"
  ])
  
  name = each.value
  
  message_retention_duration = "86400s" # 24 hours
  
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "pipeline_subscriptions" {
  for_each = google_pubsub_topic.pipeline_topics
  
  name  = "${each.value.name}-sub"
  topic = each.value.name
  
  message_retention_duration = "1200s" # 20 minutes
  retain_acked_messages      = false
  ack_deadline_seconds       = 60
  
  expiration_policy {
    ttl = "300000s" # 83 hours
  }
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }
}

resource "google_pubsub_topic" "dead_letter" {
  name = "${var.primary_domain}-dead-letter"
  
  depends_on = [google_project_service.apis]
}

# Service account for Dataflow
resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-sa"
  display_name = "Dataflow Service Account"
  description  = "Service account for Dataflow pipelines"
}

resource "google_project_iam_member" "dataflow_permissions" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/pubsub.subscriber",
    "roles/pubsub.publisher"
  ])
  
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Cloud Composer environment
resource "google_composer_environment" "composer" {
  name   = "${var.primary_domain}-composer-${var.environment}"
  region = var.region
  
  config {
    node_count = var.composer_node_count
    
    node_config {
      zone         = var.zone
      machine_type = var.composer_machine_type
      disk_size_gb = 100
    }
    
    software_config {
      image_version = "composer-2.4.6-airflow-2.6.3"
      
      pypi_packages = {
        "apache-beam[gcp]" = "==2.48.0"
        "google-cloud-storage" = "==2.10.0"
        "google-cloud-bigquery" = "==3.11.4"
        "google-cloud-datacatalog" = "==3.9.0"
        "google-cloud-lineage" = "==0.2.0"
        "pyyaml" = "==6.0"
      }
      
      env_variables = {
        GCP_PROJECT_ID = var.project_id
        GCP_REGION     = var.region
        ENVIRONMENT    = var.environment
        DOMAIN         = var.primary_domain
      }
    }
    
    private_environment_config {
      enable_private_endpoint = var.environment == "prod"
    }
  }
  
  depends_on = [google_project_service.apis]
}

# VPC network for secure communication
resource "google_compute_network" "dataflow_network" {
  name                    = "${var.primary_domain}-dataflow-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "dataflow_subnet" {
  name          = "${var.primary_domain}-dataflow-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.dataflow_network.id
  
  secondary_ip_range {
    range_name    = "gke-pods"
    ip_cidr_range = "10.1.0.0/16"
  }
  
  secondary_ip_range {
    range_name    = "gke-services"
    ip_cidr_range = "10.2.0.0/16"
  }
  
  private_ip_google_access = true
}

# Firewall rules
resource "google_compute_firewall" "dataflow_firewall" {
  name    = "${var.primary_domain}-dataflow-firewall"
  network = google_compute_network.dataflow_network.name
  
  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
  
  source_tags = ["dataflow"]
  target_tags = ["dataflow"]
}

# BigQuery audit tables
resource "google_bigquery_table" "audit_tables" {
  for_each = toset([
    "processing_logs",
    "data_lineage", 
    "processing_metrics",
    "data_quality_logs",
    "failed_dependencies",
    "fetch_errors",
    "transform_errors",
    "validation_errors",
    "reconciliation_results",
    "reconciliation_stats"
  ])
  
  dataset_id = google_bigquery_dataset.datasets["${var.primary_domain}_audit"].dataset_id
  table_id   = each.value
  
  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }
  
  clustering = ["pipeline_name", "domain"]
  
  schema = file("../config/schemas/${each.value}_schema.json")
  
  depends_on = [google_bigquery_dataset.datasets]
}

# Cloud KMS for encryption
resource "google_kms_key_ring" "dataflow_keyring" {
  count    = var.enable_encryption ? 1 : 0
  name     = "${var.primary_domain}-dataflow-keyring"
  location = var.region
}

resource "google_kms_crypto_key" "dataflow_key" {
  count           = var.enable_encryption ? 1 : 0
  name            = "${var.primary_domain}-dataflow-key"
  key_ring        = google_kms_key_ring.dataflow_keyring[0].id
  rotation_period = "2592000s" # 30 days
  
  lifecycle {
    prevent_destroy = true
  }
}

# IAM binding for KMS key
resource "google_kms_crypto_key_iam_binding" "dataflow_key_binding" {
  count         = var.enable_encryption ? 1 : 0
  crypto_key_id = google_kms_crypto_key.dataflow_key[0].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  
  members = [
    "serviceAccount:${google_service_account.dataflow_sa.email}",
    "serviceAccount:service-${data.google_project.project.number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
  ]
}

# Data source for project info
data "google_project" "project" {
  project_id = var.project_id
}

# Monitoring alert policies
resource "google_monitoring_alert_policy" "pipeline_alerts" {
  for_each     = var.monitoring_alerts
  display_name = each.value.display_name
  combiner     = each.value.combiner
  enabled      = true
  
  conditions {
    display_name = each.value.condition_display_name
    
    condition_threshold {
      filter          = each.value.filter
      duration        = each.value.duration
      comparison      = each.value.comparison
      threshold_value = each.value.threshold_value
      
      aggregations {
        alignment_period   = each.value.alignment_period
        per_series_aligner = each.value.per_series_aligner
      }
    }
  }
  
  notification_channels = var.notification_channels
  
  depends_on = [google_project_service.apis]
}
