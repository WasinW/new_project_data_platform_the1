# terraform/outputs.tf
output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP Region"
  value       = var.region
}

output "dataflow_service_account" {
  description = "Dataflow service account email"
  value       = google_service_account.dataflow_sa.email
}

output "composer_environment_name" {
  description = "Cloud Composer environment name"
  value       = google_composer_environment.composer.name
}

output "composer_uri" {
  description = "Cloud Composer Airflow URI"
  value       = google_composer_environment.composer.config[0].airflow_uri
}

output "storage_buckets" {
  description = "Created storage bucket names"
  value = {
    for k, v in google_storage_bucket.pipeline_buckets : k => v.name
  }
}

output "bigquery_datasets" {
  description = "Created BigQuery dataset IDs"
  value = {
    for k, v in google_bigquery_dataset.datasets : k => v.dataset_id
  }
}

output "pubsub_topics" {
  description = "Created Pub/Sub topic names"
  value = {
    for k, v in google_pubsub_topic.pipeline_topics : k => v.name
  }
}

output "pubsub_subscriptions" {
  description = "Created Pub/Sub subscription names"
  value = {
    for k, v in google_pubsub_subscription.pipeline_subscriptions : k => v.name
  }
}

output "dataflow_network" {
  description = "Dataflow VPC network name"
  value       = google_compute_network.dataflow_network.name
}

output "dataflow_subnetwork" {
  description = "Dataflow subnetwork name"
  value       = google_compute_subnetwork.dataflow_subnet.name
}

output "dataflow_subnetwork_full_name" {
  description = "Dataflow subnetwork full resource name"
  value       = google_compute_subnetwork.dataflow_subnet.id
}

output "kms_key_name" {
  description = "KMS key name for encryption"
  value       = var.enable_encryption ? google_kms_crypto_key.dataflow_key[0].id : null
}

output "audit_tables" {
  description = "Audit table names in BigQuery"
  value = {
    for k, v in google_bigquery_table.audit_tables : k => "${v.dataset_id}.${v.table_id}"
  }
}

output "pipeline_configuration" {
  description = "Pipeline configuration for reference"
  value = {
    project_id               = var.project_id
    region                  = var.region
    zone                    = var.zone
    environment             = var.environment
    primary_domain          = var.primary_domain
    dataflow_service_account = google_service_account.dataflow_sa.email
    dataflow_network        = google_compute_network.dataflow_network.name
    dataflow_subnetwork     = google_compute_subnetwork.dataflow_subnet.id
    temp_bucket            = google_storage_bucket.pipeline_buckets["dataflow-temp"].name
    staging_bucket         = google_storage_bucket.pipeline_buckets["dataflow-staging"].name
    config_bucket          = google_storage_bucket.pipeline_buckets["pipeline-configs"].name
    template_bucket        = google_storage_bucket.pipeline_buckets["dataflow-templates"].name
  }
}

output "airflow_variables" {
  description = "Airflow variables configuration"
  value = {
    gcp_project_id          = var.project_id
    gcp_region             = var.region
    gcp_zone               = var.zone
    dataflow_service_account = google_service_account.dataflow_sa.email
    dataflow_network       = google_compute_network.dataflow_network.id
    dataflow_subnetwork    = google_compute_subnetwork.dataflow_subnet.id
    temp_location          = "gs://${google_storage_bucket.pipeline_buckets["dataflow-temp"].name}"
    staging_location       = "gs://${google_storage_bucket.pipeline_buckets["dataflow-staging"].name}"
    config_bucket          = google_storage_bucket.pipeline_buckets["pipeline-configs"].name
    template_bucket        = google_storage_bucket.pipeline_buckets["dataflow-templates"].name
  }
  sensitive = false
}

output "setup_commands" {
  description = "Commands to run after Terraform deployment"
  value = [
    "# Upload pipeline configurations:",
    "gsutil cp config/*.yaml gs://${google_storage_bucket.pipeline_buckets["pipeline-configs"].name}/",
    "",
    "# Build and upload Dataflow templates:",
    "cd dataflow && docker build -t gcr.io/${var.project_id}/hybrid-pipeline:latest .",
    "docker push gcr.io/${var.project_id}/hybrid-pipeline:latest",
    "",
    "gcloud dataflow flex-template build gs://${google_storage_bucket.pipeline_buckets["dataflow-templates"].name}/hybrid_pipeline.json \\",
    "  --image gcr.io/${var.project_id}/hybrid-pipeline:latest \\",
    "  --sdk-language PYTHON",
    "",
    "# Import Airflow variables:",
    "gcloud composer environments storage data import \\",
    "  --environment ${google_composer_environment.composer.name} \\",
    "  --location ${var.region} \\",
    "  --source airflow/config/airflow_variables.json",
    "",
    "# Upload DAGs:",
    "gcloud composer environments storage dags import \\",
    "  --environment ${google_composer_environment.composer.name} \\",
    "  --location ${var.region} \\",
    "  --source airflow/dags/"
  ]
}
