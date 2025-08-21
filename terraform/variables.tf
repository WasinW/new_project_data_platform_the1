# terraform/variables.tf
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "asia-southeast1"
}

variable "zone" {
  description = "GCP zone for resources"
  type        = string
  default     = "asia-southeast1-a"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "primary_domain" {
  description = "Primary domain for the data pipeline"
  type        = string
  default     = "member"
}

variable "data_admin_email" {
  description = "Email of the data administrator"
  type        = string
}

variable "composer_node_count" {
  description = "Number of nodes in Composer environment"
  type        = number
  default     = 3
}

variable "composer_machine_type" {
  description = "Machine type for Composer nodes"
  type        = string
  default     = "n1-standard-1"
}

variable "enable_encryption" {
  description = "Enable KMS encryption for data"
  type        = bool
  default     = false
}

variable "notification_channels" {
  description = "List of notification channels for alerts"
  type        = list(string)
  default     = []
}

variable "monitoring_alerts" {
  description = "Configuration for monitoring alerts"
  type = map(object({
    display_name           = string
    condition_display_name = string
    combiner              = string
    filter                = string
    duration              = string
    comparison            = string
    threshold_value       = number
    alignment_period      = string
    per_series_aligner    = string
  }))
  default = {
    dataflow_job_failed = {
      display_name           = "Dataflow Job Failed"
      condition_display_name = "Dataflow job failure rate"
      combiner              = "OR"
      filter                = "resource.type=\"dataflow_job\""
      duration              = "300s"
      comparison            = "COMPARISON_GREATER_THAN"
      threshold_value       = 0
      alignment_period      = "300s"
      per_series_aligner    = "ALIGN_RATE"
    }
    
    bigquery_errors = {
      display_name           = "BigQuery Errors"
      condition_display_name = "BigQuery error rate"
      combiner              = "OR"
      filter                = "resource.type=\"bigquery_project\""
      duration              = "300s"
      comparison            = "COMPARISON_GREATER_THAN"
      threshold_value       = 5
      alignment_period      = "300s"
      per_series_aligner    = "ALIGN_RATE"
    }
  }
}

variable "domains" {
  description = "List of domains to create resources for"
  type        = list(string)
  default     = ["member", "order", "product"]
}

variable "enable_private_networking" {
  description = "Enable private networking for security"
  type        = bool
  default     = false
}

variable "bucket_lifecycle_age" {
  description = "Age in days for bucket lifecycle rules"
  type        = number
  default     = 30
}

variable "bigquery_table_expiration_days" {
  description = "Default table expiration in days"
  type        = number
  default     = 90
}

variable "pubsub_message_retention_duration" {
  description = "Pub/Sub message retention duration"
  type        = string
  default     = "86400s"
}

variable "dataflow_max_workers" {
  description = "Maximum workers for Dataflow jobs"
  type        = number
  default     = 20
}

variable "dataflow_machine_type" {
  description = "Machine type for Dataflow workers"
  type        = string
  default     = "n2-standard-4"
}
