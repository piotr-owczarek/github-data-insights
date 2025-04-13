variable "project" {
  description = "The project ID to deploy resources to"
  type        = string
  default     = "github-data-insights" # Change your project ID here
}

variable "region" {
  description = "The location of the resources to create"
  type        = string
  default     = "europe-central2" # Change your region here
}

variable "credentials" {
  description = "The path to the service account key file"
  default     = "../.keys/gcp-service-acc.json" # Change your credentials path here
}

variable "location" {
  description = "The location of the resources to create"
  type        = string
  default     = "EU" # Change your location here
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket to create"
  type        = string
  default     = "gharchive_raw"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset to create"
  type        = string
  default     = "gharchive_data"
}

variable "bq_table_user_activity" {
  description = "The name of the BigQuery table to create"
  type        = string
  default     = "fact_user_activity"
}

variable "bq_table_repo_popularity" {
  description = "The name of the BigQuery table to create"
  type        = string
  default     = "fact_repo_popularity"
}

variable "bq_table_hourly_activity" {
  description = "The name of the BigQuery table to create"
  type        = string
  default     = "fact_hourly_activity"
}