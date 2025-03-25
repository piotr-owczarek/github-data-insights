variable "project" {
  description = "The project ID to deploy resources to"
  type        = string
  default     = "github-data-insights"
}

variable "region" {
  description = "The location of the resources to create"
  type        = string
  default     = "europe-central2"
}

variable "credentials" {
  description = "The path to the service account key file"
  default     = "./keys/gcp-service-acc.json"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket to create"
  type        = string
  default     = "github-raw-data"
}

variable "location" {
  description = "The location of the resources to create"
  type        = string
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset to create"
  type        = string
  default     = "github-raw"
}