terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.28.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials)
}

resource "google_storage_bucket" "gcs_gharchive_raw" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "bq_ds_gharchive_data" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_bigquery_table" "bq_tbl_user_activity" {
  dataset_id = var.bq_dataset_name
  table_id   = var.bq_table_user_activity
  description = "Table for user activity data"
  schema     = file("user_activity.json")
  deletion_protection=true

  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  clustering = ["user_type", "hour"]  

  depends_on = [google_bigquery_dataset.bq_ds_gharchive_data]
}

resource "google_bigquery_table" "bq_tbl_repo_popularity" {
  dataset_id = var.bq_dataset_name
  table_id   = var.bq_table_repo_popularity
  description = "Table for repository popularity data"
  schema     = file("repo_popularity.json")
  deletion_protection=true

  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  clustering = ["organization", "repository"] 

  depends_on = [google_bigquery_dataset.bq_ds_gharchive_data]
}

resource "google_bigquery_table" "bq_tbl_hourly_activity" {
  dataset_id = var.bq_dataset_name
  table_id   = var.bq_table_hourly_activity
  description = "Table for hopr activity data"
  schema     = file("hourly_activity.json")
  deletion_protection=true

  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  clustering = ["hour"] 

  depends_on = [google_bigquery_dataset.bq_ds_gharchive_data]
}