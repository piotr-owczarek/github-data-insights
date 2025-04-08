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

resource "google_bigquery_table" "bq_tbl_raw_events" {
  dataset_id = var.bq_dataset_name
  table_id   = var.bq_table_name
  schema     = file("schema.json")
  deletion_protection=false

  external_data_configuration {
    source_format         = "PARQUET"
    source_uris           = ["gs://${var.gcs_bucket_name}/*.parquet"]
    autodetect            = false
    ignore_unknown_values = true
  }

  depends_on = [google_bigquery_dataset.bq_ds_gharchive_data]
}