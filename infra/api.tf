locals {
  volume_name       = "sqlite-vol"
  volume_mount_path = "/mnt/sqlite"
  sqlite_db_name    = "${var.APP_NAME}-sqlite-api.db"
}

# ------------------------------
# Cloud Storage Bucket
# ------------------------------
resource "google_storage_bucket" "sqlite_bucket" {
  name          = "${var.APP_NAME}-sqlite-bucket"
  location      = var.REGION
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  labels = local.labels
}

# ------------------------------
# Service Account for Cloud Run
# ------------------------------
resource "google_service_account" "cloud_run_sa" {
  account_id   = "${var.APP_NAME}-cloud-run-sa"
  display_name = "Cloud Run Service Account for ${var.APP_NAME}"
}

resource "google_storage_bucket_iam_member" "run_bucket_access" {
  bucket = google_storage_bucket.sqlite_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_project_iam_member" "cloud_run_sa_bigquery_jobuser" {
  project = var.PROJECT
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "cloud_run_sa_dataset_viewer" {
  dataset_id = google_bigquery_dataset.infass_test_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

# ------------------------------
# Cloud Run API Service
# ------------------------------
resource "google_cloud_run_v2_service" "api_service" {
  name                = "${var.APP_NAME}-api-service"
  location            = var.REGION
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    timeout         = "5s"
    service_account = google_service_account.cloud_run_sa.email

    containers {
      name  = "infass-api"
      image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-api:${var.DOCKER_IMAGE_TAG}"
      ports {
        container_port = 8080
      }
      volume_mounts {
        name       = local.volume_name
        mount_path = local.volume_mount_path
      }
      env {
        name  = "SQLITE_DB_PATH"
        value = "${local.volume_mount_path}/${local.sqlite_db_name}"
      }
    }

    volumes {
      name = local.volume_name
      gcs {
        bucket    = google_storage_bucket.sqlite_bucket.name
        read_only = true
      }
    }

  }

  labels = local.labels
}

# ------------------------------
# Cloud Run Job (Reversed ETL)
# ------------------------------
resource "google_cloud_run_v2_job" "reversed_etl_job" {
  name                = "${var.APP_NAME}-reversed-etl-job"
  location            = var.REGION
  deletion_protection = false

  template {
    template {
      timeout         = "300s"
      max_retries     = 0
      service_account = google_service_account.cloud_run_sa.email

      containers {
        name  = "infass-reversed-etl"
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-retl:${var.DOCKER_IMAGE_TAG}"
        volume_mounts {
          name       = local.volume_name
          mount_path = local.volume_mount_path
        }
        env {
          name  = "PROJECT_ID"
          value = var.PROJECT
        }
        env {
          name  = "DATASET_ID"
          value = google_bigquery_dataset.infass_test_dataset.dataset_id
        }
        env {
          name  = "BQ_TABLE"
          value = "dbt_ref_products"
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.sqlite_bucket.name
        }
        env {
          name  = "GCS_OBJECT"
          value = local.sqlite_db_name
        }
      }

      volumes {
        name = local.volume_name
        gcs {
          bucket = google_storage_bucket.sqlite_bucket.name
        }
      }
    }
  }

  labels = local.labels
}
