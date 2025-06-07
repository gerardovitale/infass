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
        name       = "sqlite-vol"
        mount_path = "/mnt/sqlite"
      }
    }

    volumes {
      name = "sqlite-vol"
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
        # image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-reversed_etl:${var.DOCKER_IMAGE_TAG}"
        image = "alpine:latest"
        volume_mounts {
          name       = "sqlite-vol"
          mount_path = "/mnt/sqlite"
        }
      }

      volumes {
        name = "sqlite-vol"
        gcs {
          bucket = google_storage_bucket.sqlite_bucket.name
        }
      }
    }
  }

  labels = local.labels
}
