# Service Account for Cloud Run job
resource "google_service_account" "transformer_sa" {
  account_id   = "${var.APP_NAME}-transformer"
  description  = "Transformer Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Transformer"
}

# Grant necessary permissions
resource "google_project_iam_member" "cloud_run_job_transformer_storage_permissions" {
  for_each = toset([
    "roles/storage.objectUser",
    "roles/bigquery.jobUser",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.transformer_sa.email}"
  role    = each.value
}

# Job Definition
resource "google_cloud_run_v2_job" "transformer_job" {
  name                = "${var.APP_NAME}-transformer-job"
  location            = var.REGION
  deletion_protection = false
  labels              = local.labels

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.transformer_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.APP_NAME}-transformer:${var.DOCKER_IMAGE_TAG}"
        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }

        env {
          name  = "DATA_SOURCE"
          value = google_storage_bucket.infass_bucket.name
        }
        env {
          name  = "DESTINATION"
          value = "${google_bigquery_dataset.infass_dataset.project}.${google_bigquery_dataset.infass_dataset.dataset_id}.merc"
        }
        env {
          name  = "LIMIT"
          value = var.TRANSFORMER_LIMIT
        }
        env {
          name  = "WRITE_DISPOSITION"
          value = var.TRANSFORMER_WRITE_DISPOSITION
        }
      }
    }
  }
}

# ------------------------------
# Transformer V2 Job Definition
# ------------------------------

resource "google_cloud_run_v2_job" "transformer_v2_job" {
  name                = "${var.APP_NAME}-transformer-v2"
  location            = var.REGION
  deletion_protection = false
  labels              = local.labels

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.transformer_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.APP_NAME}-transformer-v2:${var.DOCKER_IMAGE_TAG}"

        volume_mounts {
          name       = local.volume_name
          mount_path = local.volume_mount_path
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
}
