# Service Account for Cloud Run job
resource "google_service_account" "pandas_transformer_sa" {
  account_id   = "${var.APP_NAME}-pandas-transformer"
  description  = "Pandas Transformer Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Pandas Transformer"
}

# Grant necessary permissions
resource "google_project_iam_member" "cloud_run_job_transformer_storage_permissions" {
  for_each = toset([
    "roles/storage.objectViewer",
    "roles/bigquery.jobUser",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.pandas_transformer_sa.email}"
  role    = each.value
}

# Job Definition
resource "google_cloud_run_v2_job" "pandas_transformer_job" {
  name                = "${var.APP_NAME}-pandas-transformer-job"
  location            = var.REGION
  deletion_protection = true
  labels              = local.labels

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.pandas_transformer_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/pandas-transformer:${var.DOCKER_IMAGE_TAG}"
        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
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

      }
    }
  }
}
