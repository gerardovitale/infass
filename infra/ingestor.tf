# Service Account for Cloud Run job
resource "google_service_account" "selenium_ingestor_sa" {
  account_id   = "${var.APP_NAME}-ingestor"
  description  = "Selenium Ingestor Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Selenium Ingestor"
}

# Grant necessary permissions
resource "google_project_iam_member" "cloud_run_job_ingestor_storage_permissions" {
  for_each = toset([
    "roles/storage.objectCreator",
    "roles/storage.objectViewer",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.selenium_ingestor_sa.email}"
  role    = each.value
}

# Job Definition
resource "google_cloud_run_v2_job" "selenium_ingestor_job" {
  name                = "${var.APP_NAME}-ingestor-job"
  location            = var.REGION
  deletion_protection = false
  labels              = local.labels

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.selenium_ingestor_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/ingestor:${var.DOCKER_IMAGE_TAG}"
        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
        env {
          name  = "INGESTION_MERC_PATH"
          value = "gs://${google_storage_bucket.infass_bucket.name}/merc"
        }
        # env {
        #   name  = "TEST_MODE"
        #   value = true
        # }

      }
    }
  }
}
