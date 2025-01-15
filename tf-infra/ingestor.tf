# Service Account for Cloud Run job
resource "google_service_account" "selenium_ingestor_sa" {
  account_id   = "${var.APP_NAME}-selenium-ingestor"
  description  = "Selenium Ingestor Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Selenium Ingestor"
}

# Custom role
resource "google_project_iam_custom_role" "custom_storage_role" {
  role_id     = "${var.APP_NAME}_custom_storage_role"
  title       = "Custom Storage Role for ${var.APP_NAME}"
  description = "Custom role to grant specific storage permissions"
  project     = var.PROJECT
  permissions = [
    "storage.buckets.get",
    "storage.objects.create",
    "storage.objects.get",
    "storage.objects.list",
  ]
}

# SA and Custom role binding
resource "google_project_iam_member" "cloud_run_job_storage_permissions" {
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.selenium_ingestor_sa.email}"
  role    = google_project_iam_custom_role.custom_storage_role.name
}

# Job Definition
resource "google_cloud_run_v2_job" "selenium_ingestor_job" {
  name                = "${var.APP_NAME}-selenium-ingestor-job"
  location            = var.REGION
  deletion_protection = true
  labels              = local.labels

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.selenium_ingestor_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/selenium-ingestor:${var.DOCKER_IMAGE_TAG}"
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
        env {
          name  = "TEST_MODE"
          value = true
        }

      }
    }
  }
}
