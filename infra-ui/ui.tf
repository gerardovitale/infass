# ------------------------------
# Service Account for Cloud Run
# ------------------------------
resource "google_service_account" "cloud_run_ui_sa" {
  account_id   = "${var.APP_NAME}-cloud-run-ui-sa"
  display_name = "Cloud Run Service Account for ${var.APP_NAME}-ui"
}

# ------------------------------
# Cloud Run UI Service
# ------------------------------
resource "google_cloud_run_v2_service" "ui_service" {
  name                 = "${var.APP_NAME}-ui-service"
  location             = var.REGION
  ingress              = "INGRESS_TRAFFIC_ALL"
  deletion_protection  = false
  invoker_iam_disabled = true

  template {
    timeout         = "30s"
    service_account = google_service_account.cloud_run_ui_sa.email

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }

    containers {
      name  = "${var.APP_NAME}-ui"
      image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.APP_NAME}-ui:${var.DOCKER_IMAGE_TAG}"
      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }

      env {
        name  = "API_BASE_URL"
        value = var.GCP_API_URL
      }
      env {
        name  = "USE_API_MOCKS"
        value = "false"
      }

      startup_probe {
        http_get {
          path = "/"
          port = 8080
        }
        initial_delay_seconds = 2
        period_seconds        = 5
        failure_threshold     = 3
      }

      liveness_probe {
        http_get {
          path = "/"
          port = 8080
        }
        period_seconds    = 15
        failure_threshold = 3
      }
    }
  }
}

# ------------------------------
# Outputs
# ------------------------------
output "ui_service_url" {
  value = google_cloud_run_v2_service.ui_service.uri
}
