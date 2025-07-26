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
  name                = "${var.APP_NAME}-ui-service"
  location            = var.REGION
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    timeout         = "5s"
    service_account = google_service_account.cloud_run_ui_sa.email

    containers {
      name  = "${var.APP_NAME}-ui"
      image = "docker.io/${var.DOCKER_HUB_USERNAME}/${var.APP_NAME}-ui:${var.DOCKER_IMAGE_TAG}"
      ports {
        container_port = 8080
      }
      env {
        name  = "API_BASE_URL"
        value = var.GCP_API_URL
      }
      env {
        name  = "USE_API_MOCKS"
        value = "false"
      }
    }


  }

}

resource "google_cloud_run_v2_service_iam_member" "public_access" {
  project  = google_cloud_run_v2_service.ui_service.project
  location = google_cloud_run_v2_service.ui_service.location
  name     = google_cloud_run_v2_service.ui_service.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
