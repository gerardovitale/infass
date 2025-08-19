# ------------------------------
# Ingestor Bucket
# ------------------------------
resource "google_storage_bucket" "infass_bucket" {
  name          = "${var.APP_NAME}-merc"
  force_destroy = false
  location      = var.REGION
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  labels = local.labels
}

resource "google_storage_bucket_iam_member" "bucket_permissions" {
  bucket = google_storage_bucket.infass_bucket.name
  member = "serviceAccount:${google_service_account.ingestor_sa.email}"
  role   = "roles/storage.legacyBucketWriter"
}

# ------------------------------
# Cloud Run job Service Account
# ------------------------------
resource "google_service_account" "ingestor_sa" {
  account_id   = "${var.APP_NAME}-ingestor"
  description  = "Ingestor Service Account created by terraform"
  display_name = "Cloud Run Job Service Account for Ingestor"
}

resource "google_project_iam_member" "cloud_run_job_ingestor_storage_permissions" {
  for_each = toset([
    "roles/storage.objectCreator",
    "roles/storage.objectViewer",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.ingestor_sa.email}"
  role    = each.value
}

# ------------------------------
# Cloud Run Job for Ingestor
# ------------------------------
resource "google_cloud_run_v2_job" "ingestor_job" {
  name                = "${var.APP_NAME}-ingestor-job"
  location            = var.REGION
  deletion_protection = false
  labels              = local.labels

  template {
    template {
      timeout         = "1200s"
      max_retries     = 1
      service_account = google_service_account.ingestor_sa.email

      containers {
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-ingestor:${var.DOCKER_IMAGE_TAG}"
        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
    }
  }
}
