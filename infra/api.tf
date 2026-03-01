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
  force_destroy = false
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
resource "google_service_account" "api_service_account" {
  account_id   = "${var.APP_NAME}-cloud-run-sa"
  display_name = "Cloud Run Service Account for ${var.APP_NAME}"
}

resource "google_storage_bucket_iam_member" "run_bucket_access" {
  bucket = google_storage_bucket.sqlite_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.api_service_account.email}"
}

resource "google_project_iam_member" "api_service_account_bigquery_jobuser" {
  project = var.PROJECT
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.api_service_account.email}"
}

resource "google_bigquery_dataset_iam_member" "api_service_account_dataset_viewer" {
  for_each = toset([
    google_bigquery_dataset.infass_dataset.dataset_id,
    google_bigquery_dataset.infass_test_dataset.dataset_id,
  ])
  dataset_id = each.value
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.api_service_account.email}"
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
    timeout         = "30s"
    service_account = google_service_account.api_service_account.email

    scaling {
      min_instance_count = 1
      max_instance_count = 3
    }

    containers {
      name  = "infass-api"
      image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-api:${var.DOCKER_IMAGE_TAG_API}"
      ports {
        container_port = 8080
      }
      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }
      volume_mounts {
        name       = local.volume_name
        mount_path = local.volume_mount_path
      }
      env {
        name  = "SQLITE_DB_PATH"
        value = "${local.volume_mount_path}/${local.sqlite_db_name}"
      }
      startup_probe {
        tcp_socket {
          port = 8080
        }
        initial_delay_seconds = 2
        period_seconds        = 5
        failure_threshold     = 3
      }
    }

    volumes {
      name = local.volume_name
      gcs {
        bucket = google_storage_bucket.sqlite_bucket.name
      }
    }

  }

  labels = local.labels
}

data "google_iam_policy" "api_service" {
  binding {
    role = "roles/run.invoker"
    members = [
      "serviceAccount:${var.UI_SERVICE_ACCOUNT_EMAIL}",
    ]
  }
}

resource "google_cloud_run_service_iam_policy" "api_service" {
  location = google_cloud_run_v2_service.api_service.location
  project  = google_cloud_run_v2_service.api_service.project
  service  = google_cloud_run_v2_service.api_service.name

  policy_data = data.google_iam_policy.api_service.policy_data
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
      service_account = google_service_account.api_service_account.email

      containers {
        name  = "infass-reversed-etl"
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-retl:${var.DOCKER_IMAGE_TAG_RETL}"
        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
        volume_mounts {
          name       = local.volume_name
          mount_path = local.volume_mount_path
        }
        env {
          name  = "BQ_PROJECT_ID"
          value = var.PROJECT
        }
        env {
          name  = "BQ_DATASET_ID"
          value = google_bigquery_dataset.infass_dataset.dataset_id
        }
        env {
          name  = "SQLITE_DB_PATH"
          value = "${local.volume_mount_path}/${local.sqlite_db_name}"
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

resource "google_cloud_run_v2_job" "test_reversed_etl_job" {
  name                = "${var.APP_NAME}-test-reversed-etl-job"
  location            = var.REGION
  deletion_protection = false

  template {
    template {
      timeout         = "1200s"
      max_retries     = 0
      service_account = google_service_account.api_service_account.email

      containers {
        name  = "infass-reversed-etl"
        image = "docker.io/${var.DOCKER_HUB_USERNAME}/infass-retl:${var.DOCKER_IMAGE_TAG_RETL}"
        volume_mounts {
          name       = local.volume_name
          mount_path = local.volume_mount_path
        }
        env {
          name  = "BQ_PROJECT_ID"
          value = var.PROJECT
        }
        env {
          name  = "BQ_DATASET_ID"
          value = google_bigquery_dataset.infass_test_dataset.dataset_id
        }
        env {
          name  = "SQLITE_DB_PATH"
          value = "${local.volume_mount_path}/${var.APP_NAME}-test-sqlite-api.db"
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
