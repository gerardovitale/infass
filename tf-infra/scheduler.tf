resource "google_service_account" "cloud_scheduler_sa" {
  account_id   = "${var.APP_NAME}-scheduler-run-invoker"
  display_name = "Cloud Scheduler Service Account that triggers Cloud Run Jobs"
}

resource "google_project_iam_member" "cloud_scheduler_run_invoker" {
  project = var.PROJECT
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_scheduler_sa.email}"
}


resource "google_cloud_scheduler_job" "trigger_ingestion_job_daily" {
  name        = "${var.APP_NAME}-trigger-ingestion-job-daily"
  description = "Trigger Ingestion Cloud Run Job at 5 AM CET daily"
  region      = "europe-west1"
  schedule    = "0 8 * * *"
  time_zone   = "Europe/Madrid" #"Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.PROJECT}/jobs/${google_cloud_run_v2_job.selenium_ingestor_job.name}:run"
    oauth_token {
      service_account_email = google_service_account.cloud_scheduler_sa.email
    }
  }
}


resource "google_cloud_scheduler_job" "trigger_transformer_weekly" {
  name        = "${var.APP_NAME}-trigger-transformer-job-weekly"
  description = "Trigger Transformer Cloud Run Job every Saturday at 9 AM CET"
  region      = "europe-west1"
  schedule    = "0 9 * * 6"     # ⏰ Runs every Saturday at 8:00 AM UTC
  time_zone   = "Europe/Madrid" #"Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.PROJECT}/jobs/${google_cloud_run_v2_job.pandas_transformer_job.name}:run"
    oauth_token {
      service_account_email = google_service_account.cloud_scheduler_sa.email
    }
  }
}
