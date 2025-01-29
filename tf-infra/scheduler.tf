resource "google_service_account" "cloud_scheduler_sa" {
  account_id   = "${var.APP_NAME}-scheduler-run-invoker"
  display_name = "Cloud Scheduler Service Account that triggers Cloud Run Jobs"
}

resource "google_project_iam_member" "cloud_scheduler_run_invoker" {
  project = var.PROJECT
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_scheduler_sa.email}"
}

resource "google_cloud_scheduler_job" "trigger_cloud_run_job" {
  name        = "${var.APP_NAME}-trigger-cloud-run-job"
  description = "Trigger Cloud Run Job at 5 AM daily"
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
