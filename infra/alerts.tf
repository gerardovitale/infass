resource "google_monitoring_notification_channel" "email" {
  display_name = "${var.APP_NAME} Cloud Run Job Failure Alert"
  type         = "email"
  labels = merge(
    { email_address = var.GCP_USER },
    local.labels
  )
}

resource "google_monitoring_alert_policy" "cloud_run_job_failure_alert" {
  display_name = "${var.APP_NAME} Cloud Run Job Failure Alert"
  combiner     = "OR"
  severity     = "ERROR"

  conditions {
    display_name = "Job Failed Condition"
    condition_threshold {
      filter          = "resource.type = \"cloud_run_job\" AND metric.type = \"run.googleapis.com/job/completed_execution_count\" AND metric.labels.result = \"failed\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
  user_labels           = local.labels
}
