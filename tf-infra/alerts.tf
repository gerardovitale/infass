resource "google_monitoring_notification_channel" "email" {
  display_name = "Cloud Run Job Failure Alert"
  type         = "email"
  labels = {
    email_address = var.GCP_USER
  }
}

resource "google_monitoring_alert_policy" "job_failure_alert" {
  display_name = "Cloud Run Job Failure Alert"
  combiner     = "OR"
  conditions {
    display_name = "Job Failed Condition"
    condition_threshold {
      filter          = "metric.type=\"run.googleapis.com/job/completed_execution_count\" AND resource.type=\"cloud_run_revision\" AND metric.labels.result=\"failed\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.email.name]
}
