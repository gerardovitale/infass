# InfAss Dataset definition
resource "google_bigquery_dataset" "infass_dataset" {
  dataset_id    = "infass"
  project       = var.PROJECT
  location      = "EU"
  friendly_name = "Infass Dataset"
  description   = "Dataset for storing inflation assistant data"
  labels        = local.labels

  access {
    role          = "OWNER"
    user_by_email = google_service_account.pandas_transformer_sa.email
  }

  access {
    role          = "READER"
    user_by_email = var.GCP_USER
  }
}


# Service Account for BigQuery Schedule Job
resource "google_service_account" "bigquery_schedule_job_sa" {
  account_id   = "${var.APP_NAME}-bigquery-schedule-job"
  description  = "BigQuery Schedule Job Service Account created by terraform"
  display_name = "BigQuery Service Account for Schedule Jobs"
}

# Grant necessary permissions
resource "google_project_iam_member" "bigquery_schedule_job_permissions" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/bigquery.dataEditor",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.bigquery_schedule_job_sa.email}"
  role    = each.value
}


# BigQuery Schedule Weekly Data Quality Report
resource "google_bigquery_data_transfer_config" "weekly_data_quality_report" {
  display_name         = "Weekly Data Quality Report"
  data_source_id       = "scheduled_query"
  location             = "EU"
  schedule             = local.schedules.post_transformation
  service_account_name = google_service_account.bigquery_schedule_job_sa.email
  project              = "inflation-assistant"
  disabled             = false

  params = {
    query = file("${path.module}/bq_queries/data_quality_report.sql")
  }
}


# BigQuery Schedule Weekly Product Inflation Report
resource "google_bigquery_data_transfer_config" "weekly_product_inflation_report" {
  display_name         = "Weekly Product Inflation Report"
  data_source_id       = "scheduled_query"
  location             = "EU"
  schedule             = local.schedules.post_transformation
  service_account_name = google_service_account.bigquery_schedule_job_sa.email
  project              = "inflation-assistant"
  disabled             = false

  params = {
    query = file("${path.module}/bq_queries/product_inflation_report.sql")
  }
}
