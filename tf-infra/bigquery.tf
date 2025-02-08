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
  display_name           = "Weekly Data Quality Report"
  data_source_id         = "scheduled_query"
  location               = "EU"
  schedule               = "every saturday 09:00"
  service_account_name   = google_service_account.bigquery_schedule_job_sa.email
  project                = "inflation-assistant"
  destination_dataset_id = "infass"
  disabled               = false

  params = {
    destination_table_id = "quality_report"
    write_disposition    = "WRITE_TRUNCATE"
    query                = <<EOT
      DECLARE run_date DATE DEFAULT CURRENT_DATE();

      WITH null_counts AS (
        SELECT
          date,
          COUNT(*) AS total_rows,
          COUNTIF(dedup_id IS NULL) AS null_dedup_id,
          COUNTIF(name IS NULL) AS null_name,
          COUNTIF(size IS NULL) AS null_size,
          COUNTIF(category IS NULL) AS null_category,
          COUNTIF(subcategory IS NULL) AS null_subcategory,
          COUNTIF(original_price IS NULL) AS null_original_price,
          COUNTIF(prev_original_price IS NULL) AS null_prev_original_price,
          COUNTIF(discount_price IS NULL) AS null_discount_price,
          COUNTIF(is_fake_discount IS NULL) AS null_is_fake_discount,
          COUNTIF(inflation_percent IS NULL) AS null_inflation_percent,
          COUNTIF(inflation_abs IS NULL) AS null_inflation_abs
        FROM `inflation-assistant.infass.merc`
        GROUP BY 1
    ),
    duplicate_counts AS (
      SELECT
        date,
        SUM(duplicate_count - 1) AS duplicate_rows
      FROM (
        SELECT
          date,
          COUNT(*) AS duplicate_count
        FROM `inflation-assistant.infass.merc`
        GROUP BY
          date,
          name,
          size,
          category,
          subcategory,
          original_price,
          discount_price
      )
      WHERE duplicate_count > 1
      GROUP BY date
    )

    SELECT
      nc.date,
      nc.total_rows,
      COALESCE(dc.duplicate_rows, 0) AS duplicate_rows,
      nc.null_dedup_id,
      nc.null_name,
      nc.null_size,
      nc.null_category,
      nc.null_subcategory,
      nc.null_original_price,
      nc.null_prev_original_price,
      nc.null_discount_price,
      nc.null_is_fake_discount,
      nc.null_inflation_percent,
      nc.null_inflation_abs
    FROM null_counts nc
    LEFT JOIN duplicate_counts dc ON nc.date = dc.date
    ORDER BY nc.date DESC;
    EOT
  }
}
