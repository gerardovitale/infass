# Service Account for DBT
resource "google_service_account" "dbt_sa" {
  account_id   = "${var.APP_NAME}-dbt"
  description  = "DBT Service Account created by terraform"
  display_name = "DBT Service Account"
}

# Grant necessary permissions to the DBT service account
resource "google_project_iam_member" "dbt_permissions" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/bigquery.dataEditor",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.dbt_sa.email}"
  role    = each.value
}
