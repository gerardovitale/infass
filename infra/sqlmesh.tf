# Service Account for SQLMesh
resource "google_service_account" "sqlmesh_sa" {
  account_id   = "${var.APP_NAME}-sqlmesh"
  description  = "SQLMesh Service Account created by terraform"
  display_name = "SQLMesh Service Account"
}

# Grant necessary permissions to the SQLMesh service account
resource "google_project_iam_member" "sqlmesh_permissions" {
  for_each = toset([
    "roles/bigquery.dataOwner",
    "roles/bigquery.user",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.sqlmesh_sa.email}"
  role    = each.value
}
