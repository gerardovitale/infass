# Enable APIs in the new project
resource "google_project_service" "required_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  project = var.PROJECT
  service = each.value
}

# Service Account for the new project
resource "google_service_account" "cicd_service_account" {
  depends_on   = [google_project_service.required_apis]
  project      = var.PROJECT
  account_id   = "${var.APP_NAME}-cicd"
  display_name = "CI/CD Service Account"
}

# Assign roles to the CI/CD Service Account
resource "google_project_iam_member" "cicd_service_account_roles" {
  depends_on = [google_project_service.required_apis]
  for_each = toset([
    "roles/storage.admin",
    "roles/run.admin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.workloadIdentityUser",
    "roles/resourcemanager.projectIamAdmin",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = each.value
}

resource "google_project_iam_member" "cicd_service_account_act_as" {
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = "roles/iam.serviceAccountUser"
}
output "cicd_service_account_email" {
  value = google_service_account.cicd_service_account.email
}
