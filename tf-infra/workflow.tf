# Create a dedicated service account
resource "google_service_account" "workflow_sa" {
  account_id   = "${var.APP_NAME}-workflows-sa"
  display_name = "Workflows Service Account"
}

resource "google_project_iam_member" "workflow_permissions" {
  for_each = toset([
    "roles/run.invoker",
    "roles/dataproc.editor",
    "roles/compute.instanceAdmin.v1",
    "roles/iam.serviceAccountUser",
    "roles/storage.objectViewer",
    "roles/logging.logWriter",
  ])
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
  role    = each.value
}

# Read the workflow.yaml
data "local_file" "workflow_yaml" {
  filename = "${path.module}/../workflow.yaml"
}

# Create a workflow
resource "google_workflows_workflow" "default" {
  name            = "${var.APP_NAME}-workflow"
  region          = var.REGION
  description     = "Infass Workflow"
  service_account = google_service_account.workflow_sa.id
  labels          = local.labels
  user_env_vars = {
    url = "https://timeapi.io/api/Time/current/zone?timeZone=Europe/Amsterdam"
  }
  source_contents = data.local_file.workflow_yaml
}
