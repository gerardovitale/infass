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
