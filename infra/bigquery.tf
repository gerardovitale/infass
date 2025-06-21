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
    user_by_email = google_service_account.transformer_sa.email
  }

  access {
    role          = "READER"
    user_by_email = google_service_account.api_service_account.email
  }

  access {
    role          = "OWNER"
    user_by_email = var.GCP_USER
  }
}

resource "google_bigquery_dataset" "infass_test_dataset" {
  dataset_id    = "test_infass"
  project       = var.PROJECT
  location      = "EU"
  friendly_name = "Infass Test Dataset"
  description   = "Dataset for testing dbt transformations"
  labels        = local.labels

  access {
    role          = "OWNER"
    user_by_email = google_service_account.transformer_sa.email
  }

  access {
    role          = "READER"
    user_by_email = google_service_account.api_service_account.email
  }

  access {
    role          = "OWNER"
    user_by_email = var.GCP_USER
  }
}
