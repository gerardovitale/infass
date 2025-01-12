resource "google_storage_bucket" "infass_bucket" {
  name          = "${var.APP_NAME}-merc"
  force_destroy = true
  location      = var.REGION
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  labels = local.labels
}

resource "google_storage_bucket_iam_member" "bucket_permissions" {
  for_each = toset([
    "roles/storage.objectViewer",
    "roles/storage.objectAdmin",
  ])
  bucket = google_storage_bucket.infass_bucket.name
  member = "serviceAccount:${google_service_account.selenium_ingestor_sa.email}"
  role   = each.value
}
