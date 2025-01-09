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
