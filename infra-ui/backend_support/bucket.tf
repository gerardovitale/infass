resource "google_storage_bucket" "tf_state_bucket" {
  name          = "${var.APP_NAME}-bucket-tf-state"
  force_destroy = false
  location      = var.REGION
  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  labels = local.labels
}
