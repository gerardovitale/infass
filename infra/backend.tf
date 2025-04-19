terraform {
  backend "gcs" {
    bucket  = "infass-bucket-tf-state"
    prefix  = "terraform/state"
  }
}
