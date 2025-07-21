terraform {
  backend "gcs" {
    bucket = "infass-ui-bucket-tf-state"
    prefix = "terraform/state"
  }
}
