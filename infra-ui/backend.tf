terraform {
  required_version = ">= 1.5"

  backend "gcs" {
    bucket = "infass-ui-bucket-tf-state"
    prefix = "terraform/state"
  }
}
