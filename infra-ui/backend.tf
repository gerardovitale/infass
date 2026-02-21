terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.34"
    }
  }

  # NOTE: bucket name is hardcoded because backend blocks don't support variables.
  # Must match the bucket created by backend_support/ (APP_NAME-bucket-tf-state).
  backend "gcs" {
    bucket = "infass-ui-bucket-tf-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.PROJECT
  region  = var.REGION
}
