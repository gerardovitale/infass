terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.34"
    }
  }
}

resource "google_project_iam_member" "cicd_service_account_act_as" {
  project = var.PROJECT
  member  = "serviceAccount:${google_service_account.cicd_service_account.email}"
  role    = "roles/iam.serviceAccountUser"
}

provider "google" {
  project = var.PROJECT
  region  = var.REGION
}
