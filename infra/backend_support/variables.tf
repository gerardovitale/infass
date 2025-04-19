variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}

variable "APP_NAME" {
  type        = string
  description = "Application name"
}

variable "PROJECT" {
  type        = string
  description = "GCP Project name"
}
