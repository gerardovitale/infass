variable "REGION" {
  type        = string
  description = "GCP region to deploy tf state related resources"
}

variable "APP_NAME" {
  type        = string
  description = "the GCP Project name"
}

variable "PROJECT" {
  type        = string
  description = "the GCP Project name"
}

variable "DOCKER_HUB_USERNAME" {
  type        = string
  description = "DockerHub username"
}

variable "DOCKER_IMAGE_TAG" {
  type        = string
  description = "Tag to be use when pull image from docker hub"
}

variable "GCP_USER" {
  type        = string
  description = "GCP user email"
}

variable "TRANSFORMER_LIMIT" {
  type        = string
  description = "Limit for the transformer job"
  default     = "1000"
}

variable "TRANSFORMER_WRITE_DISPOSITION" {
  type        = string
  description = "Write disposition for the transformer job"
}

variable "UI_SERVICE_ACCOUNT_EMAIL" {
  type    = string
  default = "infass-cloud-run-ui-sa@inflation-assistant-ui.iam.gserviceaccount.com"
}
