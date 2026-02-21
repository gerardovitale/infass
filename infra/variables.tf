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

variable "DOCKER_IMAGE_TAG_INGESTOR" {
  type        = string
  description = "Docker image tag for the ingestor service"
}

variable "DOCKER_IMAGE_TAG_TRANSFORMER_V2" {
  type        = string
  description = "Docker image tag for the transformer-v2 service"
}

variable "DOCKER_IMAGE_TAG_API" {
  type        = string
  description = "Docker image tag for the api service"
}

variable "DOCKER_IMAGE_TAG_RETL" {
  type        = string
  description = "Docker image tag for the retl service"
}

variable "GCP_USER" {
  type        = string
  description = "GCP user email"
}

variable "UI_SERVICE_ACCOUNT_EMAIL" {
  type    = string
  default = "infass-cloud-run-ui-sa@inflation-assistant-ui.iam.gserviceaccount.com"
}
