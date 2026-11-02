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

variable "GCP_USER" {
  type        = string
  description = "GCP user email"
}
