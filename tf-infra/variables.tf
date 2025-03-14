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
