locals {
  labels = {
    "environment" = "production",
    "project"     = var.PROJECT,
    "manage_by"   = "${var.APP_NAME}-remote-tf",
  }
}
