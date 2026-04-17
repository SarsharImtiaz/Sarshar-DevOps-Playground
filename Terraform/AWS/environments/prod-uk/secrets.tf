module "secrets_manager" {
  source = "../../modules/secrets_manager"

  secrets     = var.secrets
  environment = var.environment
  project     = var.project
}