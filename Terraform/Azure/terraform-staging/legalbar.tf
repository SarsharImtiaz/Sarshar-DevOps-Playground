# Frontend App Service
resource "azurerm_linux_web_app" "legalbaraddinappservice" {
  name                = var.legalbar_addin_app_service_name
 location             = var.resource_group_location
  resource_group_name = var.resource_group_name
  service_plan_id     = azurerm_service_plan.appserviceplan.id

  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.legalbar_addin_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }
  }

  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "DOCKER_REGISTRY_SERVER_URL"          = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"     = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"     = var.docker_registry_password
  }

  identity {
    type = "SystemAssigned"
  }
}

# Backend App Service
resource "azurerm_linux_web_app" "legalbarbackendappservice" {
  name                = var.legalbar_backend_app_service_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  service_plan_id     = azurerm_service_plan.appserviceplan.id

  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.legalbar_backend_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }
  }

  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "DOCKER_REGISTRY_SERVER_URL"          = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"     = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"     = var.docker_registry_password
  }

  identity {
    type = "SystemAssigned"
  }
}
