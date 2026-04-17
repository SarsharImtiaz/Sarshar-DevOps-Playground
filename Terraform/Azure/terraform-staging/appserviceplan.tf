# Define service plan
resource "azurerm_service_plan" "appserviceplan" {
  name                = var.app_service_plan_name
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
  sku_name            = var.app_service_plan_sku
  os_type             = title(var.os_type)
}
 
# Frontend App Service
resource "azurerm_linux_web_app" "frontendappservice" {
  name                = var.frontend_app_service_name
 location             = var.resource_group_location
  resource_group_name = var.resource_group_name
  service_plan_id     = azurerm_service_plan.appserviceplan.id
 
  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.frontend_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }
 
      http2_enabled   = true   # Enable HTTP/2
        }
 
  https_only = true  # Enforce HTTPS
 
  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "DOCKER_REGISTRY_SERVER_URL"          = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"     = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"     = var.docker_registry_password
    "apiAppIdURI"                         = var.bec_api_app_id_uri
    "apiUrl"                              = "https://${var.custom_domain_api}/v1/"
    "clientId"                            = var.bec_webapp_client_id
    "newRelicEnv"                         = "staging"
    "redirectUri"                         = "https://${var.bec_web_url}/dashboard"
    "iManageClientId"                     = var.imanage_app_id
    "iManageConnectorUrl"                 = var.imanage_connector_url
    "NetDocsServer"                       = var.netdocs_server
    "NetDocsClientId"                     = var.netdocs_client_id
    "NetDocsClientSecret"                 = var.netdocs_client_secret
    "NetDocsRedirectUrl"                  = var.netdocs_redirect_url
  }
 
  identity {
    type = "SystemAssigned"
  }
}
 
# Backend App Service
resource "azurerm_linux_web_app" "backendappservice" {
  name                = var.backend_app_service_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  service_plan_id     = azurerm_service_plan.appserviceplan.id
 
  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.backend_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }
 
    http2_enabled   = true   # Enable HTTP/2
  }
  https_only = true  # Enforce HTTPS
 
  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "DOCKER_REGISTRY_SERVER_URL"          = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"     = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"     = var.docker_registry_password
    "AzureAd__Audience"                   = var.bec_api_app_id_uri
    "AzureAd__ClientId"                   = var.bec_api_app_id
    "BECConfigurationOptions__HostingTenantId" = var.bec_hosting_tenant_id
  #  "BECConfigurationOptions__KeyVaultSecretName" = azurerm_key_vault_secret.CatalogConnectionStringStaging.name
    "BECConfigurationOptions__KeyVaultUrl" = "https://${var.keyvault_name}.vault.azure.net"
    BECConfigurationOptions__KeyVaultSecretName = var.keyvault_secret_name
    "NEW_RELIC_APP_NAME"                  = "BEC-API-STAGING"
    "NEW_RELIC_LICENSE_KEY"               = "test"
  }
 
  identity {
    type = "SystemAssigned"
  }
}
 
 
# iManage Connector App Service


resource "azurerm_linux_web_app" "integratorappservice" {
  name                = var.imanage_app_service_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  service_plan_id     = azurerm_service_plan.appserviceplan.id
 
  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.imanage_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }
 
    http2_enabled   = true   # Enable HTTP/2
  }
  https_only = true  # Enforce HTTPS
 
  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"
    "DOCKER_REGISTRY_SERVER_URL"          = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"     = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"     = var.docker_registry_password
    "NEW_RELIC_APP_NAME"                  = "BEC-integrator-STAGING"
    "NEW_RELIC_LICENSE_KEY"               = "test"
    "BEC__WebURL" = "https://${var.bec_web_url}"
    "iManage__AppId" = var.imanage_app_id
    "iManage__AppSecret" = var.imanage_app_secret
    "iManage__RedirectURL" = var.imanage_redirect_url
    "iManage__Scope" = var.imanage_scope
    "iManage__Server" = var.imanage_server
    "NetDocs__Server" = var.netdocs_server
    "NetDocs__ClientId" = var.netdocs_client_id
    "NetDocs__ClientSecret" = var.netdocs_client_secret
    "NetDocs__RedirectUrl" = var.netdocs_redirect_url
  }
 
  identity {
    type = "SystemAssigned"
  }
}

# Outlook Add-in App Service
resource "azurerm_linux_web_app" "outlookaddinappservice" {
  name                = var.outlook_addin_app_service_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  service_plan_id     = azurerm_service_plan.appserviceplan.id

  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.outlook_addin_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }

    http2_enabled = true
  }

  https_only = true

  identity {
    type = "SystemAssigned"
  }
}

# Custom Domain Assignment
resource "azurerm_app_service_custom_hostname_binding" "frontend_custom_domain" {
  hostname            = "appstaging.corerelate.cloud"
  app_service_name    = azurerm_linux_web_app.frontendappservice.name
  resource_group_name = var.resource_group_name

  ssl_state           = "SniEnabled"
  thumbprint          = "DC82CE68F73F0F9E3A5F634D09011D966586B71B"

  depends_on = [azurerm_linux_web_app.frontendappservice]
}

# Custom Domain Assignment
resource "azurerm_app_service_custom_hostname_binding" "backend_custom_domain" {
  hostname            = "apistaging.corerelate.cloud"
  app_service_name    = azurerm_linux_web_app.backendappservice.name
  resource_group_name = var.resource_group_name

  ssl_state           = "SniEnabled"
  thumbprint          = "DC82CE68F73F0F9E3A5F634D09011D966586B71B"

  depends_on = [azurerm_linux_web_app.backendappservice]
}

# Custom Domain Assignment
resource "azurerm_app_service_custom_hostname_binding" "integrator_custom_domain" {
  hostname            = "integratorstaging.corerelate.cloud"
  app_service_name    = azurerm_linux_web_app.integratorappservice.name
  resource_group_name = var.resource_group_name

  ssl_state           = "SniEnabled"
  thumbprint          = "DC82CE68F73F0F9E3A5F634D09011D966586B71B"

  depends_on = [azurerm_linux_web_app.integratorappservice]
}

# Custom Domain Assignment
resource "azurerm_app_service_custom_hostname_binding" "outlookaddin_custom_domain" {
  hostname            = "outlookaddinstaging.corerelate.cloud"
  app_service_name    = azurerm_linux_web_app.outlookaddinappservice.name
  resource_group_name = var.resource_group_name

  ssl_state  = "SniEnabled"
  thumbprint = "DC82CE68F73F0F9E3A5F634D09011D966586B71B"

  depends_on = [azurerm_linux_web_app.outlookaddinappservice]
}


################## Schedule Assist App Service and App Service Plan ##################


# Define service plan
resource "azurerm_service_plan" "aiappserviceplan" {
  name                = var.ai_app_service_plan_name
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
  sku_name            = var.ai_app_service_plan_sku
  os_type             = title(var.os_type)
}
 
# Frontend App Service
resource "azurerm_linux_web_app" "aiappservice" {
  name                = var.ai_app_service_name
 location             = var.resource_group_location
  resource_group_name = var.resource_group_name
  service_plan_id     = azurerm_service_plan.aiappserviceplan.id
 
  site_config {
    application_stack {
      docker_registry_url      = var.docker_registry_url
      docker_image_name        = var.frontend_docker_image_name
      docker_registry_username = var.docker_registry_user
      docker_registry_password = var.docker_registry_password
    }
 
      http2_enabled   = true   # Enable HTTP/2
        }
 
  https_only = true  # Enforce HTTPS
 
  app_settings = {
    "DOCKER_REGISTRY_SERVER_URL"          = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"     = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"     = var.docker_registry_password
    "newRelicEnv"                         = "staging"
    # "AZURE_CONNECTION_STRING"             = var.azure_connection_string
    # "AZURE_OPENAI_API_VERSION"            = var.azure_openai_api_version
    # "AZURE_OPENAI_ENDPOINT"               = var.azure_openai_endpoint
    # "AZURE_OPENAI_KEY"                    = var.azure_openai_key
    # "GPT_INPUT_RATE"                      = var.gpt_input_rate
    # "GPT_MODEL_NAME"                      = var.gpt_model_name
    # "GPT_OUTPUT_RATE"                     = var.gpt_output_rate
    # "GPT_TOKEN_LIMIT"                     = var.gpt_token_limit
    # "HTTP_PORT"                           = var.http_port
    # "NEW_RELIC_APP_NAME"                  = var.new_relic_app_name
    # "NEW_RELIC_LICENSE_KEY"               = var.new_relic_license_key
    # "POPPLER_PATH"                         = var.poppler_path
    # "WEBSITE_HTTPLOGGING_RETENTION_DAYS"   = var.website_httplogging_retention_days
    # "WEBSITES_ENABLE_APP_SERVICE_STORAGE"  = var.websites_enable_app_service_storage
    
    "AZURE_CONNECTION_STRING"             = "TestValue"
    "AZURE_OPENAI_API_VERSION"            = "2023-05-15"
    "AZURE_OPENAI_ENDPOINT"               = "https://becopenai.openai.azure.com/"
    "AZURE_OPENAI_KEY"                    = "test"
    "GPT_INPUT_RATE"                      = "0.0028"
    "GPT_MODEL_NAME"                      = "gpt-4o"
    "GPT_OUTPUT_RATE"                     = "0.0110"
    "GPT_TOKEN_LIMIT"                     = "40000"
    "HTTP_PORT"                           = "5000"
    "POPPLER_PATH"                        = "/usr/bin"
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE" = "false"

  }
 
  identity {
    type = "SystemAssigned"
  }
}
 