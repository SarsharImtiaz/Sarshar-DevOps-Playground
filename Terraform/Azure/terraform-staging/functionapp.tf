#Audit Function App Creation

resource "azurerm_storage_account" "audit_storage" {
  name                     = var.audit_storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  account_kind             = "StorageV2"
}

resource "azurerm_linux_function_app" "audit_function_app" {
  name                       = var.audit_function_app_name
  location                   = var.resource_group_location
  resource_group_name        = var.resource_group_name
  service_plan_id            = azurerm_service_plan.appserviceplan.id
  storage_account_name       = azurerm_storage_account.audit_storage.name
  storage_account_access_key = azurerm_storage_account.audit_storage.primary_access_key
  https_only                 = true

  site_config {
    always_on = true
    http2_enabled = true  # Enable HTTP/2

      application_stack {
      docker {
        image_name = var.audit_docker_image_name
        image_tag = var.audit_docker_image_tag
        registry_url = var.docker_registry_url
        registry_username = var.docker_registry_user
        registry_password = var.docker_registry_password
      }
    }
  }

  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE"      = "false"
    "DOCKER_REGISTRY_SERVER_URL"               = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"          = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"          = var.docker_registry_password
    "BEC__keyVaultSecretName"                  = azurerm_key_vault_secret.CatalogConnectionStringStaging.name
    "BEC__keyVaultUrl"                         = "https://${var.keyvault_name}.vault.azure.net"
    "BECServiceBus__fullyQualifiedNamespace"   = "${var.service_bus_namespace}.servicebus.windows.net"
    "WEBSITE_CONTENTOVERVNET"                  = "1"
    "AzureWebJobsStorage__accountName"         = var.audit_storage_account_name
    "NEW_RELIC_APP_NAME"                       = "BEC-FUNCTION-audit-STAGING"
    "NEW_RELIC_LICENSE_KEY"                    = "6e841b6bef7d172d66182193aaafaccdFFFFNRAL"
  }

  identity {
    type = "SystemAssigned"
  }
}

# Service bus queue creation

resource "azurerm_servicebus_queue" "audit_queue" {
  name                = "audit"
  namespace_id        = azurerm_servicebus_namespace.service_bus.id
#  enable_partitioning = false
  requires_session    = true
}

resource "azurerm_role_assignment" "function_app_data_owner" {
  scope                = azurerm_servicebus_queue.audit_queue.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = azurerm_linux_function_app.audit_function_app.identity[0].principal_id
}


resource "azurerm_role_assignment" "app_service_data_sender" {
  scope                = azurerm_servicebus_namespace.service_bus.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
}


#Services Function App Creation

resource "azurerm_storage_account" "syncservice_storage" {
  name                     = var.syncservice_storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  account_kind             = "StorageV2"
}

resource "azurerm_linux_function_app" "syncservice_function_app" {
  name                       = var.syncservice_function_app_name
  location                   = var.resource_group_location
  resource_group_name        = var.resource_group_name
  service_plan_id            = azurerm_service_plan.appserviceplan.id
  storage_account_name       = azurerm_storage_account.syncservice_storage.name
  storage_account_access_key = azurerm_storage_account.syncservice_storage.primary_access_key
  https_only                 = true

  site_config {
    always_on = true
    http2_enabled = true  # Enable HTTP/2

    #linux_fx_version = "DOCKER|${var.docker_registry_url}/${var.syncservice_docker_image_name}"

    application_stack {
      docker {
        image_name = var.syncservice_docker_image_name
        image_tag = var.services_docker_image_tag
        registry_url = var.docker_registry_url
        registry_username = var.docker_registry_user
        registry_password = var.docker_registry_password
      }
    }
  }

  app_settings = {
    "WEBSITES_ENABLE_APP_SERVICE_STORAGE"                    = "false"
    "DOCKER_REGISTRY_SERVER_URL"                             = var.docker_registry_url
    "DOCKER_REGISTRY_SERVER_USERNAME"                        = var.docker_registry_user
    "DOCKER_REGISTRY_SERVER_PASSWORD"                        = var.docker_registry_password
    "AzureWebJobsStorage_accountName"                        = var.syncservice_storage_account_name
    "BEC__apiAppIdURI"                                       = var.bec_api_app_id_uri
    "BEC__apiURL"                                            = "https://${var.custom_domain_api}/v1"
    "BEC__clientId"                                          = var.bec_client_id_schedulesync
    "BEC__keyVaultSecretNameForCatalogConnectionString"      = azurerm_key_vault_secret.CatalogConnectionStringStaging.name
    "BEC__keyVaultSecretNameForClientSecret"                 = azurerm_key_vault_secret.schedule_sync_client_secret_staging.name
    "BEC__keyVaultUrl"                                       = "https://${var.keyvault_name}.vault.azure.net"
    "BEC__webURL"                                            = "https://${var.bec_web_url}"
    "BECServiceBus__fullyQualifiedNamespace"                 = "${var.service_bus_namespace}.servicebus.windows.net"
    "DailySweepSchedule"                                     = "0 0 6 * * *"
    "NEW_RELIC_APP_NAME"                                     = "BEC-FUNCTION-services-STAGING"
    "NEW_RELIC_LICENSE_KEY"                                  = "6e841b6bef7d172d66182193aaafaccdFFFFNRAL"
    "AzureFunctionsJobHost__extensions__serviceBus__maxConcurrentSessions" = "100"
    "AzureFunctionsJobHost__extensions__serviceBus__sessionIdleTimeout"    = "00:00:05"
  }


  identity {
    type = "SystemAssigned"
  }
}



# Service Bus queue creation

resource "azurerm_servicebus_queue" "crreceiver_queue" {
  name                = "crreceiver"
  namespace_id        = azurerm_servicebus_namespace.service_bus.id
#  enable_partitioning = false
  requires_session    = true
}

resource "azurerm_servicebus_queue" "schedulesync_queue" {
  name                = "schedulesync"
  namespace_id        = azurerm_servicebus_namespace.service_bus.id
#  enable_partitioning = false
  requires_session    = true
}

#assign roles to API and services function app on service bus

resource "azurerm_role_assignment" "crreceiver_function_app_data_owner" {
  scope                = azurerm_servicebus_queue.crreceiver_queue.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = azurerm_linux_function_app.syncservice_function_app.identity[0].principal_id
}


resource "azurerm_role_assignment" "crreceiver_app_service_data_sender" {
  scope                = azurerm_servicebus_queue.crreceiver_queue.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
}

resource "azurerm_role_assignment" "schedulesync_function_app_data_owner" {
  scope                = azurerm_servicebus_queue.schedulesync_queue.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = azurerm_linux_function_app.syncservice_function_app.identity[0].principal_id
}


resource "azurerm_role_assignment" "schedulesync_app_service_data_sender" {
  scope                = azurerm_servicebus_queue.schedulesync_queue.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
}

# resource "azurerm_role_assignment" "syncservice_app_service_data_sender" {
#   scope                = azurerm_servicebus_namespace.service_bus.id
#   role_definition_name = "Azure Service Bus Data Sender"
#   principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
# }

# resource "azurerm_role_assignment" "syncservice_function_app_data_owner" {
#   scope                = azurerm_servicebus_namespace.service_bus.id
#   role_definition_name = "Azure Service Bus Data Owner"
#   principal_id         = azurerm_linux_function_app.syncservice_function_app.identity[0].principal_id
# }



### UserSync Appservice Creation ######


resource "azurerm_storage_account" "usersync_storage" {
  name                     = var.usersync_storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  account_kind             = "StorageV2"
}

resource "azurerm_linux_function_app" "usersync_function_app" {
  name                       = var.usersync_function_app_name
  location                   = var.resource_group_location
  resource_group_name        = var.resource_group_name
  service_plan_id            = azurerm_service_plan.appserviceplan.id
  storage_account_name       = azurerm_storage_account.usersync_storage.name
  storage_account_access_key = azurerm_storage_account.usersync_storage.primary_access_key
  https_only                 = true

  site_config {
    always_on = true
    http2_enabled = true  # Enable HTTP/2
      application_stack {
      docker {
        image_name = var.usersync_docker_image_name
        image_tag = var.usersync_docker_image_tag
        registry_url = var.docker_registry_url
        registry_username = var.docker_registry_user
        registry_password = var.docker_registry_password
      }
    }
  }

app_settings = {
  "WEBSITES_ENABLE_APP_SERVICE_STORAGE"        = "false"
  "DOCKER_REGISTRY_SERVER_URL"                 = var.docker_registry_url
  "DOCKER_REGISTRY_SERVER_USERNAME"            = var.docker_registry_user
  "DOCKER_REGISTRY_SERVER_PASSWORD"            = var.docker_registry_password
  "AzureWebJobsStorage__accountName"           = var.usersync_storage_account_name
  "BEC__apiAppId"                              = var.bec_api_app_id
  "BEC__apiAppIdURI"                           = var.bec_api_app_id_uri
  "BEC__apiURL"                                = "https://${var.custom_domain_api}/v1"
  "BEC__clientId"                              = var.bec_client_id_usersync
  "BEC__keyVaultSecretNameForCatalogConnectionString" = azurerm_key_vault_secret.CatalogConnectionStringStaging.name
  "BEC__keyVaultSecretNameForClientSecret"     = azurerm_key_vault_secret.user_sync_client_secret_staging.name
  "BEC__keyVaultUrl"                           = "https://${var.keyvault_name}.vault.azure.net"
  "BECServiceBus__fullyQualifiedNamespace"     = "${var.service_bus_namespace}.servicebus.windows.net"
  "NEW_RELIC_APP_NAME"                         = "BEC-FUNCTION-usersync-STAGING"
  "NEW_RELIC_LICENSE_KEY"                      = "6e841b6bef7d172d66182193aaafaccdFFFFNRAL"
  "WEBSITE_CONTENTOVERVNET"                    = "1"
}


  identity {
    type = "SystemAssigned"
  }
}

# Usersyncqueue creation

resource "azurerm_servicebus_queue" "usersync_queue" {
  name                = "usersync"
  namespace_id        = azurerm_servicebus_namespace.service_bus.id
#  enable_partitioning = false
  requires_session    = true
}

resource "azurerm_role_assignment" "usersync_function_app_data_owner" {
  scope                = azurerm_servicebus_queue.usersync_queue.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = azurerm_linux_function_app.usersync_function_app.identity[0].principal_id
}


resource "azurerm_role_assignment" "usersync_app_service_data_sender" {
  scope                = azurerm_servicebus_queue.usersync_queue.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
}


### Email Reminder Appservice Creation ######

resource "azurerm_storage_account" "emailreminder_storage" {
  name                     = var.emailreminder_storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  account_kind             = "StorageV2"
}

resource "azurerm_linux_function_app" "emailreminder_function_app" {
  name                       = var.emailreminder_function_app_name
  location                   = var.resource_group_location
  resource_group_name        = var.resource_group_name
  service_plan_id            = azurerm_service_plan.appserviceplan.id
  storage_account_name       = azurerm_storage_account.emailreminder_storage.name
  storage_account_access_key = azurerm_storage_account.emailreminder_storage.primary_access_key
  https_only                 = true

  site_config {
    always_on = true
    http2_enabled = true  # Enable HTTP/2
      application_stack {
      docker {
        image_name = var.emailreminder_docker_image_name
        image_tag = var.emailreminder_docker_image_tag
        registry_url = var.docker_registry_url
        registry_username = var.docker_registry_user
        registry_password = var.docker_registry_password
      }
    }
  }

app_settings = {
  "WEBSITES_ENABLE_APP_SERVICE_STORAGE"        = "false"
  "DOCKER_REGISTRY_SERVER_URL"                 = var.docker_registry_url
  "DOCKER_REGISTRY_SERVER_USERNAME"            = var.docker_registry_user
  "DOCKER_REGISTRY_SERVER_PASSWORD"            = var.docker_registry_password
  "AzureWebJobsStorage__accountName"           = var.emailreminder_storage_account_name
 # "BEC__apiAppId"                              = var.bec_api_app_id  # done
  "BEC__apiAppIdURI"                           = var.bec_api_app_id_uri #done
  "BEC__webURL"                                = "https://${var.bec_web_url}"
  "BEC__apiURL"                                = "https://${var.custom_domain_api}/v1" #done
  "BEC__clientId"                              = var.bec_client_id_emailreminder #done
  "BEC__keyVaultSecretNameForCatalogConnectionString" = azurerm_key_vault_secret.CatalogConnectionStringStaging.name #done
  "BEC__keyVaultSecretNameForClientSecret"     = azurerm_key_vault_secret.email_reminder_client_secret.name #done
  "BEC__keyVaultUrl"                           = "https://${var.keyvault_name}.vault.azure.net/" #done
  "BECServiceBus__fullyQualifiedNamespace"     = "${var.service_bus_namespace}.servicebus.windows.net"
  "NEW_RELIC_APP_NAME"                         = "BEC-FUNCTION-emailreminder-STAGING"
  "NEW_RELIC_LICENSE_KEY"                      = "6e841b6bef7d172d66182193aaafaccdFFFFNRAL"
  "WEBSITE_CONTENTOVERVNET"                    = "1"
}


  identity {
    type = "SystemAssigned"
  }
}

# Email Reminder queue creation

resource "azurerm_servicebus_queue" "emailreminder_queue" {
  name                = "emailreminder"
  namespace_id        = azurerm_servicebus_namespace.service_bus.id
#  enable_partitioning = false
  requires_session    = true
}

resource "azurerm_role_assignment" "emailreminder_function_app_data_owner" {
  scope                = azurerm_servicebus_queue.emailreminder_queue.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = azurerm_linux_function_app.emailreminder_function_app.identity[0].principal_id
}


resource "azurerm_role_assignment" "emailreminder_app_service_data_sender" {
  scope                = azurerm_servicebus_queue.emailreminder_queue.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
}



### Calendar Rules Appservice Creation ######

resource "azurerm_storage_account" "calendarrules_storage" {
  name                     = var.calendarrules_storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  account_kind             = "StorageV2"
}

resource "azurerm_linux_function_app" "calendarrules_function_app" {
  name                       = var.calendarrules_function_app_name
  location                   = var.resource_group_location
  resource_group_name        = var.resource_group_name
  service_plan_id            = azurerm_service_plan.appserviceplan.id
  storage_account_name       = azurerm_storage_account.calendarrules_storage.name
  storage_account_access_key = azurerm_storage_account.calendarrules_storage.primary_access_key
  https_only                 = true

  site_config {
    always_on = true
    http2_enabled = true  # Enable HTTP/2
      application_stack {
      docker {
        image_name = var.calendarrules_docker_image_name
        image_tag = var.calendarrules_docker_image_tag
        registry_url = var.docker_registry_url
        registry_username = var.docker_registry_user
        registry_password = var.docker_registry_password
      }
    }
  }

app_settings = {
  "WEBSITES_ENABLE_APP_SERVICE_STORAGE"        = "false"
  "DOCKER_REGISTRY_SERVER_URL"                 = var.docker_registry_url
  "DOCKER_REGISTRY_SERVER_USERNAME"            = var.docker_registry_user
  "DOCKER_REGISTRY_SERVER_PASSWORD"            = var.docker_registry_password
  "AzureWebJobsStorage__accountName"           = var.calendarrules_storage_account_name
  #"BEC__apiAppId"                              = var.bec_api_app_id
  "BEC__apiAppIdURI"                           = var.bec_api_app_id_uri 
  "BEC__calendarRulesURL"                      = var.calendarrules_url
  "BEC__webURL"                                = "https://${var.bec_web_url}"
  "BEC__apiURL"                                = "https://${var.custom_domain_api}/v1" 
  "BEC__clientId"                              = var.bec_client_id_calendarrules 
  "BEC__keyVaultSecretNameForCatalogConnectionString" = azurerm_key_vault_secret.CatalogConnectionStringStaging.name 
  "BEC__keyVaultSecretNameForClientSecret"     = azurerm_key_vault_secret.calendar_rules_client_secret_staging.name 
  "BEC__keyVaultUrl"                           = "https://${var.keyvault_name}.vault.azure.net/" 
  "BECServiceBus__fullyQualifiedNamespace"     = "${var.service_bus_namespace}.servicebus.windows.net"
  "NEW_RELIC_APP_NAME"                         = "BEC-FUNCTION-calendarrules-STAGING"
  "NEW_RELIC_LICENSE_KEY"                      = "6e841b6bef7d172d66182193aaafaccdFFFFNRAL"
  "WEBSITE_CONTENTOVERVNET"                    = "1"
}


  identity {
    type = "SystemAssigned"
  }
}

# calendar rules queue creation

resource "azurerm_servicebus_queue" "calendarrules_queue" {
  name                = "calendarrules"
  namespace_id        = azurerm_servicebus_namespace.service_bus.id
#  enable_partitioning = false
  requires_session    = true
}

resource "azurerm_role_assignment" "calendarrules_function_app_data_owner" {
  scope                = azurerm_servicebus_queue.calendarrules_queue.id
  role_definition_name = "Azure Service Bus Data Owner"
  principal_id         = azurerm_linux_function_app.calendarrules_function_app.identity[0].principal_id
}


resource "azurerm_role_assignment" "calendarrules_app_service_data_sender" {
  scope                = azurerm_servicebus_queue.calendarrules_queue.id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = azurerm_linux_web_app.backendappservice.identity[0].principal_id
}