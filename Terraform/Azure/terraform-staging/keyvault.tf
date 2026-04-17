# Create a KeyVault
resource "azurerm_key_vault" "keyvault" {
  name                        = var.keyvault_name
  location                    = var.resource_group_location
  resource_group_name         = var.resource_group_name
  enabled_for_disk_encryption  = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days   = 7
  purge_protection_enabled     = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_linux_web_app.backendappservice.identity[0].principal_id

    key_permissions = [
      "Get",
      "List",
    ]

    secret_permissions = [
      "Get",
      "List",
    ]

    storage_permissions = [
      "Get",
      "List",
    ]
  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = var.user_haider_object_id  # Haider

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete",
      "Recover",
      "Backup",
      "Restore",
      "Purge",
    ]

    storage_permissions = [
      "Get",
      "List",
    ]

    key_permissions = [
      "Get",
      "List",
    ]
  }

  access_policy {
    tenant_id  = data.azurerm_client_config.current.tenant_id
    object_id  = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete",
      "Purge",
    ]

    storage_permissions = [
      "Get",
      "List",
    ]

    key_permissions = [
      "Get",
      "List",
    ]
  }
}

# Store the dynamically generated SQL password in Key Vault
resource "azurerm_key_vault_secret" "sql_admin_password_secret" {
  name         = "sql-admin-password"
  value        = random_password.sql_admin_password.result  # Use the generated password
  key_vault_id = azurerm_key_vault.keyvault.id
}

# Store the connection string in the Key Vault (using the generated password)
resource "azurerm_key_vault_secret" "CatalogConnectionStringStaging" {
  name         = var.keyvault_secret_name
  value        = "Server=${azurerm_mssql_server.azuresql.name}.database.windows.net;Database=${var.tenant_catalog_database_name};User ID=${azurerm_mssql_server.azuresql.administrator_login};Password=${random_password.sql_admin_password.result}"  # Use generated password here
  key_vault_id = azurerm_key_vault.keyvault.id

  depends_on = [
    azurerm_key_vault.keyvault,
    azurerm_mssql_server.azuresql
  ]
}

resource "azurerm_key_vault_secret" "calendar_rules_client_secret_staging" {
  name         = "CalendarRulesClientSecretStaging"
  value        = var.calendar_rules_client_secret_staging
  key_vault_id = azurerm_key_vault.keyvault.id
}

# resource "azurerm_key_vault_secret" "catalog_connection_string_staging" {
#   name         = "CatalogConnectionStringStaging"
#   value        = var.catalog_connection_string_staging
#   key_vault_id = azurerm_key_vault.keyvault.id
# }

resource "azurerm_key_vault_secret" "schedule_sync_client_secret_staging" {
  name         = "ScheduleSyncClienSecretStaging"
  value        = var.schedule_sync_client_secret_staging
  key_vault_id = azurerm_key_vault.keyvault.id
}

resource "azurerm_key_vault_secret" "user_sync_client_secret_staging" {
  name         = "UserSyncClienSecretStaging"
  value        = var.user_sync_client_secret_staging
  key_vault_id = azurerm_key_vault.keyvault.id
}

resource "azurerm_key_vault_secret" "email_reminder_client_secret" {
  name         = "EmailReminderClientSecret"
  value        = var.email_reminder_client_secret
  key_vault_id = azurerm_key_vault.keyvault.id
}

## Using Separate Access Policy To Not Cause A Cycle

resource "azurerm_key_vault_access_policy" "audit_function_app_access" {
  key_vault_id = azurerm_key_vault.keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_linux_function_app.audit_function_app.identity[0].principal_id

  secret_permissions = [
    "Get",
    "List",
  ]

}

resource "azurerm_key_vault_access_policy" "calendarrules_function_app_access" {
  key_vault_id = azurerm_key_vault.keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_linux_function_app.calendarrules_function_app.identity[0].principal_id

  secret_permissions = [
    "Get",
    "List",
  ]

}

resource "azurerm_key_vault_access_policy" "emailreminder_function_app_access" {
  key_vault_id = azurerm_key_vault.keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_linux_function_app.emailreminder_function_app.identity[0].principal_id

  secret_permissions = [
    "Get",
    "List",
  ]

}

  resource "azurerm_key_vault_access_policy" "usersync_function_app_access" {
  key_vault_id = azurerm_key_vault.keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_linux_function_app.usersync_function_app.identity[0].principal_id

  secret_permissions = [
    "Get",
    "List",
  ]
  }

    resource "azurerm_key_vault_access_policy" "syncservice_function_app_access" {
  key_vault_id = azurerm_key_vault.keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_linux_function_app.syncservice_function_app.identity[0].principal_id

  secret_permissions = [
    "Get",
    "List",
  ]
  }