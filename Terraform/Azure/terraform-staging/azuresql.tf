# Generate a random password for SQL Admin
resource "random_password" "sql_admin_password" {
  length  = 16
  special = true
}

output "sql_admin_password" {
  value = random_password.sql_admin_password.result
  sensitive = true
}


# Create an Azure SQL Server
resource "azurerm_mssql_server" "azuresql" {
  name                         = var.sql_server_name
  resource_group_name          = var.resource_group_name
  location                     = var.resource_group_location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = random_password.sql_admin_password.result  # Use generated password
}

resource "azurerm_mssql_elasticpool" "elasticpool" {
  name                     = var.elasticpool_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  server_name              = azurerm_mssql_server.azuresql.name
  license_type             = "LicenseIncluded"
  max_size_gb              = 50

  sku {
    name     = var.elasticpool_sku_name
    tier     = var.elasticpool_tier
    capacity = var.elasticpool_dtu
  }

  per_database_settings {
    min_capacity = 0
    max_capacity = 10
  }
}

#resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
#  name                = "AllowAzureServices"
  # resource_group_name = var.resource_group_name
  # server_name         = azurerm_mssql_server.azuresql.name
 # server_id            = azurerm_mssql_server.azuresql.id
 # start_ip_address    = "0.0.0.0"
 # end_ip_address      = "0.0.0.0"
#}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.azuresql.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}
