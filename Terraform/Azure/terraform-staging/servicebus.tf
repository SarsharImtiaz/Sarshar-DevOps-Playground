resource "azurerm_servicebus_namespace" "service_bus" {
  name                     = var.service_bus_namespace
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  sku                      = var.servicebus_account_tier 
}

