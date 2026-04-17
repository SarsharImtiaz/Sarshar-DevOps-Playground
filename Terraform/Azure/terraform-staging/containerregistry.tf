# # Create an Azure Container Registry
# resource "azurerm_container_registry" "acr" {
#   name                = var.acr_name
#   resource_group_name = var.resource_group_name
#   location            = var.resource_group_location
#   sku                 = var.acr_sku
#   admin_enabled       = true
# }

# # Output the ACR login server nameterra
# output "acr_login_server" {
#   value = var.docker_registry_url
# }

# # Output the ACR admin username
# output "acr_admin_username" {
#   value = var.docker_registry_user
# }

# # Output the ACR admin password
# output "acr_admin_password" {
#   value = var.docker_registry_password
#   sensitive = true
# }