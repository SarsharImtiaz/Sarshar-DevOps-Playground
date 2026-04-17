# Identity
resource "azurerm_user_assigned_identity" "identity" {
  name                = "identity"
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
}

# Virtual Network
resource "azurerm_virtual_network" "vnet" {
  name                = var.appgateway_vnet_name
  address_space       = ["10.0.0.0/16"]
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
}

# Subnet
resource "azurerm_subnet" "subnet" {
  name                 = var.appgateway_subnet_name
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

# # Public IP for Application Gateway
# resource "azurerm_public_ip" "public_ip" {
#   name                = var.appgateway_public_ip_name
#   location            = var.resource_group_location
#   resource_group_name = var.resource_group_name
#   allocation_method   = "Static"
#   sku                 = "Standard"
# }

# WAF Policy
resource "azurerm_web_application_firewall_policy" "waf_policy" {
  name                = var.waf_policy_name
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location

  policy_settings {
    enabled                     = true
    mode                        = "Prevention"
    request_body_check          = true
    file_upload_limit_in_mb     = 100
    max_request_body_size_in_kb = 128
  }
  custom_rules {
    name      = "BlockGeoLocation"
    priority  = 1
    rule_type = "MatchRule"

    match_conditions {
      match_variables {
        variable_name = "RemoteAddr"
      }

      operator = "GeoMatch"
      match_values = [
        "IR",  # Iran
        "IQ",  # Iraq
        "KP"   # North Korea
      ]
    }

    action = "Block"
  }

  managed_rules {
    managed_rule_set {
      type    = "OWASP"
      version = "3.2"
    }

    managed_rule_set {
      type    = "Microsoft_BotManagerRuleSet"
      version = "1.0"
    }  
  }
}

# Data source for existing Key Vault
data "azurerm_key_vault" "Terraform_keyvault_name" {
  name                = var.Terraform_keyvault_name
  resource_group_name = "terraform-rg-staging"
}

# Data source for the SSL certificate stored in Key Vault
data "azurerm_key_vault_certificate" "ssl_certificate" {
  name         = var.ssl_certificate_name  
  key_vault_id = data.azurerm_key_vault.Terraform_keyvault_name.id
}



resource "azurerm_key_vault_access_policy" "access_policy" {
  key_vault_id = data.azurerm_key_vault.Terraform_keyvault_name.id
  tenant_id    = var.tenant_id
  object_id    = azurerm_user_assigned_identity.identity.principal_id

  certificate_permissions = [
    "Get",
    "List",
    "Import",
  ]
  secret_permissions = [
    "Get",
    "List",
  ]

    key_permissions = [
    "Get",
    "List",
    "Create",
    "Delete",
    ]
}

# Application Gateway
resource "azurerm_application_gateway" "appgateway" {
  name                = var.appgateway_name
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
  
  sku {
    name     = "WAF_v2"
    tier     = "WAF_v2"
  }

  ssl_policy {
    policy_type = "Predefined"
    policy_name = "AppGwSslPolicy20220101"
  }

    autoscale_configuration {
    min_capacity = 2
    max_capacity = 10
  }

  identity {
    type                 = "UserAssigned"
    identity_ids         = [azurerm_user_assigned_identity.identity.id]  # Reference to the created identity
  }

  firewall_policy_id                = azurerm_web_application_firewall_policy.waf_policy.id
  force_firewall_policy_association = true

  gateway_ip_configuration {
    name      = var.gateway_ip_configuration_name
    subnet_id = azurerm_subnet.subnet.id
  }

  # frontend_ip_configuration {
  #   name                 = var.frontend_ip_configuration_name
  #   public_ip_address_id = azurerm_public_ip.public_ip.id
  # }

  frontend_ip_configuration {
    name                 = var.frontend_ip_configuration_name
    public_ip_address_id = var.appgateway_public_ip_id
  }
  frontend_port {
    name = var.frontend_port_name_app
    port = 443
  }

  backend_address_pool {
    name  = var.backend_address_pool_name_app
    fqdns = ["${var.frontend_app_service_name}.azurewebsites.net"]
  }

  backend_address_pool {
    name  = var.backend_address_pool_name_api
    fqdns = ["${var.backend_app_service_name}.azurewebsites.net"]
  }

  backend_http_settings {
    name                  = var.backend_http_settings_name
    cookie_based_affinity = "Disabled"
    port                  = 443
    protocol              = "Https"
    request_timeout       = 20
    probe_name            = var.prob_name  
    pick_host_name_from_backend_address = true
  }

  # Define the probe before using it
  probe {
    name                = var.prob_name
    protocol            = "Https"
    host                = var.api_host_name
    path                = "/swagger/index.html"
    interval            = 30
    timeout             = 30
    unhealthy_threshold = 3
    pick_host_name_from_backend_http_settings = false
  }

  # Listener for App
  http_listener {
    name                           = var.http_api_listener_name
    frontend_ip_configuration_name = var.frontend_ip_configuration_name 
    frontend_port_name             = var.frontend_port_name_app
    protocol                       = "Https"
    host_name                      = ""
    ssl_certificate_name           = var.ssl_certificate_name
  }

  # Listener for Api
  http_listener {
    name                           = var.http_app_listener_name
    frontend_ip_configuration_name = var.frontend_ip_configuration_name  
    frontend_port_name             = var.frontend_port_name_api
    protocol                       = "Https"
    host_name                      = var.app_host_name
    ssl_certificate_name           = var.ssl_certificate_name 
  }

  ssl_certificate {
    name                  = data.azurerm_key_vault_certificate.ssl_certificate.name
    key_vault_secret_id   = var.key_vault_secret_identifier
  }

  url_path_map {
    name                           = "appGatewayUrlPathMapApp"
    default_backend_address_pool_name = var.backend_address_pool_name_app 
    default_backend_http_settings_name = var.backend_http_settings_name  

    path_rule {
      name                       = "AppPathRuleApp"
      paths                      = ["/*"]
      backend_address_pool_name  = var.backend_address_pool_name_app
      backend_http_settings_name  = var.backend_http_settings_name  
    }

      path_rule {
      name                       = "ApiPathRuleAPi"
      paths                      = ["/v1/*"]
      backend_address_pool_name  = var.backend_address_pool_name_api
      backend_http_settings_name  = var.backend_http_settings_name
    }
    
  }

  url_path_map {
    name                           = "appGatewayUrlPathMapBase"
    default_backend_address_pool_name = var.backend_address_pool_name_api  
    default_backend_http_settings_name = var.backend_http_settings_name  

    path_rule {
      name                       = "AppPathRuleApp"
      paths                      = ["/*"]
      backend_address_pool_name  = var.backend_address_pool_name_app
      backend_http_settings_name  = var.backend_http_settings_name
    }

      path_rule {
      name                       = "ApiPathRuleApi"
      paths                      = ["/v1/*"]
      backend_address_pool_name  = var.backend_address_pool_name_api
      backend_http_settings_name  = var.backend_http_settings_name
    }
    
  }

  request_routing_rule {
    name                       = var.request_routing_rule_name_app
    rule_type                  = "PathBasedRouting"
    http_listener_name         = var.http_app_listener_name  
    url_path_map_name          = "appGatewayUrlPathMapApp"
    priority                   = 10
  }

  request_routing_rule {
    name                       = var.request_routing_rule_name_base
    rule_type                  = "PathBasedRouting"
    http_listener_name         = var.http_api_listener_name  
    url_path_map_name          = "appGatewayUrlPathMapBase"
    priority                   = 10010
  }

}