variable "resource_group_name" {
  description = "The name of the resource group"
  type        = string
}

variable "resource_group_location" {
  description = "The location of the resource group"
  type        = string
}

variable "acr_name" {
  description = "The name of the Azure Container Registry"
  type        = string
}

variable "acr_sku" {
  description = "The SKU of the Azure Container Registry"
  type        = string
  default     = "Standard"
}

variable "app_service_plan_name" {
  description = "The name of the App Service Plan"
  type        = string
}

variable "app_service_plan_sku" {
  description = "The SKU of the App Service Plan"
  type        = string
}

variable "os_type" {
  description = "The OS type of the App Service Plan"
  type        = string
  default     = "Linux"
}

variable "frontend_app_service_name" {
  description = "The name of the Frontend App Service"
  type        = string
}

variable "backend_app_service_name" {
  description = "The name of the Backend App Service"
  type        = string
}

variable "backend_docker_image_name" {
  description = "The name of the Backend App Docker Image"
  type        = string
  default     = "default-image-name:latest"
}

variable "frontend_docker_image_name" {
  description = "The name of the Frontend App Docker Image"
  type        = string
  default     = "default-image-name:latest"
}

variable "service_bus_namespace" {
  description = "The name of the Service Bus Namespace"
  type        = string
}

variable "emailreminder_function_app_name" {
  description = "The name of the Email Reminder Function"
  type        = string
}


variable "emailreminder_docker_image_name" {
  description = "The name of the Email Reminder Function Docker Image Tag"
  type        = string
}

variable "emailreminder_docker_image_tag" {
  description = "The name of the Email Reminder Function Docker Image Tag"
  type        = string
}

variable "emailreminder_storage_account_name" {
  description = "The name of the Email Reminder Storage"
  type        = string
}

variable "calendarrules_function_app_name" {
  description = "The name of the CalendarRules Function"
  type        = string
}


variable "calendarrules_docker_image_name" {
  description = "The name of the CalendarRules Function Docker Image Tag"
  type        = string
}

variable "calendarrules_docker_image_tag" {
  description = "The name of the CalendarRules  Function Docker Image Tag"
  type        = string
}

variable "calendarrules_storage_account_name" {
  description = "The name of the CalendarRules Storage"
  type        = string
}



####################Legal BAR#################################

variable "legalbar_addin_app_service_name" {
  description = "The name of the Legalbar Frontend App Service"
  type        = string
}

variable "legalbar_backend_app_service_name" {
  description = "The name of the Legalbar Backend App Service"
  type        = string
}

variable "legalbar_backend_docker_image_name" {
  description = "The name of the Legalbar Backend App Docker Image"
  type        = string
  default     = "default-image-name:latest"
}

variable "legalbar_addin_docker_image_name" {
  description = "The name of the Lgelabar Addin App Docker Image"
  type        = string
  default     = "default-image-name:latest"
}

################################################################

variable "servicebus_account_tier" {
  description = "The Servicebus Account Tier e.g Standard"
  type        = string
}

variable "syncservice_bec_clientid" {
  description = "The Servicebus Client ID"
  type        = string
  sensitive = true
  default = ""
}

variable "syncservice_bec_clientsecret" {
  description = "The Servicebus Client ID"
  type        = string
  sensitive = true
}

variable "audit_function_app_name" {
  description = "The name of the Audit Function"
  type        = string
}


variable "audit_docker_image_name" {
  description = "The name of the Audit Function Docker Image Tag"
  type        = string
}

variable "audit_docker_image_tag" {
  description = "The name of the Audit Function Docker Image Tag"
  type        = string
}

variable "audit_storage_account_name" {
  description = "The name of the Audit Storage"
  type        = string
}

variable "storage_account_replication_type" {
  description = "Storage Account Replication Type"
  type        = string
}

variable "storage_account_tier" {
  description = "Storage Account Replication Type"
  type        = string
}

variable "syncservice_storage_account_name" {
  description = "The name of the SyncService Storage"
  type        = string
}

variable "syncservice_function_app_name" {
  description = "The name of SyncService Function App"
  type        = string
}

variable "syncservice_docker_image_name" {
  description = "The name of the SyncService Function Docker Image Tag"
  type        = string
}


variable "services_docker_image_tag" {
  description = "The name of the SyncService Function Docker Image Tag"
  type        = string
}

variable "usersync_storage_account_name" {
  description = "The name of the UserSync Storage"
  type        = string
}

variable "usersync_function_app_name" {
  description = "The name of usersync Function App"
  type        = string
}

variable "usersync_docker_image_name" {
  description = "The name of the SyncService Function Docker Image Tag"
  type        = string
}


variable "usersync_docker_image_tag" {
  description = "The name of the SyncService Function Docker Image Tag"
  type        = string
}


variable "sql_server_name" {
  description = "The name of the SQL Server"
  type        = string
}

variable "elasticpool_name" {
  description = "The name of the Elastic Pool"
  type        = string
}

variable "elasticpool_sku_name" {
  description = "Elastic Pool SKU name"
  type        = string
}

variable "elasticpool_tier" {
  description = "Elastic Pool Tier"
  type        = string
}

variable "elasticpool_dtu" {
  description = "Elastic Pool DTUs"
  type        = string
}

variable "sql_admin_username" {
  description = "The user name of SQL Server"
  type        = string
}

# variable "sql_admin_password" {
#   description = "The password of the SQL Server"
#   type        = string
# }

variable "tenant_catalog_database_name" {
  description = "Tenant Catalog Database Name"
  type        = string
}

variable "keyvault_name" {
  description = "The Name of a Key Vault"
  type        = string
}

variable "keyvault_secret_name" {
  description = "Keyvault Secret Name"
  type        = string
}

variable "Terraform_keyvault_name" {
  description = "The Name of a Terraform Key Vault"
  type        = string
}

variable "Terraform_keyvault_name_ssl" {
  description = "The Name of a Terraform Key Vault"
  type        = string
}


variable "user_haider_object_id" {
  description = "Haider User Object ID"
  type        = string
}

# App Gateway

variable "appgateway_vnet_name" {
  description = "The name of the application gateway vnet"
  type        = string
}

variable "appgateway_public_ip_name" {
  description = "The name of the public IP address"
  type        = string
}

variable "appgateway_subnet_name" {
  description = "The name of the virtual network"
  type        = string
}

variable "appgateway_name" {
  description = "The name of the subnet"
  type        = string
}

variable "gateway_ip_configuration_name" {
  description = "Name of the WAF policy"
  type        = string

}

variable "frontend_ip_configuration_name" {
  description = "The name of the virtual network"
  type        = string
}

variable "backend_http_settings_name" {
  description = "The name of the subnet"
  type        = string
}

variable "backend_address_pool_name_app" {
  description = "Name of the app backend address pool"
  type        = string

}

variable "backend_address_pool_name_api" {
  description = "Name of the api backend address pool"
  type        = string

}

variable "fqdn_app" {
  description = "Name of the fqdn app name"
  type        = string

}

variable "fqdn_api" {
  description = "Name of the fqdn api name"
  type        = string

}

variable "prob_name" {
  description = "Name of the prob name"
  type        = string

}

variable "frontend_port_name_app" {
  description = "The name of the frontend app port"
  type        = string
}

variable "frontend_port_name_api" {
  description = "The name of the frontend api port"
  type        = string
}

variable "http_api_listener_name" {
  description = "The name of the http api listener"
  type        = string
}

variable "http_app_listener_name" {
  description = "The name of the http app listener"
  type        = string
}

variable "request_routing_rule_name_app" {
  description = "The name of the request routing rule name for app"
  type        = string
}

variable "request_routing_rule_name_base" {
  description = "The name of the routing rule base"
  type        = string
}

variable "waf_policy_name" {
  description = "WAF Policy name"
  type        = string
}

variable "api_host_name" {
  description = "The name of the api host"
  type        = string
}

variable "app_host_name" {
  description = "The name of the app host"
  type        = string
}

variable "ssl_certificate_name" {
  description = "The name of the SSL certificate"
  type        = string
}

variable "tenant_id" {
  description = "Tenant ID"
  type        = string
}

variable "key_vault_secret_identifier" {
  description = "value of key vault secret identifier"
  type        = string
}

# Updated values for keyvault secrets

variable "calendar_rules_client_secret_staging" {
  description = "Client Secret for Calendar Rules Staging"
  type        = string
}

# variable "catalog_connection_string_staging" {
#   description = "Connection String for Catalog Staging"
#   type        = string
# }

variable "schedule_sync_client_secret_staging" {
  description = "Client Secret for Schedule Sync Staging"
  type        = string
}

variable "user_sync_client_secret_staging" {
  description = "Client Secret for User Sync Staging"
  type        = string
}

variable "email_reminder_client_secret" {
  description = "Client Secret for Email Reminder"
  type        = string
}

variable "bec_api_app_id_uri" {
  description = "Client ID of BEC API APP Registration"
  type        = string
}

variable "bec_client_id_schedulesync" {
  description = "Client ID of BEC Web App"
  type        = string
}

variable "bec_api_app_id" {
  description = "Client ID of BEC Web App"
  type        = string
}

variable "bec_client_id_usersync" {
  description = "Client ID of User Sync"
  type        = string
}

variable "bec_client_id_emailreminder" {
  description = "Client ID of Email Reminder"
  type        = string
}

variable "bec_client_id_calendarrules" {
  description = "Client ID of User Sync"
  type        = string
}

variable "calendarrules_url" {
  description = "Calendar Rules URL"
  type        = string
}

variable "bec_hosting_tenant_id" {
  description = "App Registration Hosting Tenant ID"
  type        = string
}

variable "bec_webapp_client_id" {
  description = "BEC web App Client ID"
  type        = string
}

variable "custom_domain_app" {
  description = "BEC web App Client ID"
  type        = string
}

variable "custom_domain_api" {
  description = "BEC web App Client ID"
  type        = string
}

variable "appgateway_public_ip_id" {
  description = "Existing Public IP for AppGateway"
  type        = string
}

variable "docker_registry_url" {
  description = "Existing URL"
  type        = string
}

variable "docker_registry_user" {
  description = "Existing Username"
  type        = string
}

variable "docker_registry_password" {
  description = "Existing Password"
  type        = string
}

variable "key_vault_id" {
  description = "Existing Password"
  type        = string
}

variable "certificate_id" {
  description = "Existing Password"
  type        = string
}

variable "imanage_app_service_name" {
  description = "iManage App Service Name"
  type        = string
}


variable "bec_web_url" {
  description = "iManage App WEB URL Name"
  type        = string
}

variable "imanage_app_id" {
  description = "iManage App ID Name"
  type        = string
}

variable "imanage_app_secret" {
  description = "iManage App Secret Name"
  type        = string
}

variable "imanage_redirect_url" {
  description = "iManage App Redirect URL Name"
  type        = string
}

variable "imanage_scope" {
  description = "iManage App Scope Name"
  type        = string
}

variable "imanage_server" {
  description = "iManage App Server Name"
  type        = string
}

variable "imanage_docker_image_name" {
  description = "iManage App Image Name"
  type        = string
}

variable "imanage_connector_url" {
  description = "iManage App Connector URL"
  type        = string
}
variable netdocs_server {
  description = "NetDocs App Server Name"
  type        = string
}

variable netdocs_client_id {
  description = "NetDocs App Id Name"
  type        = string
}

variable netdocs_client_secret {
  description = "NetDocs App Id Secret"
  type        = string
}

variable netdocs_redirect_url {
  description = "NetDocs App Redirect Url"
  type        = string
}

variable "outlook_addin_app_service_name" {
  type        = string
  description = "Name of the Outlook Add-in Linux App Service"
}

variable "outlook_addin_docker_image_name" {
  type        = string
  description = "Docker image (repo:tag) for the Outlook Add-in app"
}


# Schedule Assist AI variables #

variable "ai_app_service_plan_name" {
  type        = string
  description = "Docker image (repo:tag) for the Outlook Add-in app"
}

variable "ai_app_service_name" {
  type        = string
  description = "Docker image (repo:tag) for the Outlook Add-in app"
}

variable "ai_app_service_plan_sku" {
  type        = string
  description = "Docker image (repo:tag) for the Outlook Add-in app"
}

variable "azure_connection_string" {
  type        = string
  description = "Azure Storage connection string used by the app."
}

variable "azure_openai_api_version" {
  type        = string
  description = "Azure OpenAI API version."
}

variable "azure_openai_endpoint" {
  type        = string
  description = "Azure OpenAI endpoint URL."
}

variable "azure_openai_key" {
  type        = string
  description = "Azure OpenAI API key."
}

variable "gpt_input_rate" {
  type        = string
  description = "Input token rate/cost for the configured GPT model."
}

variable "gpt_model_name" {
  type        = string
  description = "Name of the GPT model deployment to use."
}

variable "gpt_output_rate" {
  type        = string
  description = "Output token rate/cost for the configured GPT model."
}

variable "gpt_token_limit" {
  type        = string
  description = "Maximum token limit for GPT requests."
}

variable "http_port" {
  type        = string
  description = "HTTP port the application listens on."
}

# variable "new_relic_app_name" {
#   type        = string
#   description = "New Relic application name."
# }

# variable "new_relic_license_key" {
#   type        = string
#   description = "New Relic license key."
# }

variable "poppler_path" {
  type        = string
  description = "Filesystem path to the Poppler binaries (used for PDF processing)."
}

# variable "website_httplogging_retention_days" {
#   type        = string
#   description = "Number of days to retain App Service HTTP logs."
# }

variable "websites_enable_app_service_storage" {
  type        = string
  description = "Whether to enable App Service storage (true/false)."
}
