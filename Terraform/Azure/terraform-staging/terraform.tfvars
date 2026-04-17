# Resource Group
resource_group_name     = "example-rg"
resource_group_location = "East US"

# Container Registry
acr_name = "exampleacr"
acr_sku  = "Basic"

# App Service
app_service_plan_name      = "example-plan"
app_service_plan_sku       = "B1"
os_type                    = "Linux"
frontend_app_service_name  = "frontend-app"
frontend_docker_image_name = "frontend:latest"
backend_app_service_name   = "backend-app"
backend_docker_image_name  = "backend:latest"
legalbar_addin_app_service_name = "addin-app"
legalbar_addin_docker_image_name = "addin:latest"
legalbar_backend_app_service_name = "addin-api"
legalbar_backend_docker_image_name = "addin-api:latest"

# Service Bus
service_bus_namespace   = "example-sb"
servicebus_account_tier = "Basic"

# Storage / Function Apps
storage_account_tier              = "Standard"
storage_account_replication_type  = "LRS"

audit_function_app_name           = "audit-func"
audit_docker_image_name           = "audit"
audit_docker_image_tag            = "latest"
audit_storage_account_name        = "auditstorage"

syncservice_storage_account_name  = "syncstorage"
syncservice_function_app_name     = "sync-func"
syncservice_docker_image_name     = "sync"
services_docker_image_tag         = "latest"

usersync_storage_account_name     = "usersyncstorage"
usersync_function_app_name        = "usersync-func"
usersync_docker_image_name        = "usersync"
usersync_docker_image_tag         = "latest"

calendarrules_storage_account_name = "calendarstorage"
calendarrules_function_app_name    = "calendar-func"
calendarrules_docker_image_name    = "calendar"
calendarrules_docker_image_tag     = "latest"

emailreminder_storage_account_name = "emailstorage"
emailreminder_function_app_name    = "email-func"
emailreminder_docker_image_name    = "email"
emailreminder_docker_image_tag     = "latest"

# SQL
sql_server_name       = "examplesql"
sql_admin_username    = "admin"

elasticpool_sku_name  = "BasicPool"
elasticpool_tier      = "Basic"
elasticpool_dtu       = "20"
elasticpool_name      = "examplepool"
tenant_catalog_database_name = "tenant-db"

# Key Vault
keyvault_name              = "examplekv"
keyvault_secret_name       = "example-secret"
user_haider_object_id      = "00000000-0000-0000-0000-000000000000"
Terraform_keyvault_name    = "tfkv-example"

# Dummy secrets
calendar_rules_client_secret_staging = "dummy-secret"
schedule_sync_client_secret_staging  = "dummy-secret"
user_sync_client_secret_staging      = "dummy-secret"
email_reminder_client_secret         = "dummy-secret"

bec_api_app_id_uri            = "api://example"
bec_client_id_schedulesync    = "00000000-0000-0000-0000-000000000000"
bec_api_app_id                = "00000000-0000-0000-0000-000000000000"
bec_client_id_usersync        = "00000000-0000-0000-0000-000000000000"
bec_client_id_emailreminder   = "00000000-0000-0000-0000-000000000000"
bec_client_id_calendarrules   = "00000000-0000-0000-0000-000000000000"

calendarrules_url        = "https://example.com"
bec_hosting_tenant_id    = "00000000-0000-0000-0000-000000000000"
bec_webapp_client_id     = "00000000-0000-0000-0000-000000000000"

# App Gateway
appgateway_vnet_name                  = "vnet-example"
appgateway_public_ip_name             = "publicip-example"
appgateway_subnet_name                = "subnet-example"
appgateway_name                       = "appgw-example"
gateway_ip_configuration_name         = "ipconfig"
frontend_ip_configuration_name        = "frontend-ip"
backend_http_settings_name            = "backend-settings"
backend_address_pool_name_app         = "pool-app"
backend_address_pool_name_api         = "pool-api"
fqdn_app                              = "app.azurewebsites.net"
fqdn_api                              = "api.azurewebsites.net"
custom_domain_app                     = "app.example.com"
custom_domain_api                     = "api.example.com"
prob_name                             = "probe"
frontend_port_name_app                = "port443"
frontend_port_name_api                = "port443"
http_api_listener_name                = "listener-api"
http_app_listener_name                = "listener-app"
request_routing_rule_name_app         = "rule-app"
request_routing_rule_name_base        = "rule-base"
waf_policy_name                       = "waf-example"
app_host_name                         = "app.example.com"
api_host_name                         = "api.example.com"
ssl_certificate_name                  = "example-cert"
tenant_id                             = "00000000-0000-0000-0000-000000000000"
key_vault_secret_identifier           = "https://example.vault.azure.net/secrets/sample"
appgateway_public_ip_id               = "/subscriptions/000/resourceGroups/example/providers/Microsoft.Network/publicIPAddresses/example"

# Container Registry creds
docker_registry_url      = "https://example.azurecr.io"
docker_registry_user     = "example"
docker_registry_password = "dummy-password"

# Cert
key_vault_id                = "https://example.vault.azure.net/"
certificate_id              = "https://example.vault.azure.net/secrets/sample"
Terraform_keyvault_name_ssl = "tfkv-example"

# iManage / Integrations
imanage_app_service_name = "integrator-app"
bec_web_url              = "example.com"
imanage_app_id           = "00000000-0000-0000-0000-000000000000"
imanage_app_secret       = "dummy-secret"
imanage_redirect_url     = "https://example.com/callback"
imanage_scope            = "user"
imanage_server           = "https://example.com"
imanage_docker_image_name = "integrator:latest"
imanage_connector_url     = "https://example.com"

netdocs_server         = "https://example.com"
netdocs_client_id      = "dummy"
netdocs_client_secret  = "dummy"
netdocs_redirect_url   = "https://example.com/callback"

# Outlook Addin
outlook_addin_app_service_name  = "outlook-app"
outlook_addin_docker_image_name = "outlook:latest"

# AI App
ai_app_service_plan_name = "ai-plan"
ai_app_service_name      = "ai-app"
ai_app_service_plan_sku  = "B1"

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=example;AccountKey=dummy;"
azure_openai_api_version = "2023-05-15"
azure_openai_endpoint    = "https://example.openai.azure.com/"
azure_openai_key         = "dummy-key"

gpt_input_rate  = "0.001"
gpt_model_name  = "gpt-4"
gpt_output_rate = "0.005"
gpt_token_limit = "10000"

http_port = "5000"
poppler_path = "/usr/bin"
websites_enable_app_service_storage = "false"