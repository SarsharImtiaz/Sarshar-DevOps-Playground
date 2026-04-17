terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "3.111.0"
    }
  }
}

provider "azurerm" {
  features {}
 #skip_provider_registration = true
 subscription_id = "546aebf7-a4eb-49ae-b137-bb7be05bc21f"
}

locals {
  tags = {
    environment = "staging"
  }
}

data "azurerm_client_config" "current" {}