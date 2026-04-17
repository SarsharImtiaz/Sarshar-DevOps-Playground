terraform {
  backend "azurerm" {
    resource_group_name   = "terraform-rg-staging"
    storage_account_name  = "terraformstrgstaging"
    container_name        = "tfstate"
    key                   = "terraform.tfstate"
  }
}