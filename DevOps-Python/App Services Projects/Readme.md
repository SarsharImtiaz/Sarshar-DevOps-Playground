This Project helps us to auto scale the SKU of the App Service Plan on Azure Services in case you want to automate it to save cost over the weekend or off-hours.

Use the following command for implementation:

          python change_sku.py \
            --subscription-id "$(AZURE_SUBSCRIPTION_ID)" \
            --resource-group "$(RESOURCE_GROUP)" \
            --plan-name "$(PLAN_NAME)" \
            --target-sku "$(TARGET_SKU)" \
            --capacity "$(TARGET_CAPACITY)" \
            --auto-adjust-capacity \
            --prefer-az-cli

To integrate it with Azure DevOps pipelines, refer to the following pipeline:

https://github.com/SarsharImtiaz/Sarshar-DevOps-Playground/blob/main/Pipelines/scale_appserviceplan.yml
