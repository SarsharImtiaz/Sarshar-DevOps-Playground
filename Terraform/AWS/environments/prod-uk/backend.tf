# terraform {
#   backend "s3" {
#     bucket       = "REPLACE_ME_STAGING_TFSTATE_BUCKET"
#     key          = "staging/terraform.tfstate"
#     region       = "us-east-1"
#     encrypt      = true
#     use_lockfile = true
#   }
# }