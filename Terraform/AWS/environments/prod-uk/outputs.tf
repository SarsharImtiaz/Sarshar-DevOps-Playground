output "vpc_id" {
  value = module.vpc.vpc_id
}

output "beanstalk_subnet_ids" {
  value = module.subnets.beanstalk_subnet_ids
}

output "lambda_subnet_ids" {
  value = module.subnets.lambda_subnet_ids
}

output "nat_gateway_id" {
  value = module.nat_gateway.nat_gateway_id
}

output "db_endpoint" {
  value = module.rds.db_endpoint
}

output "lambda_function_arns" {
  value = module.lambda.lambda_function_arns
}

output "secret_arns" {
  value = module.secrets_manager.secret_arns
}


output "elastic_beanstalk_environment_names" {
  value = module.elastic_beanstalk.environment_names
}

output "waf_web_acl_arn" {
  value = module.waf.web_acl_arn
}

output "waf_web_acl_id" {
  value = module.waf.web_acl_id
}

output "waf_web_acl_name" {
  value = module.waf.web_acl_name
}