module "vpc" {
  source = "../../modules/vpc"

  name                 = var.vpc_name
  vpc_cidr             = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  environment          = var.environment
  project              = var.project
}

module "subnets" {
  source = "../../modules/subnets"

  vpc_id              = module.vpc.vpc_id
  internet_gateway_id = module.vpc.internet_gateway_id

  beanstalk_subnets = var.beanstalk_subnets
  lambda_subnets    = var.lambda_subnets

  environment = var.environment
  project     = var.project
}

module "nat_gateway" {
  source = "../../modules/nat_gateway"

  name                    = var.vpc_name
  enable_nat_gateway      = var.enable_nat_gateway
  public_subnet_id        = module.subnets.beanstalk_subnet_ids[0]
  private_route_table_ids = [module.subnets.lambda_route_table_id]
  environment             = var.environment
  project                 = var.project
}