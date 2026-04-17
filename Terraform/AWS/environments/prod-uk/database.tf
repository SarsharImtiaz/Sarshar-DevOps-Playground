module "rds" {
  source = "../../modules/rds"

  identifier              = var.db_identifier
  db_name                 = var.db_name
  username                = var.db_username
  password                = var.db_password
  engine                  = var.db_engine
  engine_version          = var.db_engine_version
  instance_class          = var.db_instance_class
  allocated_storage       = var.db_allocated_storage
  port                    = var.db_port
  vpc_id                  = module.vpc.vpc_id
  subnet_ids              = module.subnets.lambda_subnet_ids
  publicly_accessible     = var.db_publicly_accessible
  multi_az                = var.db_multi_az
  backup_retention_period = var.db_backup_retention_period
  allowed_cidr            = var.db_allowed_cidr
  environment             = var.environment
  project                 = var.project
}