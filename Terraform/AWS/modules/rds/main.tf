resource "aws_db_instance" "this" {
  identifier              = var.identifier
  db_name                 = var.db_name
  username                = var.username
  password                = var.password
  engine                  = var.engine
  engine_version          = var.engine_version
  instance_class          = var.instance_class
  allocated_storage       = var.allocated_storage
  port                    = var.port
  db_subnet_group_name    = aws_db_subnet_group.this.name
  vpc_security_group_ids  = [aws_security_group.this.id]
  publicly_accessible     = var.publicly_accessible
  multi_az                = var.multi_az
  backup_retention_period = var.backup_retention_period
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  tags = {
    Name        = var.identifier
    Environment = var.environment
    Project     = var.project
  }
}