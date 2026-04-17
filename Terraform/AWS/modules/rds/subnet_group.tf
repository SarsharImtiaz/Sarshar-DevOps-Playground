resource "aws_db_subnet_group" "this" {
  name       = "${var.identifier}-subnet-group"
  subnet_ids = var.subnet_ids

  tags = {
    Name        = "${var.identifier}-subnet-group"
    Environment = var.environment
    Project     = var.project
  }
}