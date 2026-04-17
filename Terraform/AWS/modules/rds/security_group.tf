resource "aws_security_group" "this" {
  name        = "${var.identifier}-sg"
  description = "RDS security group"
  vpc_id      = var.vpc_id

  tags = {
    Name        = "${var.identifier}-sg"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_vpc_security_group_ingress_rule" "db_ingress" {
  security_group_id = aws_security_group.this.id
  cidr_ipv4         = var.allowed_cidr
  from_port         = var.port
  ip_protocol       = "tcp"
  to_port           = var.port
}

resource "aws_vpc_security_group_egress_rule" "all_outbound" {
  security_group_id = aws_security_group.this.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}