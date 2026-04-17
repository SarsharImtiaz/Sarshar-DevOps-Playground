resource "aws_eip" "this" {
  count  = var.enable_nat_gateway ? 1 : 0
  domain = "vpc"

  tags = {
    Name        = "${var.name}-nat-eip"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_nat_gateway" "this" {
  count         = var.enable_nat_gateway ? 1 : 0
  allocation_id = aws_eip.this[0].id
  subnet_id     = var.public_subnet_id

  tags = {
    Name        = "${var.name}-nat"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_route" "private_default" {
  count                  = var.enable_nat_gateway ? length(var.private_route_table_ids) : 0
  route_table_id         = var.private_route_table_ids[count.index]
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[0].id
}