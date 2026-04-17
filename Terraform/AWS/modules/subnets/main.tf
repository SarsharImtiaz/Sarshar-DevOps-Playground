resource "aws_subnet" "beanstalk" {
  for_each = {
    for subnet in var.beanstalk_subnets : subnet.name => subnet
  }

  vpc_id                  = var.vpc_id
  cidr_block              = each.value.cidr
  availability_zone       = each.value.az
  map_public_ip_on_launch = true

  tags = {
    Name        = each.value.name
    Tier        = "beanstalk"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_subnet" "lambda" {
  for_each = {
    for subnet in var.lambda_subnets : subnet.name => subnet
  }

  vpc_id                  = var.vpc_id
  cidr_block              = each.value.cidr
  availability_zone       = each.value.az
  map_public_ip_on_launch = false

  tags = {
    Name        = each.value.name
    Tier        = "lambda"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_route_table" "beanstalk" {
  vpc_id = var.vpc_id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = var.internet_gateway_id
  }

  tags = {
    Name        = "Beanstalk-RouteTable"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_route_table_association" "beanstalk" {
  for_each = aws_subnet.beanstalk

  subnet_id      = each.value.id
  route_table_id = aws_route_table.beanstalk.id
}

resource "aws_route_table" "lambda" {
  vpc_id = var.vpc_id

  tags = {
    Name        = "Lambda-RouteTable"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_route_table_association" "lambda" {
  for_each = aws_subnet.lambda

  subnet_id      = each.value.id
  route_table_id = aws_route_table.lambda.id
}