output "beanstalk_subnet_ids" {
  value = values(aws_subnet.beanstalk)[*].id
}

output "lambda_subnet_ids" {
  value = values(aws_subnet.lambda)[*].id
}

output "beanstalk_route_table_id" {
  value = aws_route_table.beanstalk.id
}

output "lambda_route_table_id" {
  value = aws_route_table.lambda.id
}