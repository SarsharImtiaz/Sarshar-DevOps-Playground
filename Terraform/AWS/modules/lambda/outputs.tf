output "lambda_function_arns" {
  value = {
    for k, v in aws_lambda_function.this : k => v.arn
  }
}

output "lambda_role_arns" {
  value = {
    for k, v in aws_iam_role.lambda : k => v.arn
  }
}

output "sqs_event_source_mapping_ids" {
  value = {
    for k, v in aws_lambda_event_source_mapping.sqs : k => v.id
  }
}