locals {
  sqs_mapping_list = flatten([
    for fn in var.lambda_functions : [
      for idx, trigger in try(fn.sqs_triggers, []) : {
        key           = "${fn.function_name}-${idx}"
        function_name = fn.function_name
        queue_name    = trigger.queue_name
        batch_size    = try(trigger.batch_size, 10)
        enabled       = try(trigger.enabled, true)
      }
    ]
  ])

  sqs_mappings = {
    for m in local.sqs_mapping_list : m.key => m
  }
}

resource "aws_lambda_function" "this" {
  for_each = {
    for fn in var.lambda_functions : fn.function_name => fn
  }

  function_name = each.value.function_name
  role          = aws_iam_role.lambda[each.key].arn
  handler       = each.value.handler
  runtime       = each.value.runtime
  timeout       = each.value.timeout
  memory_size   = each.value.memory_size

  filename          = try(each.value.filename, null)
  s3_bucket         = try(each.value.s3_bucket, null)
  s3_key            = try(each.value.s3_key, null)
  s3_object_version = try(each.value.s3_object_version, null)

  source_code_hash = try(each.value.filename, null) != null ? filebase64sha256(each.value.filename) : null

  vpc_config {
    subnet_ids         = each.value.subnet_ids
    security_group_ids = each.value.security_group_ids
  }

  environment {
    variables = try(each.value.environment_variables, {})
  }

  tags = {
    Name        = each.value.function_name
    Environment = var.environment
    Project     = var.project
  }

  depends_on = [
  aws_iam_role_policy_attachment.basic_execution,
  aws_iam_role_policy_attachment.vpc_access,
  aws_iam_role_policy_attachment.sqs_execution
]

}

resource "aws_lambda_event_source_mapping" "sqs" {
  for_each = local.sqs_mappings

  event_source_arn = var.sqs_queue_arns[each.value.queue_name]
  function_name    = aws_lambda_function.this[each.value.function_name].arn
  batch_size       = each.value.batch_size
  enabled          = each.value.enabled
}