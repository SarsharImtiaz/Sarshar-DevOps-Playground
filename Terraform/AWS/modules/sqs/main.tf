locals {
  queues = {
    for q in var.queues : q.name => q
  }
}

resource "aws_sqs_queue" "dlq" {
  for_each = local.queues

  name                       = each.value.dlq_name
  visibility_timeout_seconds = each.value.dlq_visibility_timeout
  sqs_managed_sse_enabled    = true

  tags = {
    Name        = each.value.dlq_name
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_sqs_queue" "this" {
  for_each = local.queues

  name                       = each.value.name
  visibility_timeout_seconds = each.value.visibility_timeout
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = each.value.max_receive_count
  })

  tags = {
    Name        = each.value.name
    Environment = var.environment
    Project     = var.project
  }
}