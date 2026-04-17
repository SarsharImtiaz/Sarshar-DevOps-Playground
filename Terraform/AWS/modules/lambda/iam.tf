data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each = {
    for fn in var.lambda_functions : fn.function_name => fn
  }

  name               = "${var.project}-${var.environment}-${each.key}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  tags = {
    Name        = "${var.project}-${var.environment}-${each.key}-role"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  for_each = {
    for fn in var.lambda_functions : fn.function_name => fn
  }

  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "vpc_access" {
  for_each = {
    for fn in var.lambda_functions : fn.function_name => fn
  }

  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "sqs_execution" {
  for_each = {
    for fn in var.lambda_functions : fn.function_name => fn
    if length(try(fn.sqs_triggers, [])) > 0
  }

  role       = aws_iam_role.lambda[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
}

resource "aws_iam_role_policy" "secrets_access" {
  for_each = {
    for fn in var.lambda_functions : fn.function_name => fn
    if length(try(fn.secret_arns, [])) > 0
  }

  name = "${each.key}-secrets-access"
  role = aws_iam_role.lambda[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = each.value.secret_arns
      }
    ]
  })
}