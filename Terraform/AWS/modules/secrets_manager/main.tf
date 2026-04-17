locals {
  secrets_by_name = nonsensitive({
    for s in var.secrets : s.name => s
  })
}

resource "aws_secretsmanager_secret" "this" {
  for_each = local.secrets_by_name

  name                    = each.value.name
  description             = try(each.value.description, null)
  recovery_window_in_days = each.value.recovery_window_in_days

  tags = {
    Name        = each.value.name
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_secretsmanager_secret_version" "this" {
  for_each = {
    for k, s in local.secrets_by_name : k => s
    if try(s.secret_string, null) != null
  }

  secret_id     = aws_secretsmanager_secret.this[each.key].id
  secret_string = each.value.secret_string
}