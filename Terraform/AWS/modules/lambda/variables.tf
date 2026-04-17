variable "lambda_functions" {
  type = list(object({
    function_name         = string
    handler               = string
    runtime               = string
    timeout               = number
    memory_size           = number

    filename              = optional(string)
    s3_bucket             = optional(string)
    s3_key                = optional(string)
    s3_object_version     = optional(string)

    subnet_ids            = optional(list(string), [])
    security_group_ids    = optional(list(string), [])
    secret_arns           = optional(list(string), [])
    environment_variables = optional(map(string), {})

    sqs_triggers = optional(list(object({
      queue_name = string
      batch_size = optional(number, 10)
      enabled    = optional(bool, true)
    })), [])
  }))
  default = []
}

variable "sqs_queue_arns" {
  type    = map(string)
  default = {}
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}