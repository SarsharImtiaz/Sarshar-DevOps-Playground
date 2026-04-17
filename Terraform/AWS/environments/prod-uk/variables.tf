variable "aws_region" {
  type = string
}

variable "aws_profile" {
  type = string
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "vpc_name" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "enable_nat_gateway" {
  type    = bool
  default = true
}


variable "db_identifier" {
  type = string
}

variable "db_name" {
  type = string
}

variable "db_username" {
  type = string
}

variable "db_password" {
  type      = string
  sensitive = true
}

variable "db_engine" {
  type = string
}

variable "db_engine_version" {
  type = string
}

variable "db_instance_class" {
  type = string
}

variable "db_allocated_storage" {
  type = number
}

variable "db_port" {
  type = number
}

variable "db_publicly_accessible" {
  type    = bool
  default = false
}

variable "db_multi_az" {
  type    = bool
  default = false
}

variable "db_backup_retention_period" {
  type    = number
  default = 7
}

variable "db_allowed_cidr" {
  type    = string
  default = "10.0.0.0/8"
}

variable "secrets" {
  type = list(object({
    name                    = string
    description             = optional(string)
    secret_string           = optional(string)
    recovery_window_in_days = optional(number, 7)
  }))
  default   = []
  sensitive = true
}

variable "shield_protections" {
  type = list(object({
    name         = string
    resource_arn = string
  }))
  default = []
}

variable "beanstalk_subnets" {
  type = list(object({
    name = string
    cidr = string
    az   = string
  }))
}

variable "lambda_subnets" {
  type = list(object({
    name = string
    cidr = string
    az   = string
  }))
}

variable "elastic_beanstalk_apps" {
  type = list(object({
    application_name    = string
    description         = optional(string)
    environment_name    = string
    solution_stack_name = string
    tier                = string
    cname_prefix        = optional(string)
    instance_type       = string
    min_size            = number
    max_size            = number
    application_subnets = optional(list(string), [])
    elb_subnets         = optional(list(string), [])
    env_vars            = optional(map(string), {})
  }))
  default = []
}

variable "sqs_queues" {
  type = list(object({
    name                   = string
    visibility_timeout     = number
    dlq_name               = string
    dlq_visibility_timeout = number
    max_receive_count      = optional(number, 5)
  }))
  default = []
}

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
      queue_name  = string
      batch_size  = optional(number, 10)
      enabled     = optional(bool, true)
    })), [])
  }))
  default = []
}

variable "lambda_artifacts_bucket_name" {
  type = string
}

variable "waf_name" {
  type = string
}

