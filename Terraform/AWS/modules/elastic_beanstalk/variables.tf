variable "applications" {
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
    application_subnets = list(string)
    elb_subnets         = list(string)
    env_vars            = optional(map(string), {})
    vpc_id              = string
  }))
  default = []
}

variable "service_role" {
  type = string
}

variable "instance_profile_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}