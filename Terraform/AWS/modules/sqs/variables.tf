variable "queues" {
  type = list(object({
    name                   = string
    visibility_timeout     = number
    dlq_name               = string
    dlq_visibility_timeout = number
    max_receive_count      = optional(number, 5)
  }))
  default = []
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}