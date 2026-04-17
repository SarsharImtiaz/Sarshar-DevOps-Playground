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

variable "environment" {
  type = string
}

variable "project" {
  type = string
}