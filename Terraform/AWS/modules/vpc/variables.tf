variable "name" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "enable_dns_support" {
  type    = bool
  default = true
}

variable "enable_dns_hostnames" {
  type    = bool
  default = true
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}