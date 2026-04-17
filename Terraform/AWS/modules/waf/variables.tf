variable "name" {
  type = string
}

variable "scope" {
  type    = string
  default = "REGIONAL"
}

variable "metric_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}