variable "vpc_id" {
  type = string
}

variable "internet_gateway_id" {
  type = string
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

variable "environment" {
  type = string
}

variable "project" {
  type = string
}