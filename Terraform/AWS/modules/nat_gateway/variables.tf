variable "name" {
  type = string
}

variable "enable_nat_gateway" {
  type = bool
}

variable "public_subnet_id" {
  type = string
}

variable "private_route_table_ids" {
  type = list(string)
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}