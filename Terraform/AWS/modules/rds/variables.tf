variable "identifier" {
  type = string
}

variable "db_name" {
  type = string
}

variable "username" {
  type = string
}

variable "password" {
  type      = string
  sensitive = true
}

variable "engine" {
  type = string
}

variable "engine_version" {
  type = string
}

variable "instance_class" {
  type = string
}

variable "allocated_storage" {
  type = number
}

variable "port" {
  type = number
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "publicly_accessible" {
  type = bool
}

variable "multi_az" {
  type = bool
}

variable "backup_retention_period" {
  type = number
}

variable "allowed_cidr" {
  type = string
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}