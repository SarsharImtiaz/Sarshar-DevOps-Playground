variable "bucket_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "project" {
  type = string
}

variable "versioning_enabled" {
  type    = bool
  default = true
}

variable "sse_algorithm" {
  type    = string
  default = "AES256"
}

variable "block_public_acls" {
  type    = bool
  default = true
}

variable "block_public_policy" {
  type    = bool
  default = true
}

variable "ignore_public_acls" {
  type    = bool
  default = true
}

variable "restrict_public_buckets" {
  type    = bool
  default = true
}