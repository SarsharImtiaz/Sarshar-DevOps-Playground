output "nat_gateway_id" {
  value = try(aws_nat_gateway.this[0].id, null)
}

output "eip_id" {
  value = try(aws_eip.this[0].id, null)
}