output "application_names" {
  value = { for k, v in aws_elastic_beanstalk_application.this : k => v.name }
}

output "environment_names" {
  value = { for k, v in aws_elastic_beanstalk_environment.this : k => v.name }
}