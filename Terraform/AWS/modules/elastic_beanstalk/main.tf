resource "aws_elastic_beanstalk_application" "this" {
  for_each = { for app in var.applications : app.application_name => app }

  name        = each.value.application_name
  description = try(each.value.description, null)

  tags = {
    Name        = each.value.application_name
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_elastic_beanstalk_environment" "this" {
  for_each = { for app in var.applications : app.environment_name => app }

  name                = each.value.environment_name
  application         = aws_elastic_beanstalk_application.this[each.value.application_name].name
  solution_stack_name = each.value.solution_stack_name
  tier                = each.value.tier
  cname_prefix        = try(each.value.cname_prefix, null)

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "InstanceType"
    value     = each.value.instance_type
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "IamInstanceProfile"
    value     = var.instance_profile_name
  }

  setting {
    namespace = "aws:autoscaling:asg"
    name      = "MinSize"
    value     = each.value.min_size
  }

  setting {
    namespace = "aws:autoscaling:asg"
    name      = "MaxSize"
    value     = each.value.max_size
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "EnvironmentType"
    value     = "LoadBalanced"
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "ServiceRole"
    value     = var.service_role
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "LoadBalancerType"
    value     = "application"
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "VPCId"
    value     = each.value.vpc_id
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "AssociatePublicIpAddress"
    value     = "true"
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "Subnets"
    value     = join(",", each.value.application_subnets)
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "ELBSubnets"
    value     = join(",", each.value.elb_subnets)
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "ELBScheme"
    value     = "public"
  }

  dynamic "setting" {
    for_each = each.value.env_vars
    content {
      namespace = "aws:elasticbeanstalk:application:environment"
      name      = setting.key
      value     = setting.value
    }
  }

  tags = {
    Name        = each.value.environment_name
    Environment = var.environment
    Project     = var.project
  }
}