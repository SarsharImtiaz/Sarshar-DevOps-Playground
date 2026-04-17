resource "aws_security_group" "lambda" {
  name        = "${var.project}-${var.environment}-lambda-sg"
  description = "Lambda security group"
  vpc_id      = module.vpc.vpc_id

  tags = {
    Name        = "${var.project}-${var.environment}-lambda-sg"
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_vpc_security_group_egress_rule" "lambda_all_outbound" {
  security_group_id = aws_security_group.lambda.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

module "sqs" {
  source = "../../modules/sqs"

  queues      = var.sqs_queues
  environment = var.environment
  project     = var.project
}

module "lambda" {
  source = "../../modules/lambda"

  lambda_functions = [
    for fn in var.lambda_functions : merge(fn, {
      subnet_ids         = module.subnets.lambda_subnet_ids
      security_group_ids = [aws_security_group.lambda.id]
      s3_bucket          = module.lambda_artifacts_bucket.bucket_name
    })
  ]

  sqs_queue_arns = module.sqs.queue_arns
  environment    = var.environment
  project        = var.project

  depends_on = [
    aws_s3_object.lambda_placeholder_zip
  ]
}

module "elastic_beanstalk" {
  source = "../../modules/elastic_beanstalk"

  applications = [
    for app in var.elastic_beanstalk_apps : merge(app, {
      vpc_id              = module.vpc.vpc_id
      application_subnets = module.subnets.beanstalk_subnet_ids
      elb_subnets         = module.subnets.beanstalk_subnet_ids
    })
  ]

  service_role          = "arn:aws:iam::615299747030:role/aws-elasticbeanstalk-ec2-role"
  instance_profile_name = "ec2-instance-profile"
  environment           = var.environment
  project               = var.project
}