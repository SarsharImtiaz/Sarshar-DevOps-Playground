aws_region  = "us-east-1"
aws_profile = "default"
environment = "dev"
project     = "sample-app"

vpc_name = "sample-vpc"
vpc_cidr = "10.0.0.0/16"

beanstalk_subnets = [
  { name = "sample-subnet-1", cidr = "10.0.1.0/24", az = "us-east-1a" },
  { name = "sample-subnet-2", cidr = "10.0.2.0/24", az = "us-east-1b" },
  { name = "sample-subnet-3", cidr = "10.0.3.0/24", az = "us-east-1c" }
]

lambda_subnets = [
  { name = "lambda-subnet-1", cidr = "10.0.11.0/24", az = "us-east-1a" },
  { name = "lambda-subnet-2", cidr = "10.0.12.0/24", az = "us-east-1b" },
  { name = "lambda-subnet-3", cidr = "10.0.13.0/24", az = "us-east-1c" }
]

enable_nat_gateway = true

secrets = [
  {
    name        = "example-secret"
    description = "dummy secret"
  }
]

db_identifier              = "sample-db"
db_name                    = "sampledb"
db_username                = "admin"
db_password                = "changeme123"
db_engine                  = "postgres"
db_engine_version          = "15"
db_instance_class          = "db.t3.micro"
db_allocated_storage       = 20
db_port                    = 5432
db_publicly_accessible     = false
db_multi_az                = false
db_backup_retention_period = 7
db_allowed_cidr            = "10.0.0.0/16"

sqs_queues = [
  { name = "queue1", visibility_timeout = 300, dlq_name = "queue1-dlq", dlq_visibility_timeout = 60 },
  { name = "queue2", visibility_timeout = 300, dlq_name = "queue2-dlq", dlq_visibility_timeout = 60 }
]

lambda_functions = [
  {
    function_name = "SampleFunction"
    handler       = "app.handler"
    runtime       = "nodejs18.x"
    timeout       = 60
    memory_size   = 128
    s3_key        = "lambda.zip"
    environment_variables = {
      AppName        = "SampleFunction"
      AwsSecret      = "example-secret"
      LogLevel       = "Info"
      CommandTimeout = "60"
    }
    sqs_triggers = [{ queue_name = "queue1" }]
  }
]

elastic_beanstalk_apps = [
  {
    application_name    = "sample-app"
    environment_name    = "sample-env"
    solution_stack_name = "64bit Amazon Linux 2 v3.5.0 running Node.js"
    tier                = "WebServer"
    cname_prefix        = "sample-app"
    instance_type       = "t3.micro"
    min_size            = 1
    max_size            = 1
    env_vars            = {}
  }
]

lambda_artifacts_bucket_name = "sample-bucket-123"

waf_name = "sample-waf"