module "lambda_artifacts_bucket" {
  source = "../../modules/s3_bucket"

  bucket_name = var.lambda_artifacts_bucket_name
  environment = var.environment
  project     = var.project
}

resource "aws_s3_object" "lambda_placeholder_zip" {
  bucket = module.lambda_artifacts_bucket.bucket_name
  key    = "lambda-placeholder.zip"
  source = "../../artifacts/lambda-placeholder.zip"
  etag   = filemd5("../../artifacts/lambda-placeholder.zip")
}