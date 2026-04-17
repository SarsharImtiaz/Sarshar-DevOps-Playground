module "waf" {
  source = "../../modules/waf"

  name        = var.waf_name
  metric_name = "${var.project}-${var.environment}-waf"
  scope       = "REGIONAL"
  environment = var.environment
  project     = var.project
}