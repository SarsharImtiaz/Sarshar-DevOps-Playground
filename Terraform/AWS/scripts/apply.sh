#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-staging}"
cd "environments/${ENVIRONMENT}"
terraform init
terraform apply