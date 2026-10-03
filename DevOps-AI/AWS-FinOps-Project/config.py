import os


# ---------------------------------------------------------
# AWS CONFIGURATION
# ---------------------------------------------------------

# Bedrock is working for you in us-west-1.
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "us-west-1")

# This is the inference profile that we already tested.
BEDROCK_MODEL_ID = os.getenv(
    "BEDROCK_MODEL_ID",
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
)

# Optional.
# Leave empty to use the credentials already configured
# through AWS CLI.
AWS_PROFILE = os.getenv("AWS_PROFILE")

# Number of historical days to inspect.
ANALYSIS_DAYS = int(
    os.getenv("ANALYSIS_DAYS", "30")
)

# Don't waste time investigating tiny service costs.
MIN_SERVICE_COST = float(
    os.getenv("MIN_SERVICE_COST", "5")
)