# AWS FinOps AI Agent

An **agentic AWS cost optimization assistant** built with Python, Amazon
Bedrock, Claude Sonnet, boto3, and Streamlit.

The application analyzes real AWS cost and infrastructure data,
dynamically investigates cost drivers, correlates resource configuration
with CloudWatch utilization, and produces evidence-based FinOps
recommendations.


## Overview

Unlike a traditional script with a hardcoded investigation sequence,
this project exposes a set of read-only AWS tools to Claude through
**Amazon Bedrock tool use**. The model decides which tools it needs
based on the user's question and the results returned by previous tool
calls.

For example, when asked to investigate EC2 spending, the agent can:

1.  Query AWS Cost Explorer to identify EC2 spend.
2.  Determine which AWS regions are responsible for that spend.
3.  Discover running EC2 instances in those regions.
4.  Retrieve historical CPU utilization from CloudWatch.
5.  Inspect unattached EBS volumes for potential waste.
6.  Correlate the collected evidence.
7.  Generate prioritized rightsizing, scheduling, and optimization
    recommendations.

The model does **not** receive AWS credentials and does not directly
access AWS resources. Python executes only the explicitly registered
boto3 tools.

## Architecture

``` text
                         User
                          |
                          v
                    Streamlit UI
                          |
                          v
                   Python Agent Loop
                          |
                          v
                  Amazon Bedrock
                          |
                          v
                  Claude Sonnet 4.5
                    Reasoning Engine
                          |
                     Tool Requests
                          |
              +-----------+-----------+
              |           |           |
              v           v           v
        Cost Explorer    EC2/RDS   CloudWatch
              |           |           |
              +-----------+-----------+
                          |
                        boto3
                          |
                          v
                    AWS Account
                          |
                          v
                 Tool Results / Evidence
                          |
                          v
                  Claude FinOps Analysis
                          |
                          v
               Optimization Recommendations
```

## Current Capabilities

### Account-wide cost visibility

The agent can use AWS Cost Explorer to:

-   Analyze AWS spending by service.
-   Identify major cost drivers.
-   Break service costs down by AWS region.
-   Analyze daily cost trends.
-   Preserve credits, refunds, taxes, and non-regional billing entries
    without incorrectly treating them as infrastructure resources.

### EC2 optimization

The agent can:

-   Discover running EC2 instances by region.
-   Inspect instance type, tags, platform, availability zone, and launch
    time.
-   Retrieve historical average and peak CPU utilization from
    CloudWatch.
-   Identify potential rightsizing candidates.
-   Identify development resources that may be candidates for scheduled
    shutdown.
-   Distinguish heavily utilized resources from potentially
    underutilized resources.

### RDS optimization

The agent can:

-   Discover RDS database instances.
-   Inspect engine, instance class, storage, Multi-AZ configuration, and
    other metadata.
-   Retrieve historical average and peak CPU utilization.
-   Identify potential optimization opportunities while requiring
    additional validation before recommending production changes.

### EBS waste detection

The agent can:

-   Find unattached EBS volumes.
-   Inspect volume type, size, age, encryption, and region.
-   Flag unattached storage as a candidate for investigation rather than
    automatically recommending deletion.

### ECS visibility

The agent can:

-   Discover ECS clusters and services.
-   Inspect desired, running, and pending task counts.
-   Identify launch type/capacity-provider usage.

Deep ECS CPU/memory optimization is not yet implemented.

## Agent Tools

The current Python agent exposes the following read-only tools to
Bedrock:

``` text
get_cost_by_service
get_cost_by_region
get_daily_cost

get_ec2_instances
get_ec2_cpu_metrics

get_unattached_ebs_volumes

get_rds_instances
get_rds_cpu_metrics

get_ecs_services
```

Claude chooses which tools to call during an investigation.

## Example Agent Flow

User:

``` text
Investigate my EC2 spending.
```

Possible investigation:

``` text
Claude
  |
  | Need account cost information
  v
get_cost_by_service()
  |
  v
Claude
  |
  | EC2 is a significant cost driver.
  | Need regional breakdown.
  v
get_cost_by_region()
  |
  v
Claude
  |
  | Need actual instances in expensive regions.
  v
get_ec2_instances()
  |
  v
Claude
  |
  | Need utilization evidence.
  v
get_ec2_cpu_metrics()
  |
  v
Claude
  |
  | Check for orphaned storage.
  v
get_unattached_ebs_volumes()
  |
  v
AWS FinOps Analysis
```

The investigation sequence is selected dynamically by the model rather
than being hardcoded into the application.

## Security Model

The prototype is intentionally **read-only**.

Key controls:

-   AWS authentication is handled through AWS IAM.
-   No Anthropic/OpenAI API key is embedded in the application.
-   boto3 uses the AWS credentials available to the Python runtime.
-   Claude does not receive AWS credentials.
-   The model can request only explicitly registered tools.
-   The registered tools perform read-only operations.
-   No EC2 termination, resizing, RDS modification, ECS update, or
    resource deletion capability is exposed.
-   Tool execution and results are visible through the investigation
    trace.

For a production implementation, IAM permissions should be reduced to
the minimum actions and resources required by the application.

## Technology Stack

  Component             Technology
  --------------------- -----------------------------
  AI platform           Amazon Bedrock
  Foundation model      Anthropic Claude Sonnet 4.5
  Bedrock interface     Converse API + Tool Use
  Agent orchestration   Python
  AWS SDK               boto3
  Cost data             AWS Cost Explorer
  Metrics               Amazon CloudWatch
  Infrastructure data   EC2, RDS, EBS, ECS APIs
  UI                    Streamlit
  Authentication        AWS IAM

## Project Structure

``` text
aws-finops-agent/
|
|-- app.py
|-- agent.py
|-- aws_tools.py
|-- config.py
|-- requirements.txt
`-- README.md
```

### `app.py`

Streamlit web interface. Provides analysis options, displays agent
progress, renders the final FinOps report, and exposes the investigation
trace.

### `agent.py`

Contains the Bedrock Converse API integration, Claude system prompt,
tool definitions, and agent/tool-calling loop.

### `aws_tools.py`

Contains the read-only boto3 implementations for Cost Explorer, EC2,
CloudWatch, RDS, EBS, and ECS.

### `config.py`

Contains Bedrock region, inference profile, analysis period, and
optional AWS profile configuration.

## Prerequisites

-   Python 3.10+
-   AWS CLI configured and authenticated
-   AWS IAM permissions for the required read-only APIs
-   Amazon Bedrock model access
-   A supported Claude inference profile
-   Cost Explorer access

Verify AWS authentication:

``` bash
aws sts get-caller-identity
```

## Amazon Bedrock Configuration

This project uses Amazon Bedrock through AWS IAM authentication. A
separate Anthropic API key is **not** required.

Example configuration:

``` python
BEDROCK_REGION = "us-west-1"

BEDROCK_MODEL_ID = (
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
)
```

The configured ID is a Bedrock inference profile.

For Anthropic models, AWS may require submission of Anthropic model
use-case details for the AWS account before invocation is enabled.

## Installation

Clone or copy the project and create a virtual environment:

``` bash
python -m venv .venv
```

Activate it on Git Bash/Windows:

``` bash
source .venv/Scripts/activate
```

Install dependencies:

``` bash
pip install -r requirements.txt
```

Example `requirements.txt`:

``` text
boto3>=1.40.0
streamlit>=1.40.0
pandas>=2.2.0
```

## Running the Application

Start Streamlit:

``` bash
streamlit run app.py
```

Then open:

``` text
http://localhost:8501
```

The application currently runs locally and uses the AWS credentials
available to the local Python/boto3 environment.

## Using the Application

The UI provides common investigation prompts such as:

### Full Cost Analysis

Provides account-wide cost visibility and asks the agent to investigate
major cost drivers using the tools currently available.

### Investigate EC2

Analyzes EC2 cost by region, discovers running instances, retrieves
CloudWatch CPU utilization, checks for unattached EBS storage, and
produces evidence-based optimization recommendations.

### Investigate RDS

Analyzes RDS spending, discovers database instances, retrieves
CloudWatch CPU utilization, and identifies potential optimization
opportunities.

A custom natural-language question can also be entered directly.

## Investigation Trace

The UI exposes an **Agent Investigation Trace** showing every tool
Claude requested.

Example:

``` text
1. get_cost_by_service
2. get_cost_by_region
3. get_ec2_instances
4. get_ec2_instances
5. get_ec2_cpu_metrics
6. get_ec2_cpu_metrics
7. get_unattached_ebs_volumes
```

This makes the agent's investigation observable and demonstrates that
the model is dynamically selecting tools rather than receiving a
pre-generated report.

## Important Design Principle

The application separates deterministic infrastructure data from AI
reasoning:

``` text
AWS APIs
   |
   v
Facts and Metrics
   |
   v
Python / boto3
   |
   v
Amazon Bedrock
   |
   v
Reasoning and Explanation
```

AWS APIs remain the source of truth.

Claude is used to:

-   Decide what information to investigate.
-   Correlate evidence from multiple AWS services.
-   Identify likely optimization opportunities.
-   Explain recommendations.
-   Highlight risk and missing information.
-   Prioritize next actions.

Claude should not be treated as the authoritative source for
infrastructure state or pricing.

## Limitations

The current prototype has several deliberate limitations:

-   Deep optimization is strongest for EC2 and RDS.
-   ECS resource discovery is implemented, but ECS CPU/memory analysis
    is not yet implemented.
-   S3, Lambda, ELB, NAT Gateway/VPC, DynamoDB, EKS, ElastiCache, and
    other services do not yet have specialized resource-level
    optimization tools.
-   EC2 memory utilization is unavailable unless the CloudWatch Agent is
    installed/configured.
-   CPU utilization alone is not sufficient evidence for safe
    rightsizing.
-   Savings estimates should not be considered authoritative unless
    calculated deterministically from AWS pricing/recommendation data.
-   The application does not modify infrastructure.
-   MCP is not used in the current implementation.
-   Amazon Bedrock Agents are not used; the agent loop is implemented
    directly in Python using Bedrock tool use.

## Recommended Future Improvements

Potential extensions include:

-   AWS Compute Optimizer integration.
-   Deterministic per-resource savings calculations.
-   EC2 memory/network/EBS I/O analysis.
-   ECS CPU and memory utilization analysis.
-   S3 storage-class and lifecycle optimization.
-   Lambda memory/duration/concurrency optimization.
-   NAT Gateway and data-transfer analysis.
-   ELB utilization analysis.
-   DynamoDB capacity analysis.
-   EKS resource-request and node-utilization analysis.
-   Cost anomaly investigation.
-   Cost allocation/tagging analysis.
-   Bedrock token usage and AI cost tracking.
-   Human-in-the-loop remediation proposals.
-   Restricted remediation runbooks using a separate least-privilege IAM
    role.
-   Audit logging and policy enforcement.
-   Optional MCP exposure of the FinOps tools.

## Production Remediation Design

A future remediation architecture should maintain a strict separation
between analysis and write access:

``` text
Bedrock / Claude
      |
      v
Read-only Investigation
      |
      v
Recommendation
      |
      X
 Human Approval
      |
      v
Validated Runbook
      |
      v
Restricted IAM Role
      |
      v
AWS Change
      |
      v
Post-change Validation
```

The model should never receive unrestricted production permissions.