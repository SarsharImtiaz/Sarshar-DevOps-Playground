import json

import boto3

from config import (
    AWS_PROFILE,
    BEDROCK_REGION,
    BEDROCK_MODEL_ID,
)

from aws_tools import execute_tool


# =========================================================
# BEDROCK CLIENT
# =========================================================

def get_session():

    if AWS_PROFILE:

        return boto3.Session(
            profile_name=AWS_PROFILE
        )

    return boto3.Session()


session = get_session()

bedrock = session.client(
    "bedrock-runtime",
    region_name=BEDROCK_REGION,
)


# =========================================================
# SYSTEM PROMPT
# =========================================================

SYSTEM_PROMPT = """
You are an autonomous AWS FinOps investigation agent.

Your job is to investigate AWS infrastructure costs using
the read-only tools provided to you.

You are not given infrastructure state in advance.

You must decide which tools you need in order to answer the
user's question.

IMPORTANT RULES:

1. Never invent AWS resources, metrics, utilization,
   costs, regions, or configurations.

2. Use tools to gather evidence before making conclusions.

3. Cost Explorer data may contain credits, discounts,
   refunds, taxes, support charges, or NoRegion entries.
   Do not assume every cost entry maps directly to a
   running AWS resource.

4. When a service has significant spend, investigate
   relevant regions and resources when tools are available.

5. Low CPU alone does NOT prove that an EC2 or RDS resource
   should be downsized. Memory, network, storage I/O,
   workload patterns, licensing, CPU credits, and
   application requirements may also matter.

6. If memory metrics are unavailable, explicitly say so.

7. Never claim exact savings unless the evidence returned
   by the tools supports the calculation.

8. Development/test resources may be candidates for
   scheduled shutdown, but resource names alone do not
   prove business-hours requirements.

9. Unattached EBS volumes are optimization candidates,
   not automatically safe to delete.

10. You have READ-ONLY tools. Never claim that you changed
    AWS infrastructure.

11. Prioritize findings by likely financial impact and
    strength of evidence.

12. Explain WHY you called tools and how the evidence
    supports the recommendation.

When producing the final answer, use this structure:

# AWS FinOps Analysis

## Executive Summary

Summarize the strongest findings.

## Cost Drivers

Explain which AWS services and regions are driving spend.

## Optimization Opportunities

For every opportunity include:

Resource:
Finding:
Evidence:
Recommendation:
Risk:
Additional Validation Required:

## Potential Waste

Identify idle or potentially unnecessary resources only
when supported by evidence.

## Recommended Next Actions

Provide a prioritized sequence of actions.

## Limitations

Explain important metrics or information that were not
available and would be needed before making production
changes.
"""


# =========================================================
# TOOL DEFINITIONS
# =========================================================

TOOLS = [
    {
        "toolSpec": {
            "name": "get_cost_by_service",
            "description":
                "Get AWS account spending grouped by "
                "AWS service for a historical period.",

            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "days": {
                            "type": "integer",
                            "description":
                                "Number of historical "
                                "days to analyze."
                        }
                    }
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_cost_by_region",
            "description":
                "Get cost for a specific AWS service "
                "grouped by AWS region.",

            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "service_name": {
                            "type": "string",
                            "description":
                                "Exact Cost Explorer "
                                "service name."
                        },

                        "days": {
                            "type": "integer"
                        }
                    },

                    "required": [
                        "service_name"
                    ]
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_daily_cost",
            "description":
                "Get account-wide AWS spend by day.",

            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "days": {
                            "type": "integer"
                        }
                    }
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_ec2_instances",
            "description":
                "List running EC2 instances in a "
                "specific AWS region.",

            "inputSchema": {
                "json": {
                    "type": "object",

                    "properties": {
                        "region": {
                            "type": "string",
                            "description":
                                "AWS region such as "
                                "us-east-2."
                        }
                    },

                    "required": [
                        "region"
                    ]
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_ec2_cpu_metrics",
            "description":
                "Get historical average and peak "
                "CloudWatch CPU utilization for an "
                "EC2 instance.",

            "inputSchema": {
                "json": {
                    "type": "object",

                    "properties": {
                        "region": {
                            "type": "string"
                        },

                        "instance_id": {
                            "type": "string"
                        },

                        "days": {
                            "type": "integer"
                        }
                    },

                    "required": [
                        "region",
                        "instance_id"
                    ]
                }
            }
        }
    },

    {
        "toolSpec": {
            "name":
                "get_unattached_ebs_volumes",

            "description":
                "Find unattached EBS volumes in a "
                "specific AWS region.",

            "inputSchema": {
                "json": {
                    "type": "object",

                    "properties": {
                        "region": {
                            "type": "string"
                        }
                    },

                    "required": [
                        "region"
                    ]
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_rds_instances",

            "description":
                "List RDS database instances in a "
                "specific AWS region.",

            "inputSchema": {
                "json": {
                    "type": "object",

                    "properties": {
                        "region": {
                            "type": "string"
                        }
                    },

                    "required": [
                        "region"
                    ]
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_rds_cpu_metrics",

            "description":
                "Get historical average and peak "
                "CloudWatch CPU utilization for an "
                "RDS database instance.",

            "inputSchema": {
                "json": {
                    "type": "object",

                    "properties": {
                        "region": {
                            "type": "string"
                        },

                        "db_identifier": {
                            "type": "string"
                        },

                        "days": {
                            "type": "integer"
                        }
                    },

                    "required": [
                        "region",
                        "db_identifier"
                    ]
                }
            }
        }
    },

    {
        "toolSpec": {
            "name": "get_ecs_services",

            "description":
                "List ECS clusters and services in "
                "a specific AWS region.",

            "inputSchema": {
                "json": {
                    "type": "object",

                    "properties": {
                        "region": {
                            "type": "string"
                        }
                    },

                    "required": [
                        "region"
                    ]
                }
            }
        }
    },
]


# =========================================================
# AGENT
# =========================================================

def run_agent(
    user_question,
    progress_callback=None,
    max_iterations=20,
):

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": user_question
                }
            ],
        }
    ]

    tool_history = []

    for iteration in range(
        max_iterations
    ):

        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,

            system=[
                {
                    "text":
                        SYSTEM_PROMPT
                }
            ],

            messages=messages,

            toolConfig={
                "tools": TOOLS
            },

            inferenceConfig={
                "maxTokens": 4000,
                "temperature": 0.1,
            },
            
        )

        assistant_message = response[
            "output"
        ]["message"]

        messages.append(
            assistant_message
        )

        stop_reason = response[
            "stopReason"
        ]

        # -------------------------------------------------
        # MODEL FINISHED
        # -------------------------------------------------

        if stop_reason != "tool_use":

            final_text = []

            for block in assistant_message[
                "content"
            ]:

                if "text" in block:

                    final_text.append(
                        block["text"]
                    )

            return {
                "answer":
                    "\n".join(
                        final_text
                    ),

                "tool_history":
                    tool_history,

                "iterations":
                    iteration + 1,
            }

        # -------------------------------------------------
        # MODEL REQUESTED TOOLS
        # -------------------------------------------------

        tool_results = []

        for block in assistant_message[
            "content"
        ]:

            if "toolUse" not in block:
                continue

            tool_use = block["toolUse"]

            tool_use_id = tool_use[
                "toolUseId"
            ]

            tool_name = tool_use[
                "name"
            ]

            tool_input = tool_use.get(
                "input",
                {},
            )

            if progress_callback:

                progress_callback(
                    f"Claude requested: "
                    f"{tool_name}"
                )

            result = execute_tool(
                tool_name,
                tool_input,
            )

            tool_history.append(
                {
                    "tool":
                        tool_name,

                    "input":
                        tool_input,

                    "result":
                        result,
                }
            )

            if progress_callback:

                progress_callback(
                    f"Completed: "
                    f"{tool_name}"
                )

            tool_results.append(
                {
                    "toolResult": {
                        "toolUseId":
                            tool_use_id,

                        "content": [
                            {
                                "json":
                                    result
                            }
                        ],
                    }
                }
            )

        # Tool results must come back as a USER message.
        messages.append(
            {
                "role": "user",
                "content": tool_results,
            }
        )

    return {
        "answer":
            "The analysis reached the maximum "
            "number of investigation steps.",

        "tool_history":
            tool_history,

        "iterations":
            max_iterations,
    }