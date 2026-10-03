import json
from datetime import datetime, timedelta, timezone

import boto3

from config import AWS_PROFILE, ANALYSIS_DAYS


# =========================================================
# SESSION
# =========================================================

def get_session(region_name=None):
    """
    Create a boto3 session using the same credentials
    configured for the AWS CLI.
    """

    if AWS_PROFILE:
        return boto3.Session(
            profile_name=AWS_PROFILE,
            region_name=region_name,
        )

    return boto3.Session(
        region_name=region_name
    )


# =========================================================
# HELPERS
# =========================================================

def date_range(days=ANALYSIS_DAYS):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)

    return start.isoformat(), end.isoformat()


def safe_round(value, digits=2):
    if value is None:
        return None

    return round(float(value), digits)


def tags_to_dict(tags):
    return {
        tag["Key"]: tag["Value"]
        for tag in tags or []
    }


# =========================================================
# COST EXPLORER
# =========================================================

def get_cost_by_service(days=ANALYSIS_DAYS):
    """
    Return AWS spend grouped by service.
    """

    session = get_session("us-east-1")
    ce = session.client("ce")

    start, end = date_range(days)

    response = ce.get_cost_and_usage(
        TimePeriod={
            "Start": start,
            "End": end,
        },
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[
            {
                "Type": "DIMENSION",
                "Key": "SERVICE",
            }
        ],
    )

    services = {}

    for period in response["ResultsByTime"]:
        for group in period["Groups"]:

            service = group["Keys"][0]

            amount = float(
                group["Metrics"]
                ["UnblendedCost"]
                ["Amount"]
            )

            services[service] = (
                services.get(service, 0)
                + amount
            )

    results = [
        {
            "service": service,
            "cost_usd": safe_round(cost),
        }
        for service, cost in services.items()
        if abs(cost) >= 0.01
    ]

    results.sort(
        key=lambda item: item["cost_usd"],
        reverse=True,
    )

    return {
        "period": {
            "start": start,
            "end": end,
        },
        "services": results,
    }


def get_cost_by_region(
    service_name,
    days=ANALYSIS_DAYS,
):
    """
    Return spend for a specific AWS service grouped
    by region.
    """

    session = get_session("us-east-1")
    ce = session.client("ce")

    start, end = date_range(days)

    response = ce.get_cost_and_usage(
        TimePeriod={
            "Start": start,
            "End": end,
        },
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],

        Filter={
            "Dimensions": {
                "Key": "SERVICE",
                "Values": [service_name],
            }
        },

        GroupBy=[
            {
                "Type": "DIMENSION",
                "Key": "REGION",
            }
        ],
    )

    regions = []

    for period in response["ResultsByTime"]:
        for group in period["Groups"]:

            region = group["Keys"][0]

            amount = float(
                group["Metrics"]
                ["UnblendedCost"]
                ["Amount"]
            )

            regions.append(
                {
                    "region": region,
                    "cost_usd": safe_round(amount),
                }
            )

    regions.sort(
        key=lambda item: item["cost_usd"],
        reverse=True,
    )

    return {
        "service": service_name,
        "regions": regions,
    }


def get_daily_cost(days=ANALYSIS_DAYS):
    """
    Return account-wide daily AWS spend.
    """

    session = get_session("us-east-1")
    ce = session.client("ce")

    start, end = date_range(days)

    response = ce.get_cost_and_usage(
        TimePeriod={
            "Start": start,
            "End": end,
        },
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
    )

    results = []

    for period in response["ResultsByTime"]:

        amount = float(
            period["Total"]
            ["UnblendedCost"]
            ["Amount"]
        )

        results.append(
            {
                "date": period["TimePeriod"]["Start"],
                "cost_usd": safe_round(amount),
            }
        )

    return {
        "period": {
            "start": start,
            "end": end,
        },
        "daily_costs": results,
    }


# =========================================================
# EC2
# =========================================================

def get_ec2_instances(region):
    """
    Return running EC2 instances from a region.
    """

    session = get_session(region)
    ec2 = session.client("ec2")

    paginator = ec2.get_paginator(
        "describe_instances"
    )

    results = []

    for page in paginator.paginate(
        Filters=[
            {
                "Name": "instance-state-name",
                "Values": ["running"],
            }
        ]
    ):

        for reservation in page["Reservations"]:

            for instance in reservation["Instances"]:

                tags = tags_to_dict(
                    instance.get("Tags", [])
                )

                results.append(
                    {
                        "instance_id":
                            instance["InstanceId"],

                        "name":
                            tags.get("Name", ""),

                        "instance_type":
                            instance["InstanceType"],

                        "availability_zone":
                            instance["Placement"]
                            ["AvailabilityZone"],

                        "private_ip":
                            instance.get(
                                "PrivateIpAddress"
                            ),

                        "platform":
                            instance.get(
                                "PlatformDetails"
                            ),

                        "launch_time":
                            instance["LaunchTime"]
                            .isoformat(),

                        "tags": tags,
                    }
                )

    return {
        "region": region,
        "instances": results,
    }


def get_ec2_cpu_metrics(
    region,
    instance_id,
    days=ANALYSIS_DAYS,
):
    """
    Return EC2 CPU utilization from CloudWatch.
    """

    session = get_session(region)

    cloudwatch = session.client(
        "cloudwatch"
    )

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/EC2",
        MetricName="CPUUtilization",

        Dimensions=[
            {
                "Name": "InstanceId",
                "Value": instance_id,
            }
        ],

        StartTime=start,
        EndTime=end,

        # One datapoint per hour.
        Period=3600,

        Statistics=[
            "Average",
            "Maximum",
        ],
    )

    datapoints = response.get(
        "Datapoints",
        []
    )

    if not datapoints:
        return {
            "region": region,
            "instance_id": instance_id,
            "days": days,
            "average_cpu_percent": None,
            "peak_cpu_percent": None,
            "datapoints": 0,
        }

    average_cpu = sum(
        point["Average"]
        for point in datapoints
    ) / len(datapoints)

    peak_cpu = max(
        point["Maximum"]
        for point in datapoints
    )

    return {
        "region": region,
        "instance_id": instance_id,
        "days": days,

        "average_cpu_percent":
            safe_round(average_cpu),

        "peak_cpu_percent":
            safe_round(peak_cpu),

        "datapoints":
            len(datapoints),
    }


# =========================================================
# EBS
# =========================================================

def get_unattached_ebs_volumes(region):
    """
    Find unattached EBS volumes.
    """

    session = get_session(region)
    ec2 = session.client("ec2")

    paginator = ec2.get_paginator(
        "describe_volumes"
    )

    results = []

    for page in paginator.paginate(
        Filters=[
            {
                "Name": "status",
                "Values": ["available"],
            }
        ]
    ):

        for volume in page["Volumes"]:

            tags = tags_to_dict(
                volume.get("Tags", [])
            )

            age_days = (
                datetime.now(timezone.utc)
                - volume["CreateTime"]
            ).days

            results.append(
                {
                    "volume_id":
                        volume["VolumeId"],

                    "name":
                        tags.get("Name", ""),

                    "size_gb":
                        volume["Size"],

                    "volume_type":
                        volume["VolumeType"],

                    "availability_zone":
                        volume[
                            "AvailabilityZone"
                        ],

                    "age_days":
                        age_days,

                    "encrypted":
                        volume["Encrypted"],

                    "tags":
                        tags,
                }
            )

    return {
        "region": region,
        "unattached_volumes": results,
    }


# =========================================================
# RDS
# =========================================================

def get_rds_instances(region):
    """
    Return RDS database instances.
    """

    session = get_session(region)
    rds = session.client("rds")

    paginator = rds.get_paginator(
        "describe_db_instances"
    )

    results = []

    for page in paginator.paginate():

        for db in page["DBInstances"]:

            results.append(
                {
                    "db_identifier":
                        db["DBInstanceIdentifier"],

                    "engine":
                        db["Engine"],

                    "engine_version":
                        db.get(
                            "EngineVersion"
                        ),

                    "instance_class":
                        db["DBInstanceClass"],

                    "status":
                        db["DBInstanceStatus"],

                    "allocated_storage_gb":
                        db.get(
                            "AllocatedStorage"
                        ),

                    "storage_type":
                        db.get(
                            "StorageType"
                        ),

                    "multi_az":
                        db.get(
                            "MultiAZ"
                        ),

                    "availability_zone":
                        db.get(
                            "AvailabilityZone"
                        ),

                    "publicly_accessible":
                        db.get(
                            "PubliclyAccessible"
                        ),

                    "storage_encrypted":
                        db.get(
                            "StorageEncrypted"
                        ),
                }
            )

    return {
        "region": region,
        "databases": results,
    }


def get_rds_cpu_metrics(
    region,
    db_identifier,
    days=ANALYSIS_DAYS,
):
    """
    Return RDS CPU utilization.
    """

    session = get_session(region)

    cloudwatch = session.client(
        "cloudwatch"
    )

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/RDS",
        MetricName="CPUUtilization",

        Dimensions=[
            {
                "Name": "DBInstanceIdentifier",
                "Value": db_identifier,
            }
        ],

        StartTime=start,
        EndTime=end,

        Period=3600,

        Statistics=[
            "Average",
            "Maximum",
        ],
    )

    datapoints = response.get(
        "Datapoints",
        []
    )

    if not datapoints:
        return {
            "region": region,
            "db_identifier": db_identifier,
            "average_cpu_percent": None,
            "peak_cpu_percent": None,
            "datapoints": 0,
        }

    average_cpu = sum(
        point["Average"]
        for point in datapoints
    ) / len(datapoints)

    peak_cpu = max(
        point["Maximum"]
        for point in datapoints
    )

    return {
        "region": region,
        "db_identifier": db_identifier,

        "average_cpu_percent":
            safe_round(average_cpu),

        "peak_cpu_percent":
            safe_round(peak_cpu),

        "datapoints":
            len(datapoints),
    }


# =========================================================
# ECS
# =========================================================

def get_ecs_services(region):
    """
    Return ECS services in a region.
    """

    session = get_session(region)
    ecs = session.client("ecs")

    cluster_paginator = ecs.get_paginator(
        "list_clusters"
    )

    cluster_arns = []

    for page in cluster_paginator.paginate():

        cluster_arns.extend(
            page.get(
                "clusterArns",
                []
            )
        )

    results = []

    for cluster_arn in cluster_arns:

        service_paginator = (
            ecs.get_paginator(
                "list_services"
            )
        )

        service_arns = []

        for page in service_paginator.paginate(
            cluster=cluster_arn
        ):

            service_arns.extend(
                page.get(
                    "serviceArns",
                    []
                )
            )

        for index in range(
            0,
            len(service_arns),
            10,
        ):

            batch = service_arns[
                index:index + 10
            ]

            if not batch:
                continue

            response = ecs.describe_services(
                cluster=cluster_arn,
                services=batch,
            )

            for service in response[
                "services"
            ]:

                results.append(
                    {
                        "cluster":
                            cluster_arn
                            .split("/")[-1],

                        "service":
                            service[
                                "serviceName"
                            ],

                        "desired_count":
                            service[
                                "desiredCount"
                            ],

                        "running_count":
                            service[
                                "runningCount"
                            ],

                        "pending_count":
                            service[
                                "pendingCount"
                            ],

                        "launch_type":
                            service.get(
                                "launchType",
                                "CAPACITY_PROVIDER",
                            ),
                    }
                )

    return {
        "region": region,
        "services": results,
    }


# =========================================================
# TOOL DISPATCH
# =========================================================

TOOL_FUNCTIONS = {
    "get_cost_by_service":
        get_cost_by_service,

    "get_cost_by_region":
        get_cost_by_region,

    "get_daily_cost":
        get_daily_cost,

    "get_ec2_instances":
        get_ec2_instances,

    "get_ec2_cpu_metrics":
        get_ec2_cpu_metrics,

    "get_unattached_ebs_volumes":
        get_unattached_ebs_volumes,

    "get_rds_instances":
        get_rds_instances,

    "get_rds_cpu_metrics":
        get_rds_cpu_metrics,

    "get_ecs_services":
        get_ecs_services,
}


def execute_tool(
    tool_name,
    tool_input,
):
    """
    Execute only explicitly registered read-only tools.
    """

    function = TOOL_FUNCTIONS.get(
        tool_name
    )

    if not function:
        return {
            "error":
                f"Unknown tool: {tool_name}"
        }

    try:

        result = function(
            **tool_input
        )

        return result

    except Exception as exc:

        return {
            "error": str(exc),
            "tool": tool_name,
            "input": tool_input,
        }