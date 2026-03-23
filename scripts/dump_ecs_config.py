#!/usr/bin/env python3
"""ECS Config Dump - Caches all ECS service configurations for offline search.

Scans all ECS clusters/services/task definitions, resolves secrets and SSM
parameters, and dumps everything to a JSON cache file. This avoids hitting
AWS APIs on every search.

Usage:
    # Full dump (all clusters)
    python scripts/dump_ecs_config.py

    # Dump specific cluster
    python scripts/dump_ecs_config.py --cluster my-cluster

    # Custom output path
    python scripts/dump_ecs_config.py --output output/ecs_cache_20260319.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3


def create_session(profile, region):
    return boto3.Session(profile_name=profile, region_name=region)


def list_all_clusters(ecs_client):
    clusters = []
    paginator = ecs_client.get_paginator("list_clusters")
    for page in paginator.paginate():
        clusters.extend(page["clusterArns"])
    return clusters


def list_all_services(ecs_client, cluster_arn):
    services = []
    paginator = ecs_client.get_paginator("list_services")
    for page in paginator.paginate(cluster=cluster_arn):
        services.extend(page["serviceArns"])
    return services


def describe_services_batch(ecs_client, cluster_arn, service_arns):
    all_services = []
    for i in range(0, len(service_arns), 10):
        batch = service_arns[i : i + 10]
        resp = ecs_client.describe_services(cluster=cluster_arn, services=batch)
        all_services.extend(resp["services"])
    return all_services


def resolve_secret(secrets_client, secret_arn, cache):
    """Resolve secret with dedup cache."""
    if secret_arn in cache:
        return cache[secret_arn]
    try:
        resp = secrets_client.get_secret_value(SecretId=secret_arn)
        value = resp.get("SecretString")
        cache[secret_arn] = value
        return value
    except Exception as e:
        print(f"    [WARN] Could not resolve secret {secret_arn}: {e}", file=sys.stderr)
        cache[secret_arn] = None
        return None


def resolve_ssm(ssm_client, param_name, cache):
    """Resolve SSM parameter with dedup cache."""
    if param_name in cache:
        return cache[param_name]
    try:
        resp = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
        value = resp["Parameter"]["Value"]
        cache[param_name] = value
        return value
    except Exception as e:
        print(f"    [WARN] Could not resolve SSM {param_name}: {e}", file=sys.stderr)
        cache[param_name] = None
        return None


def classify_secret_source(value_from):
    """Determine if a valueFrom points to Secrets Manager or SSM."""
    if ":secretsmanager:" in value_from or value_from.startswith("arn:aws:secretsmanager:"):
        return "secrets_manager"
    elif ":ssm:" in value_from or value_from.startswith("arn:aws:ssm:"):
        return "ssm_parameter"
    elif value_from.startswith("/"):
        return "ssm_parameter"
    else:
        return "secrets_manager"


def dump_task_definition(ecs_client, secrets_client, ssm_client, task_def_arn, secrets_cache, ssm_cache):
    """Extract all env vars and resolved secrets from a task definition."""
    resp = ecs_client.describe_task_definition(taskDefinition=task_def_arn)
    task_def = resp["taskDefinition"]

    containers = []
    for container in task_def.get("containerDefinitions", []):
        env_vars = []
        for env in container.get("environment", []):
            env_vars.append({
                "name": env["name"],
                "value": env.get("value", ""),
            })

        secrets = []
        for secret in container.get("secrets", []):
            value_from = secret.get("valueFrom", "")
            source_type = classify_secret_source(value_from)

            if source_type == "secrets_manager":
                resolved = resolve_secret(secrets_client, value_from, secrets_cache)
            else:
                resolved = resolve_ssm(ssm_client, value_from, ssm_cache)

            secrets.append({
                "name": secret["name"],
                "valueFrom": value_from,
                "source_type": source_type,
                "resolved_value": resolved,
            })

        containers.append({
            "name": container.get("name", "unknown"),
            "image": container.get("image", ""),
            "environment": env_vars,
            "secrets": secrets,
        })

    return containers


def dump_ecs_config(session, region, cluster_filter=None):
    """Dump all ECS service configurations."""
    ecs_client = session.client("ecs", region_name=region)
    secrets_client = session.client("secretsmanager", region_name=region)
    ssm_client = session.client("ssm", region_name=region)

    # Dedup caches: many services share the same secrets/SSM params
    secrets_cache = {}
    ssm_cache = {}

    clusters_data = []
    cluster_arns = list_all_clusters(ecs_client)
    print(f"Found {len(cluster_arns)} ECS cluster(s)")

    for cluster_arn in cluster_arns:
        cluster_name = cluster_arn.split("/")[-1]

        if cluster_filter and cluster_name != cluster_filter:
            continue

        print(f"\nScanning cluster: {cluster_name}")
        service_arns = list_all_services(ecs_client, cluster_arn)

        if not service_arns:
            print("  No services found")
            continue

        print(f"  Found {len(service_arns)} service(s)")
        services_desc = describe_services_batch(ecs_client, cluster_arn, service_arns)

        # Dedup task definitions: multiple services can share one
        task_def_cache = {}
        services_data = []

        for svc in services_desc:
            service_name = svc["serviceName"]
            task_def_arn = svc["taskDefinition"]

            print(f"  Processing: {service_name} -> {task_def_arn.split('/')[-1]}")

            if task_def_arn not in task_def_cache:
                task_def_cache[task_def_arn] = dump_task_definition(
                    ecs_client, secrets_client, ssm_client,
                    task_def_arn, secrets_cache, ssm_cache,
                )

            services_data.append({
                "service_name": service_name,
                "service_arn": svc["serviceArn"],
                "task_definition_arn": task_def_arn,
                "desired_count": svc.get("desiredCount", 0),
                "running_count": svc.get("runningCount", 0),
                "launch_type": svc.get("launchType", ""),
                "containers": task_def_cache[task_def_arn],
            })

        clusters_data.append({
            "cluster_name": cluster_name,
            "cluster_arn": cluster_arn,
            "services": services_data,
        })

    total_services = sum(len(c["services"]) for c in clusters_data)
    print(f"\nDump complete: {len(clusters_data)} cluster(s), {total_services} service(s)")
    print(f"Secrets resolved: {len(secrets_cache)} unique")
    print(f"SSM params resolved: {len(ssm_cache)} unique")

    return {
        "dumped_at": datetime.now(timezone.utc).isoformat(),
        "aws_region": region,
        "total_clusters": len(clusters_data),
        "total_services": total_services,
        "total_secrets_resolved": len(secrets_cache),
        "total_ssm_resolved": len(ssm_cache),
        "clusters": clusters_data,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Dump all ECS service configurations to a JSON cache file."
    )
    parser.add_argument(
        "--profile", default="prd",
        help="AWS profile (default: prd)",
    )
    parser.add_argument(
        "--region", default="us-east-1",
        help="AWS region (default: us-east-1)",
    )
    parser.add_argument(
        "--cluster",
        help="Only dump a specific cluster (by name)",
    )
    parser.add_argument(
        "--output", default="output/ecs_config_cache.json",
        help="Output JSON path (default: output/ecs_config_cache.json)",
    )
    args = parser.parse_args()

    session = create_session(args.profile, args.region)

    print(f"ECS Config Dump - profile={args.profile}, region={args.region}")
    print("=" * 60)

    data = dump_ecs_config(session, args.region, args.cluster)

    with open(args.output, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\nCache written to: {args.output}")


if __name__ == "__main__":
    main()
