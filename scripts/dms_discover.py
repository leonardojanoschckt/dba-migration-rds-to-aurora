#!/usr/bin/env python3
"""DMS - Discover tasks and endpoints associated with a migration config.

For each source RDS in the config (matched by endpoint hostname or CNAME):
  - Finds the DMS SOURCE endpoint
  - Finds all DMS tasks using that endpoint
  - Reports status and saves output/dms_discovery_{config_name}.json

Usage:
    python scripts/dms_discover.py --config config/migration_microservices1_1.yaml

    # With explicit AWS profile
    python scripts/dms_discover.py --config config/migration_microservices1_1.yaml --profile data-prd

    # Save report to custom path
    python scripts/dms_discover.py --config config/migration_microservices1_1.yaml --output output/my_report.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import boto3
import yaml


DEFAULT_PROFILE = "data-prd"
REGION          = "us-east-1"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def get_dms_client(profile):
    session = boto3.Session(profile_name=profile, region_name=REGION)
    return session.client("dms")


def list_all_endpoints(dms):
    endpoints = []
    paginator = dms.get_paginator("describe_endpoints")
    for page in paginator.paginate():
        endpoints.extend(page["Endpoints"])
    return endpoints


def list_all_tasks(dms):
    tasks = []
    paginator = dms.get_paginator("describe_replication_tasks")
    for page in paginator.paginate():
        tasks.extend(page["ReplicationTasks"])
    return tasks


def build_server_index(endpoints):
    """Map server hostname -> endpoint (SOURCE only)."""
    idx = {}
    for ep in endpoints:
        if ep.get("EndpointType", "").upper() != "SOURCE":
            continue
        server = (ep.get("PostgreSQLSettings") or {}).get("ServerName") or ep.get("ServerName") or ""
        if server:
            idx[server.lower()] = ep
    return idx


def match_endpoints(config, server_index):
    """
    For each source RDS in config, find its DMS SOURCE endpoint.
    Matches against both the raw RDS endpoint and any CNAMEs.
    Returns list of {rds_name, rds_endpoint, cnames, dms_endpoint | None}.
    """
    results = []
    for src in config.get("source_rds_endpoints", []):
        candidates = [src["endpoint"]] + src.get("cnames", [])
        matched = None
        matched_by = None
        for c in candidates:
            ep = server_index.get(c.lower())
            if ep:
                matched = ep
                matched_by = c
                break
        results.append({
            "rds_name":     src["name"],
            "rds_endpoint": src["endpoint"],
            "cnames":       src.get("cnames", []),
            "dms_endpoint": matched,
            "matched_by":   matched_by,
        })
    return results


def find_tasks_for_endpoint(tasks, endpoint_arn):
    return [t for t in tasks if t.get("SourceEndpointArn") == endpoint_arn]


def print_report(matches, tasks_by_arn, aurora_host):
    print()
    CW = {"rds": 42, "ep": 38, "task": 38, "status": 10, "type": 20}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(rds, ep, task, status, mtype):
        return (
            f"| {rds:<{CW['rds']}} "
            f"| {ep:<{CW['ep']}} "
            f"| {task:<{CW['task']}} "
            f"| {status:<{CW['status']}} "
            f"| {mtype:<{CW['type']}} |"
        )

    print(sep)
    print(row("RDS SOURCE", "DMS ENDPOINT ID", "DMS TASK", "STATUS", "TYPE"))
    print(sep)

    for m in matches:
        ep = m["dms_endpoint"]
        if not ep:
            print(row(m["rds_name"][:CW["rds"]], "NOT FOUND", "-", "-", "-"))
            print(sep)
            continue
        ep_id = ep["EndpointIdentifier"]
        ep_arn = ep["EndpointArn"]
        task_list = tasks_by_arn.get(ep_arn, [])
        if not task_list:
            print(row(m["rds_name"][:CW["rds"]], ep_id[:CW["ep"]], "NO TASKS", "-", "-"))
            print(sep)
        else:
            for i, t in enumerate(task_list):
                rds_col = m["rds_name"][:CW["rds"]] if i == 0 else ""
                ep_col  = ep_id[:CW["ep"]]           if i == 0 else ""
                print(row(rds_col, ep_col, t["ReplicationTaskIdentifier"][:CW["task"]],
                          t["Status"][:CW["status"]], t["MigrationType"][:CW["type"]]))
            print(sep)


def main():
    parser = argparse.ArgumentParser(description="Discover DMS tasks linked to a migration config.")
    parser.add_argument("--config",  required=True, help="Migration YAML config file")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--output",  default="", help="Output JSON path (default: output/dms_discovery_<config>.json)")
    args = parser.parse_args()

    config = load_config(args.config)
    config_name = os.path.splitext(os.path.basename(args.config))[0]
    output_path = args.output or f"output/dms_discovery_{config_name}.json"

    cluster     = config.get("target_aurora_clusters", [{}])[0]
    aurora_host = (cluster.get("cnames") or [cluster.get("endpoint", "")])[0]

    print(f"DMS Discovery")
    print(f"Config  : {args.config}")
    print(f"Aurora  : {aurora_host}")
    print(f"Profile : {args.profile}")
    print("=" * 70)

    dms = get_dms_client(args.profile)

    print("Fetching DMS endpoints...", end=" ", flush=True)
    all_endpoints = list_all_endpoints(dms)
    print(f"{len(all_endpoints)} found")

    print("Fetching DMS tasks...", end=" ", flush=True)
    all_tasks = list_all_tasks(dms)
    print(f"{len(all_tasks)} found")

    server_index   = build_server_index(all_endpoints)
    matches        = match_endpoints(config, server_index)
    tasks_by_arn   = {}
    for m in matches:
        ep = m["dms_endpoint"]
        if ep:
            arn = ep["EndpointArn"]
            tasks_by_arn[arn] = find_tasks_for_endpoint(all_tasks, arn)

    print_report(matches, tasks_by_arn, aurora_host)

    # Summary
    found    = sum(1 for m in matches if m["dms_endpoint"])
    missing  = len(matches) - found
    total_tasks = sum(len(v) for v in tasks_by_arn.values())
    print(f"\n  RDS with DMS endpoint : {found}/{len(matches)}")
    print(f"  RDS without endpoint  : {missing}")
    print(f"  Total tasks           : {total_tasks}")

    # Build JSON report
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config":       args.config,
        "aurora_host":  aurora_host,
        "sources": []
    }
    for m in matches:
        ep = m["dms_endpoint"]
        task_list = tasks_by_arn.get(ep["EndpointArn"] if ep else "", [])
        report["sources"].append({
            "rds_name":     m["rds_name"],
            "rds_endpoint": m["rds_endpoint"],
            "cnames":       m["cnames"],
            "matched_by":   m["matched_by"],
            "dms_endpoint": {
                "id":     ep["EndpointIdentifier"],
                "arn":    ep["EndpointArn"],
                "db":     (ep.get("PostgreSQLSettings") or {}).get("DatabaseName", ""),
                "status": ep.get("Status", ""),
            } if ep else None,
            "tasks": [
                {
                    "id":       t["ReplicationTaskIdentifier"],
                    "arn":      t["ReplicationTaskArn"],
                    "status":   t["Status"],
                    "type":     t["MigrationType"],
                    "target_arn": t["TargetEndpointArn"],
                    "instance_arn": t["ReplicationInstanceArn"],
                    "settings": t.get("ReplicationTaskSettings", ""),
                    "mappings": t.get("TableMappings", ""),
                }
                for t in task_list
            ],
        })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved to: {output_path}")


if __name__ == "__main__":
    main()
