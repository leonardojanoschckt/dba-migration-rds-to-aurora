#!/usr/bin/env python3
"""DMS - Show status of RDS and Aurora tasks for a migration config.

Reads the discovery report (dms_discover.py output) and queries live status
for both the original RDS-sourced tasks and the duplicated Aurora tasks.

Columns: RDS SOURCE | DB | TASK (RDS) | STATUS | TASK (AURORA) | STATUS

Usage:
    python scripts/dms_task_status.py \\
        --config config/migration_microservices1.yaml \\
        --profile data-prd

    # Watch mode — auto-refresh every N seconds
    python scripts/dms_task_status.py \\
        --config config/migration_microservices1.yaml \\
        --profile data-prd --watch 15
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import boto3
import yaml


DEFAULT_PROFILE  = "data-prd"
REGION           = "us-east-1"
AURORA_EP_PREFIX = "aurora-psql-"


def load_discovery(path):
    with open(path) as f:
        return json.load(f)


def get_dms_client(profile):
    session = boto3.Session(profile_name=profile, region_name=REGION)
    return session.client("dms")


def fetch_task_statuses(dms, task_ids):
    """Return {task_id: {status, cdc_latency, stop_reason}} for the given task IDs."""
    if not task_ids:
        return {}
    result = {}
    try:
        paginator = dms.get_paginator("describe_replication_tasks")
        for page in paginator.paginate(
            Filters=[{"Name": "replication-task-id", "Values": list(task_ids)}]
        ):
            for t in page["ReplicationTasks"]:
                tid = t["ReplicationTaskIdentifier"]
                stats = t.get("ReplicationTaskStats") or {}
                result[tid] = {
                    "status":       t.get("Status", "unknown"),
                    "stop_reason":  t.get("StopReason", ""),
                    "cdc_latency":  stats.get("CDCLatencySource"),
                }
    except Exception as e:
        print(f"  WARN: error fetching task statuses: {e}", file=sys.stderr)
    return result


def aurora_task_id(original_task_id):
    return f"{original_task_id}-aurora"


def status_display(status, stop_reason=""):
    """Short display label for a task status."""
    if status is None:
        return "(not found)"
    label = status
    if stop_reason and status == "stopped":
        short = stop_reason[:30]
        label = f"stopped [{short}]"
    return label


def render(discovery, dms, use_color):
    sources = discovery.get("sources", [])

    # Collect all task IDs to fetch in one call
    task_ids = set()
    for src in sources:
        for t in src.get("tasks", []):
            task_ids.add(t["id"])
            task_ids.add(aurora_task_id(t["id"]))

    statuses = fetch_task_statuses(dms, task_ids)

    CW = {"src": 36, "db": 26, "task": 42, "status": 20}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(src, db, task, status):
        return (
            f"| {src:<{CW['src']}} "
            f"| {db:<{CW['db']}} "
            f"| {task:<{CW['task']}} "
            f"| {status:<{CW['status']}} |"
        )

    def colorize(status_str, original_status):
        if not use_color:
            return status_str
        if original_status == "running":
            return f"\033[32m{status_str}\033[0m"   # green
        if original_status in ("stopped", "failed"):
            return f"\033[33m{status_str}\033[0m"   # yellow
        if original_status == "starting":
            return f"\033[36m{status_str}\033[0m"   # cyan
        return status_str

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\nDMS Task Status  [{now}]")
    print(sep)
    print(row("RDS SOURCE", "DB", "TASK", "STATUS"))
    print(sep)

    for src in sources:
        rds_name = src["rds_name"]
        ep_info  = src.get("dms_endpoint")
        tasks    = src.get("tasks", [])
        dbname   = (ep_info or {}).get("db", "-")

        if not tasks:
            print(row(rds_name[:CW["src"]], dbname[:CW["db"]], "(no tasks)", "-"))
            print(sep)
            continue

        for i, task in enumerate(tasks):
            src_col = rds_name[:CW["src"]] if i == 0 else ""
            db_col  = dbname[:CW["db"]]    if i == 0 else ""

            # RDS task
            rds_tid    = task["id"]
            rds_info   = statuses.get(rds_tid, {})
            rds_status = rds_info.get("status")
            rds_disp   = status_display(rds_status, rds_info.get("stop_reason", ""))

            print(row(
                src_col,
                db_col,
                rds_tid[:CW["task"]],
                colorize(rds_disp[:CW["status"]], rds_status),
            ))

            # Aurora task
            aurora_tid    = aurora_task_id(rds_tid)
            aurora_info   = statuses.get(aurora_tid, {})
            aurora_status = aurora_info.get("status")
            aurora_disp   = status_display(aurora_status, aurora_info.get("stop_reason", ""))

            latency = aurora_info.get("cdc_latency")
            if latency is not None and aurora_status == "running":
                aurora_disp += f" lag={latency}s"

            print(row(
                "",
                "",
                aurora_tid[:CW["task"]],
                colorize(aurora_disp[:CW["status"]], aurora_status),
            ))

        print(sep)


def main():
    parser = argparse.ArgumentParser(
        description="Show live DMS task status for RDS and Aurora tasks in a migration config."
    )
    parser.add_argument("--config",    required=True, help="Migration YAML config")
    parser.add_argument("--discovery", default="",    help="Discovery JSON (default: output/dms_discovery_<config>.json)")
    parser.add_argument("--profile",   default=DEFAULT_PROFILE)
    parser.add_argument("--watch",     type=int, metavar="SECONDS",
                        help="Auto-refresh interval in seconds")
    parser.add_argument("--no-color",  action="store_true")
    args = parser.parse_args()

    config_name    = os.path.splitext(os.path.basename(args.config))[0]
    discovery_path = args.discovery or f"output/dms_discovery_{config_name}.json"

    if not os.path.exists(discovery_path):
        print(f"ERROR: discovery file not found: {discovery_path}", file=sys.stderr)
        print(f"  Run first: python scripts/dms_discover.py --config {args.config} --profile {args.profile}", file=sys.stderr)
        sys.exit(1)

    discovery  = load_discovery(discovery_path)
    dms        = get_dms_client(args.profile)
    use_color  = not args.no_color and sys.stdout.isatty()

    if args.watch:
        try:
            while True:
                if use_color:
                    print("\033[2J\033[H", end="")
                render(discovery, dms, use_color)
                print(f"\n  Refreshing every {args.watch}s — Ctrl+C to stop")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        render(discovery, dms, use_color)


if __name__ == "__main__":
    main()
