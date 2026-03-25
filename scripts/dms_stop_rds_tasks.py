#!/usr/bin/env python3
"""DMS - Stop RDS source tasks for databases in the migration config.

Reads the discovery report (dms_discover.py output) and stops all DMS tasks
whose SOURCE endpoint points to an RDS instance (not Aurora) listed in the config.

Aurora-sourced tasks ({task}-aurora) are intentionally skipped.

Usage:
    # Dry-run (show what would be stopped)
    python scripts/dms_stop_rds_tasks.py \\
        --config config/migration_microservices1_1.yaml \\
        --profile data-prd --dry-run

    # Stop tasks
    python scripts/dms_stop_rds_tasks.py \\
        --config config/migration_microservices1_1.yaml \\
        --profile data-prd

    # Stop and wait until all reach 'stopped' status
    python scripts/dms_stop_rds_tasks.py \\
        --config config/migration_microservices1_1.yaml \\
        --profile data-prd --wait
"""

import argparse
import json
import os
import sys
import time

import boto3
import yaml


DEFAULT_PROFILE = "data-prd"
REGION          = "us-east-1"
AURORA_EP_PREFIX = "aurora-psql-"


def load_discovery(path):
    with open(path) as f:
        return json.load(f)


def get_dms_client(profile):
    session = boto3.Session(profile_name=profile, region_name=REGION)
    return session.client("dms")


def get_task_status(dms, task_arn):
    try:
        r = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}]
        )
        tasks = r.get("ReplicationTasks", [])
        return tasks[0]["Status"] if tasks else None
    except Exception:
        return None


def stop_task(dms, task_id, task_arn, dry_run):
    status = get_task_status(dms, task_arn)
    if status is None:
        print(f"    {task_id} : not found")
        return "not_found"
    if status == "stopped":
        print(f"    {task_id} : already stopped")
        return "already_stopped"
    if status in ("stopping", "deleting"):
        print(f"    {task_id} : {status} (in progress)")
        return status
    if dry_run:
        print(f"    {task_id} : [dry-run] would stop (current={status})")
        return "dry_run"
    try:
        dms.stop_replication_task(ReplicationTaskArn=task_arn)
        print(f"    {task_id} : stop requested (was {status})")
        return "stop_requested"
    except Exception as e:
        print(f"    {task_id} : ERROR — {e}", file=sys.stderr)
        return "error"


def wait_for_stopped(dms, pending, timeout=300, interval=10):
    """Wait until all tasks in pending reach 'stopped'. pending = [(task_id, task_arn)]."""
    deadline = time.time() + timeout
    remaining = list(pending)
    print(f"\n  Waiting for {len(remaining)} task(s) to stop (timeout={timeout}s)...")

    while remaining and time.time() < deadline:
        still_running = []
        for task_id, task_arn in remaining:
            status = get_task_status(dms, task_arn)
            if status == "stopped":
                print(f"    {task_id} : stopped")
            elif status is None:
                print(f"    {task_id} : disappeared (status=None)")
            else:
                still_running.append((task_id, task_arn))

        remaining = still_running
        if remaining:
            statuses = []
            for task_id, task_arn in remaining:
                s = get_task_status(dms, task_arn)
                statuses.append(f"{task_id}={s}")
            print(f"    still running: {', '.join(statuses)}")
            time.sleep(interval)

    if remaining:
        print(f"\n  WARNING: {len(remaining)} task(s) did not stop within {timeout}s", file=sys.stderr)
        return False
    print(f"  All tasks stopped.")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Stop DMS tasks whose source is an RDS instance in the migration config."
    )
    parser.add_argument("--config",    required=True, help="Migration YAML config")
    parser.add_argument("--discovery", default="",    help="Discovery JSON (default: output/dms_discovery_<config>.json)")
    parser.add_argument("--profile",   default=DEFAULT_PROFILE)
    parser.add_argument("--wait",      action="store_true", help="Wait until all tasks reach 'stopped'")
    parser.add_argument("--timeout",   type=int, default=300, help="Wait timeout in seconds (default: 300)")
    parser.add_argument("--dry-run",   action="store_true")
    args = parser.parse_args()

    config_name    = os.path.splitext(os.path.basename(args.config))[0]
    discovery_path = args.discovery or f"output/dms_discovery_{config_name}.json"

    if not os.path.exists(discovery_path):
        print(f"ERROR: discovery file not found: {discovery_path}", file=sys.stderr)
        print(f"  Run first: python scripts/dms_discover.py --config {args.config} --profile {args.profile}", file=sys.stderr)
        sys.exit(1)

    discovery = load_discovery(discovery_path)

    print("DMS — Stop RDS source tasks")
    print(f"Config    : {args.config}")
    print(f"Discovery : {discovery_path}")
    print(f"Profile   : {args.profile}")
    if args.dry_run:
        print("MODE      : dry-run")
    print("=" * 70)

    dms = get_dms_client(args.profile)

    stop_requested = []
    total_stopped = total_skipped = total_errors = 0

    for src in discovery["sources"]:
        rds_name = src["rds_name"]
        ep_info  = src.get("dms_endpoint")
        tasks    = src.get("tasks", [])

        print(f"\n[ {rds_name} ]")

        if not ep_info:
            print(f"  No DMS endpoint found — skipping")
            continue

        if not tasks:
            print(f"  No tasks — skipping")
            continue

        # Only process tasks whose source endpoint is NOT an Aurora endpoint
        rds_tasks = [t for t in tasks if not ep_info["id"].startswith(AURORA_EP_PREFIX)]
        if not rds_tasks:
            print(f"  No RDS-sourced tasks — skipping")
            continue

        print(f"  Source endpoint : {ep_info['id']}")

        for task in rds_tasks:
            result = stop_task(dms, task["id"], task["arn"], args.dry_run)
            if result == "stop_requested":
                stop_requested.append((task["id"], task["arn"]))
                total_stopped += 1
            elif result in ("already_stopped", "stopping", "dry_run", "not_found"):
                total_skipped += 1
            elif result == "error":
                total_errors += 1

    print(f"\n{'=' * 70}")
    print(f"  Stop requested : {total_stopped}")
    print(f"  Skipped        : {total_skipped} (already stopped / not found)")
    print(f"  Errors         : {total_errors}")

    if args.wait and stop_requested and not args.dry_run:
        wait_for_stopped(dms, stop_requested, timeout=args.timeout)

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
