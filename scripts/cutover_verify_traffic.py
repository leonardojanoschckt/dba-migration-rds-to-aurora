#!/usr/bin/env python3
"""Cutover Step 5 — Verify ECS services are connected to TARGET, not SOURCE.

Checks three signals:

  1. SOURCE connections — non-system users should be 0 (or near 0)
  2. TARGET connections — service users should be present and active
  3. ECS task health   — all desired tasks running, no pending/stopped

Exits 0 if all checks pass, 1 if any check fails.

Usage:
    python scripts/cutover_verify_traffic.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Loop until all checks pass (poll every N seconds)
    python scripts/cutover_verify_traffic.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --watch 15

    # Allow up to N non-system connections still on SOURCE (default: 0)
    python scripts/cutover_verify_traffic.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --max-source-conns 2
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import boto3
import botocore.exceptions
import psycopg2
import yaml


DEFAULT_USER    = "svc_claude"
DEFAULT_PORT    = 5432
DEFAULT_PROFILE = "prd"
DEFAULT_REGION  = "us-east-1"
MIRRORS_REPORT  = "output/peerdb_mirrors.json"
DISCOVERY_REPORT = "output/discovery_report.json"

SYSTEM_USERS = {"svc_claude", "svc_datadog", "svc_pmm", "svc_data_replication", "rdsadmin"}

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
WARN = "\033[33mWARN\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def find_service(config, service_name):
    for src in config.get("source_rds_endpoints", []):
        if src["name"] == service_name:
            return src
    return None


def databases_for_service(service_name, mirrors_report=MIRRORS_REPORT):
    try:
        with open(mirrors_report) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {mirrors_report} not found", file=sys.stderr)
        sys.exit(1)
    return [m["database"] for m in data.get("mirrors", [])
            if m.get("source_rds") == service_name]


def ecs_services_for(service_name, discovery_report=DISCOVERY_REPORT):
    try:
        with open(discovery_report) as f:
            report = json.load(f)
    except FileNotFoundError:
        return []
    matches = report.get("endpoints", {}).get(service_name, {}).get("matches", [])
    seen, svcs = set(), []
    for m in matches:
        arn = m.get("service_arn", "")
        if arn and arn not in seen:
            seen.add(arn)
            svcs.append({"cluster": m["cluster"], "service": m["service"], "service_arn": arn})
    return svcs


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def get_connections(host, port, user, databases):
    """Returns {datname -> {usename -> {state -> count}}}."""
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    datname,
                    usename,
                    COALESCE(state, 'unknown') AS state,
                    count(*)::int
                FROM pg_stat_activity
                WHERE datname = ANY(%s)
                  AND pid <> pg_backend_pid()
                GROUP BY datname, usename, state
                ORDER BY datname, usename
            """, (list(databases),))
            rows = cur.fetchall()
        conn.close()
        result = {}
        for datname, usename, state, count in rows:
            result.setdefault(datname, {}).setdefault(usename, {})[state] = count
        return result
    except Exception as e:
        return {"__error__": str(e)[:80]}


def count_non_system(conn_data):
    """Count total connections from non-system users."""
    total = 0
    for datname, users in conn_data.items():
        if datname == "__error__":
            continue
        for usename, states in users.items():
            if usename not in SYSTEM_USERS:
                total += sum(states.values())
    return total


def count_service_users(conn_data):
    """Count connections from non-system users (service app users)."""
    total = 0
    users = set()
    for datname, umap in conn_data.items():
        if datname == "__error__":
            continue
        for usename, states in umap.items():
            if usename not in SYSTEM_USERS:
                total += sum(states.values())
                users.add(usename)
    return total, users


def get_ecs_status(ecs_services, region, profile):
    """Returns list of status dicts for each ECS service."""
    results = []
    try:
        session = boto3.Session(profile_name=profile, region_name=region)
        client  = session.client("ecs")
    except Exception as e:
        return [{"service": s["service"], "error": str(e)} for s in ecs_services]

    for svc in ecs_services:
        try:
            resp = client.describe_services(
                cluster=svc["cluster"], services=[svc["service_arn"]]
            )
            svcs = resp.get("services", [])
            if not svcs:
                results.append({**svc, "error": "not found"})
                continue
            s = svcs[0]
            deployments = s.get("deployments", [])
            primary     = next((d for d in deployments if d["status"] == "PRIMARY"), None)
            results.append({
                **svc,
                "status":  s.get("status", "?"),
                "desired": s.get("desiredCount", 0),
                "running": s.get("runningCount", 0),
                "pending": s.get("pendingCount", 0),
                "deploy":  primary.get("rolloutState", "?") if primary else "?",
                "error":   None,
            })
        except botocore.exceptions.ClientError as e:
            results.append({**svc, "error": str(e)[:60]})
    return results


def check_source(src_conns, max_source_conns, use_color):
    """Check 1: SOURCE should have ≤ max_source_conns non-system connections."""
    non_sys = count_non_system(src_conns)
    ok = non_sys <= max_source_conns

    status = (PASS if ok else FAIL) if use_color else ("PASS" if ok else "FAIL")
    print(f"\n  [{status}] SOURCE connections (non-system) = {non_sys}  "
          f"(threshold ≤ {max_source_conns})")

    if "__error__" in src_conns:
        print(f"         ERROR querying SOURCE: {src_conns['__error__']}")
        return False

    for datname, users in sorted(src_conns.items()):
        for usename, states in sorted(users.items()):
            if usename in SYSTEM_USERS:
                continue
            parts = ", ".join(f"{s}={n}" for s, n in states.items())
            marker = "  !!!" if not ok else ""
            print(f"         {datname}  {usename}: {parts}{marker}")

    return ok


def check_target(tgt_conns, databases, use_color):
    """Check 2: TARGET should have connections from service users."""
    total, users = count_service_users(tgt_conns)
    ok = total > 0

    status = (PASS if ok else FAIL) if use_color else ("PASS" if ok else "FAIL")
    print(f"\n  [{status}] TARGET connections (service users) = {total}")

    if "__error__" in tgt_conns:
        print(f"         ERROR querying TARGET: {tgt_conns['__error__']}")
        return False

    for datname in sorted(databases):
        users_in_db = tgt_conns.get(datname, {})
        if not users_in_db:
            print(f"         {datname}: (no connections yet)")
            continue
        for usename, states in sorted(users_in_db.items()):
            if usename in SYSTEM_USERS:
                continue
            parts = ", ".join(f"{s}={n}" for s, n in states.items())
            print(f"         {datname}  {usename}: {parts}")

    return ok


def check_ecs(ecs_statuses, use_color):
    """Check 3: all ECS services should be ACTIVE with running == desired."""
    all_ok = True
    print(f"\n  ECS Services:")

    for svc in ecs_statuses:
        name = svc["service"]
        if svc.get("error"):
            status_str = (FAIL if use_color else "FAIL")
            print(f"  [{status_str}] {name}: ERROR — {svc['error']}")
            all_ok = False
            continue

        healthy = (
            svc["status"] == "ACTIVE"
            and svc["running"] == svc["desired"]
            and svc["pending"] == 0
        )
        ok_str  = (PASS if use_color else "PASS") if healthy else (WARN if use_color else "WARN")
        print(f"  [{ok_str}] {name}  "
              f"status={svc['status']}  "
              f"desired={svc['desired']}  running={svc['running']}  pending={svc['pending']}  "
              f"deploy={svc['deploy']}")
        if not healthy:
            all_ok = False

    return all_ok


def run_checks(source_host, target_host, user, port, databases,
               ecs_services, region, profile,
               max_source_conns, use_color):

    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"\n{'=' * 70}")
    print(f"  Verification  [{now}]")
    print(f"{'=' * 70}")

    src_conns = get_connections(source_host, port, user, databases)
    tgt_conns = get_connections(target_host, port, user, databases)
    ecs_stats = get_ecs_status(ecs_services, region, profile)

    ok1 = check_source(src_conns, max_source_conns, use_color)
    ok2 = check_target(tgt_conns, databases, use_color)
    ok3 = check_ecs(ecs_stats, use_color)

    all_ok = ok1 and ok2 and ok3
    verdict = (
        f"{BOLD}\033[32m  ✓ ALL CHECKS PASSED{RESET}" if (all_ok and use_color)
        else (f"{BOLD}\033[31m  ✗ CHECKS FAILED{RESET}" if use_color
              else ("  ✓ ALL CHECKS PASSED" if all_ok else "  ✗ CHECKS FAILED"))
    )
    print(f"\n{verdict}")
    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description="Cutover Step 5: verify ECS services are connected to TARGET Aurora."
    )
    parser.add_argument("--service", required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml")
    parser.add_argument("--watch",   type=int, metavar="SECONDS",
                        help="Repeat every N seconds until all checks pass")
    parser.add_argument("--max-source-conns", type=int, default=0,
                        help="Max allowed non-system connections still on SOURCE (default: 0)")
    parser.add_argument("--user",    default=DEFAULT_USER)
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--aws-profile",     default=DEFAULT_PROFILE)
    parser.add_argument("--region",          default=DEFAULT_REGION)
    parser.add_argument("--mirrors-report",  default=MIRRORS_REPORT)
    parser.add_argument("--discovery-report",default=DISCOVERY_REPORT)
    parser.add_argument("--no-color", action="store_true")
    args = parser.parse_args()

    use_color = not args.no_color and sys.stdout.isatty()

    config      = load_config(args.config)
    source      = find_service(config, args.service)
    if not source:
        print(f"ERROR: service '{args.service}' not found in config", file=sys.stderr)
        sys.exit(1)

    clusters    = config.get("target_aurora_clusters", [])
    source_host = source["endpoint"]
    target_host = clusters[0]["endpoint"] if clusters else ""
    databases   = databases_for_service(args.service, args.mirrors_report)
    ecs_svcs    = ecs_services_for(args.service, args.discovery_report)

    print(f"Cutover Step 5 — Verify Traffic Migration")
    print(f"Service : {args.service}")
    print(f"SOURCE  : {source_host}")
    print(f"TARGET  : {target_host}")
    print(f"DBs     : {databases}")
    print(f"ECS     : {len(ecs_svcs)} service(s)")

    if args.watch:
        try:
            while True:
                all_ok = run_checks(
                    source_host, target_host, args.user, args.port, databases,
                    ecs_svcs, args.region, args.aws_profile,
                    args.max_source_conns, use_color,
                )
                if all_ok:
                    break
                print(f"\n  Retrying in {args.watch}s... (Ctrl+C to stop)")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
            sys.exit(1)
    else:
        all_ok = run_checks(
            source_host, target_host, args.user, args.port, databases,
            ecs_svcs, args.region, args.aws_profile,
            args.max_source_conns, use_color,
        )
        if not all_ok:
            sys.exit(1)


if __name__ == "__main__":
    main()
