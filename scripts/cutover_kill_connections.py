#!/usr/bin/env python3
"""Cutover Step 4 — Kill idle connections on SOURCE to force reconnect to TARGET.

After DNS has been updated to point to Aurora, applications will reconnect
to TARGET on their next connection attempt. This script accelerates that
by terminating idle connections on SOURCE so apps are forced to reconnect
immediately rather than waiting for their connection pool to cycle.

Only connections in STATE = 'idle' are killed by default.
Use --include-idle-txn to also kill 'idle in transaction' connections.

The script can loop (--watch N) until no more idle connections remain on SOURCE.

Usage:
    # Dry-run — show what would be killed
    python scripts/cutover_kill_connections.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Apply once
    python scripts/cutover_kill_connections.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply

    # Loop every 10s until SOURCE idles = 0
    python scripts/cutover_kill_connections.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply --watch 10

    # Also kill idle-in-transaction
    python scripts/cutover_kill_connections.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply --include-idle-txn

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import psycopg2
import yaml


DEFAULT_USER   = "svc_claude"
DEFAULT_PORT   = 5432
MIRRORS_REPORT = "output/peerdb_mirrors.json"
SYSTEM_USERS   = {"svc_claude", "svc_datadog", "svc_pmm", "svc_data_replication", "rdsadmin"}


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
    dbs = [m["database"] for m in data.get("mirrors", [])
           if m.get("source_rds") == service_name]
    if not dbs:
        print(f"ERROR: no databases found for '{service_name}'", file=sys.stderr)
        sys.exit(1)
    return dbs


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def get_idle_connections(host, port, user, databases, include_idle_txn=False):
    """Return list of idle connection dicts for the given databases."""
    states = ["idle"]
    if include_idle_txn:
        states.append("idle in transaction")

    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pid,
                    datname,
                    usename,
                    state,
                    COALESCE(EXTRACT(EPOCH FROM (now() - state_change))::int, 0) AS idle_secs,
                    application_name,
                    client_addr::text
                FROM pg_stat_activity
                WHERE datname = ANY(%s)
                  AND state = ANY(%s)
                  AND usename NOT IN %s
                  AND pid <> pg_backend_pid()
                ORDER BY datname, idle_secs DESC
            """, (list(databases), states, tuple(SYSTEM_USERS)))
            rows = cur.fetchall()
        conn.close()
        return [
            {
                "pid":              row[0],
                "datname":          row[1],
                "usename":          row[2],
                "state":            row[3],
                "idle_secs":        row[4],
                "application_name": row[5],
                "client_addr":      row[6],
            }
            for row in rows
        ]
    except Exception as e:
        print(f"  ERROR reading connections from {host}: {e}", file=sys.stderr)
        return []


def get_connection_summary(host, port, user, databases):
    """Return {datname -> {state -> count}} for the given databases."""
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname, COALESCE(state, 'unknown'), count(*)::int
                FROM pg_stat_activity
                WHERE datname = ANY(%s)
                  AND pid <> pg_backend_pid()
                GROUP BY datname, state
                ORDER BY datname, count(*) DESC
            """, (list(databases),))
            rows = cur.fetchall()
        conn.close()
        result = {}
        for datname, state, count in rows:
            result.setdefault(datname, {})[state] = count
        return result
    except Exception as e:
        return {"error": str(e)[:60]}


def kill_connections(host, port, user, pids):
    """Terminate a list of PIDs. Returns (killed, failed)."""
    if not pids:
        return 0, 0
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(autocommit=True)
        killed = failed = 0
        with conn.cursor() as cur:
            for pid in pids:
                try:
                    cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                    result = cur.fetchone()[0]
                    if result:
                        killed += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
        conn.close()
        return killed, failed
    except Exception as e:
        print(f"  ERROR connecting to {host} for kill: {e}", file=sys.stderr)
        return 0, len(pids)


def print_connections(conns):
    """Print idle connections in a table."""
    if not conns:
        print("    (none)")
        return
    CW = {"pid": 7, "db": 28, "user": 20, "state": 22, "idle": 8, "app": 25}
    sep = "  +" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(pid, db, user, state, idle, app):
        return (
            f"  | {str(pid):<{CW['pid']}} "
            f"| {db:<{CW['db']}} "
            f"| {user:<{CW['user']}} "
            f"| {state:<{CW['state']}} "
            f"| {str(idle):>{CW['idle']}} "
            f"| {app:<{CW['app']}} |"
        )

    print(sep)
    print(row("PID", "DATABASE", "USER", "STATE", "IDLE(s)", "APPLICATION"))
    print(sep)
    for c in conns:
        app = (c["application_name"] or "")[:25]
        print(row(c["pid"], c["datname"], c["usename"],
                  c["state"], c["idle_secs"], app))
    print(sep)


def print_summary(label, summary):
    total = sum(n for states in summary.values() if isinstance(states, dict)
                for n in states.values())
    print(f"\n  {label}  (total={total})")
    for db, states in sorted(summary.items()):
        if isinstance(states, dict):
            parts = ", ".join(f"{s}={n}" for s, n in sorted(states.items()))
            print(f"    {db}: {parts}")
        else:
            print(f"    ERROR: {states}")


def run_once(source_host, port, user, databases, include_idle_txn, dry_run, iteration=1):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"\n[{iteration}] {now}")

    idle_conns = get_idle_connections(source_host, port, user, databases, include_idle_txn)

    states_label = "idle" + (" + idle-in-txn" if include_idle_txn else "")
    print(f"  Idle connections on SOURCE ({states_label}): {len(idle_conns)}")
    print_connections(idle_conns)

    if not idle_conns:
        return 0, 0, 0

    pids = [c["pid"] for c in idle_conns]

    if dry_run:
        print(f"\n  DRY-RUN — would kill {len(pids)} connection(s)")
        return len(idle_conns), 0, 0

    print(f"\n  Killing {len(pids)} connection(s)...")
    killed, failed = kill_connections(source_host, port, user, pids)
    print(f"  Killed: {killed}  Failed: {failed}")
    return len(idle_conns), killed, failed


def main():
    parser = argparse.ArgumentParser(
        description="Cutover Step 4: kill idle SOURCE connections to force reconnect to TARGET Aurora."
    )
    parser.add_argument("--service", required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml")
    parser.add_argument("--apply",   action="store_true",
                        help="Kill connections (default: dry-run)")
    parser.add_argument("--watch",   type=int, metavar="SECONDS",
                        help="Repeat every N seconds until no idle connections remain")
    parser.add_argument("--include-idle-txn", action="store_true",
                        help="Also kill 'idle in transaction' connections")
    parser.add_argument("--user",    default=DEFAULT_USER)
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--mirrors-report", default=MIRRORS_REPORT)
    args = parser.parse_args()

    dry_run = not args.apply

    config      = load_config(args.config)
    source      = find_service(config, args.service)
    if not source:
        print(f"ERROR: service '{args.service}' not found in config", file=sys.stderr)
        sys.exit(1)

    clusters    = config.get("target_aurora_clusters", [])
    source_host = source["endpoint"]
    target_host = clusters[0]["endpoint"] if clusters else ""
    databases   = databases_for_service(args.service, args.mirrors_report)

    print(f"Cutover Step 4 — Kill Idle Connections on SOURCE")
    print(f"Service : {args.service}")
    print(f"SOURCE  : {source_host}")
    print(f"TARGET  : {target_host}")
    print(f"DBs     : {databases}")
    print(f"Mode    : {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 70)

    # Connection summary before
    src_before = get_connection_summary(source_host, args.port, args.user, databases)
    tgt_before = get_connection_summary(target_host, args.port, args.user, databases)
    print_summary("SOURCE before", src_before)
    print_summary("TARGET before", tgt_before)

    total_killed = total_failed = 0
    iteration = 0

    if args.watch:
        try:
            while True:
                iteration += 1
                found, killed, failed = run_once(
                    source_host, args.port, args.user,
                    databases, args.include_idle_txn, dry_run, iteration,
                )
                total_killed += killed
                total_failed += failed

                if found == 0 and not dry_run:
                    print(f"\n  No more idle connections on SOURCE — done.")
                    break

                if dry_run:
                    break

                print(f"\n  Waiting {args.watch}s before next pass... (Ctrl+C to stop)")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        iteration = 1
        found, killed, failed = run_once(
            source_host, args.port, args.user,
            databases, args.include_idle_txn, dry_run, iteration,
        )
        total_killed += killed
        total_failed += failed

    # Connection summary after
    if not dry_run:
        print()
        src_after = get_connection_summary(source_host, args.port, args.user, databases)
        tgt_after = get_connection_summary(target_host, args.port, args.user, databases)
        print_summary("SOURCE after ", src_after)
        print_summary("TARGET after ", tgt_after)

    print(f"\n{'=' * 70}")
    if dry_run:
        print(f"  DRY-RUN — use --apply to kill connections.")
    else:
        print(f"  Total killed : {total_killed}")
        print(f"  Total failed : {total_failed}")

    if total_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
