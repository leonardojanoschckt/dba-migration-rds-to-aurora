#!/usr/bin/env python3
"""Run VACUUM (ANALYZE) on all tables in all schemas for a service's databases.

Connects to TARGET Aurora, enumerates every user table in every non-system
schema, and runs VACUUM (ANALYZE) on each one. Useful after a PeerDB bulk-load
or data migration to refresh statistics and reclaim space.

Usage:
    # Dry-run — list tables that would be vacuumed
    python scripts/vacuum_analyze.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Apply — run VACUUM (ANALYZE)
    python scripts/vacuum_analyze.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply

    # Target SOURCE instead of TARGET
    python scripts/vacuum_analyze.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply --source

    # Only a specific schema
    python scripts/vacuum_analyze.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply --schema v2

    # All services in config
    python scripts/vacuum_analyze.py \\
        --config config/catalog_services_migration.yaml \\
        --all --apply
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

SYSTEM_SCHEMAS = {
    "pg_catalog", "information_schema", "pg_toast",
    "pg_temp_1", "pg_toast_temp_1",
}


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
        print(f"ERROR: no databases found for '{service_name}' in {mirrors_report}",
              file=sys.stderr)
        sys.exit(1)
    return dbs


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def list_tables(conn, schema_filter=None):
    """Return list of (schema, table) for all user tables."""
    with conn.cursor() as cur:
        query = """
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN %s
        """
        params = [tuple(SYSTEM_SCHEMAS)]
        if schema_filter:
            query += " AND schemaname = %s"
            params.append(schema_filter)
        query += " ORDER BY schemaname, tablename"
        cur.execute(query, params)
        return cur.fetchall()


def vacuum_table(conn, schema, table):
    """Run VACUUM (ANALYZE) on one table. Returns (ok, elapsed_ms, error)."""
    fqn = f'"{schema}"."{table}"'
    t0 = time.monotonic()
    try:
        # VACUUM cannot run inside a transaction — need autocommit
        old_ac = conn.autocommit
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"VACUUM (ANALYZE) {fqn}")
        conn.autocommit = old_ac
        elapsed = int((time.monotonic() - t0) * 1000)
        return True, elapsed, None
    except Exception as e:
        conn.autocommit = False
        elapsed = int((time.monotonic() - t0) * 1000)
        return False, elapsed, str(e)[:120]


def process_database(host, port, user, dbname, schema_filter, dry_run):
    print(f"\n  Database: {dbname}")
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.autocommit = True
    except Exception as e:
        print(f"    ERROR connecting: {e}", file=sys.stderr)
        return 0, 0, 1

    tables = list_tables(conn, schema_filter)

    if not tables:
        print("    (no user tables found)")
        conn.close()
        return 0, 0, 0

    schemas = sorted({s for s, _ in tables})
    print(f"    Schemas : {schemas}")
    print(f"    Tables  : {len(tables)}")

    ok_count = skip_count = err_count = 0

    if dry_run:
        for schema, table in tables:
            print(f"    DRY-RUN  {schema}.{table}")
            skip_count += 1
        conn.close()
        return skip_count, 0, 0

    col_w = max(len(f"{s}.{t}") for s, t in tables) + 2

    for schema, table in tables:
        fqn = f"{schema}.{table}"
        ok, ms, err = vacuum_table(conn, schema, table)
        if ok:
            print(f"    OK   {fqn:<{col_w}}  {ms:>6} ms")
            ok_count += 1
        else:
            print(f"    ERR  {fqn:<{col_w}}  {err}")
            err_count += 1

    conn.close()
    return ok_count, skip_count, err_count


def main():
    parser = argparse.ArgumentParser(
        description="Run VACUUM (ANALYZE) on all tables in all schemas for a service."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--service", help="RDS service name")
    group.add_argument("--all", action="store_true",
                       help="Process all services in config")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml")
    parser.add_argument("--apply",   action="store_true",
                        help="Run VACUUM ANALYZE (default: dry-run)")
    parser.add_argument("--source",  action="store_true",
                        help="Run on SOURCE RDS instead of TARGET Aurora")
    parser.add_argument("--schema",  metavar="SCHEMA",
                        help="Limit to a specific schema")
    parser.add_argument("--user",    default=DEFAULT_USER)
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--mirrors-report", default=MIRRORS_REPORT)
    args = parser.parse_args()

    dry_run = not args.apply
    config  = load_config(args.config)

    clusters = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)
    target_host = clusters[0]["endpoint"]

    # Build service list
    if args.all:
        sources = config.get("source_rds_endpoints", [])
    else:
        source = find_service(config, args.service)
        if not source:
            print(f"ERROR: service '{args.service}' not found in config",
                  file=sys.stderr)
            sys.exit(1)
        sources = [source]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"VACUUM (ANALYZE) — {now}")
    print(f"Target  : {'SOURCE' if args.source else 'TARGET'}")
    if not args.source:
        print(f"Host    : {target_host}")
    print(f"Schema  : {args.schema or '(all user schemas)'}")
    print(f"Mode    : {'DRY-RUN' if dry_run else 'APPLY'}")

    grand_ok = grand_skip = grand_err = 0

    for source in sources:
        service_name = source["name"]
        host = source["endpoint"] if args.source else target_host
        databases = databases_for_service(service_name, args.mirrors_report)

        print(f"\n{'─' * 60}")
        print(f"Service : {service_name}")
        print(f"Host    : {host}")
        print(f"DBs     : {databases}")

        for dbname in databases:
            ok, skip, err = process_database(
                host, args.port, args.user, dbname, args.schema, dry_run
            )
            grand_ok   += ok
            grand_skip += skip
            grand_err  += err

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"  Would vacuum : {grand_skip} table(s)")
        print("\n  DRY-RUN — use --apply to run VACUUM (ANALYZE).")
    else:
        print(f"  Vacuumed : {grand_ok}")
        print(f"  Errors   : {grand_err}")

    if grand_err > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
