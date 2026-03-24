#!/usr/bin/env python3
"""Compare COUNT(*) for all tables in all schemas between SOURCE and TARGET.

For each database belonging to a service, connects to both SOURCE RDS and
TARGET Aurora in parallel and reports row counts side-by-side, flagging
tables where the counts differ.

Usage:
    python scripts/compare_row_counts.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Only a specific schema
    python scripts/compare_row_counts.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --schema v2

    # Show only mismatches
    python scripts/compare_row_counts.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --diff-only

    # All services
    python scripts/compare_row_counts.py \\
        --config config/catalog_services_migration.yaml \\
        --all
"""

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
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

OK   = "\033[32m OK \033[0m"
DIFF = "\033[31mDIFF\033[0m"
ERR  = "\033[33mERR \033[0m"


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


def get_row_counts(host, port, user, dbname, schema_filter=None):
    """Return {schema.table -> count} for all user tables in dbname."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            # Enumerate tables
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
            tables = cur.fetchall()

        counts = {}
        with conn.cursor() as cur:
            for schema, table in tables:
                fqn = f'"{schema}"."{table}"'
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {fqn}")
                    counts[f"{schema}.{table}"] = cur.fetchone()[0]
                except Exception as e:
                    counts[f"{schema}.{table}"] = f"ERROR: {str(e)[:60]}"

        conn.close()
        return counts, None
    except Exception as e:
        return {}, str(e)[:100]


def fmt_count(val):
    if isinstance(val, int):
        return f"{val:,}"
    return str(val)


def print_database_report(dbname, src_counts, tgt_counts,
                          src_err, tgt_err, diff_only, use_color):
    print(f"\n  Database: {dbname}")

    if src_err:
        print(f"    SOURCE ERROR: {src_err}")
    if tgt_err:
        print(f"    TARGET ERROR: {tgt_err}")
    if src_err or tgt_err:
        return 0, 0, 0

    all_tables = sorted(set(src_counts) | set(tgt_counts))
    if not all_tables:
        print("    (no user tables found)")
        return 0, 0, 0

    col_t = max(len(t) for t in all_tables) + 2
    col_n = 15

    header = f"    {'TABLE':<{col_t}}  {'SOURCE':>{col_n}}  {'TARGET':>{col_n}}  STATUS"
    sep    = "    " + "-" * (col_t + col_n * 2 + 14)
    print(sep)
    print(header)
    print(sep)

    ok_count = diff_count = err_count = 0

    for fqn in all_tables:
        src_val = src_counts.get(fqn, "MISSING")
        tgt_val = tgt_counts.get(fqn, "MISSING")

        is_err  = isinstance(src_val, str) or isinstance(tgt_val, str)
        is_diff = not is_err and src_val != tgt_val

        if is_err:
            status = ERR if use_color else "ERR"
            err_count += 1
        elif is_diff:
            status = DIFF if use_color else "DIFF"
            diff_count += 1
        else:
            status = OK if use_color else "OK"
            ok_count += 1

        if diff_only and not is_err and not is_diff:
            continue

        delta_str = ""
        if not is_err and is_diff and isinstance(src_val, int) and isinstance(tgt_val, int):
            delta = tgt_val - src_val
            sign  = "+" if delta >= 0 else ""
            delta_str = f"  ({sign}{delta:,})"

        print(f"    {fqn:<{col_t}}  {fmt_count(src_val):>{col_n}}  "
              f"{fmt_count(tgt_val):>{col_n}}  {status}{delta_str}")

    print(sep)
    print(f"    Tables: {len(all_tables)}  OK: {ok_count}  DIFF: {diff_count}  ERR: {err_count}")

    return ok_count, diff_count, err_count


def process_service(source, target_host, port, user,
                    mirrors_report, schema_filter, diff_only, use_color):
    service_name  = source["name"]
    source_host   = source["endpoint"]
    databases     = databases_for_service(service_name, mirrors_report)

    print(f"\n{'─' * 60}")
    print(f"Service : {service_name}")
    print(f"SOURCE  : {source_host}")
    print(f"TARGET  : {target_host}")
    print(f"DBs     : {databases}")

    grand_ok = grand_diff = grand_err = 0

    for dbname in databases:
        # Fetch counts from SOURCE and TARGET in parallel
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_src = ex.submit(get_row_counts, source_host, port, user, dbname, schema_filter)
            f_tgt = ex.submit(get_row_counts, target_host, port, user, dbname, schema_filter)
            src_counts, src_err = f_src.result()
            tgt_counts, tgt_err = f_tgt.result()

        ok, diff, err = print_database_report(
            dbname, src_counts, tgt_counts, src_err, tgt_err, diff_only, use_color
        )
        grand_ok   += ok
        grand_diff += diff
        grand_err  += err

    return grand_ok, grand_diff, grand_err


def main():
    parser = argparse.ArgumentParser(
        description="Compare COUNT(*) for all tables between SOURCE RDS and TARGET Aurora."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--service", help="RDS service name")
    group.add_argument("--all", action="store_true",
                       help="Compare all services in config")
    parser.add_argument("--config",    default="config/catalog_services_migration.yaml")
    parser.add_argument("--schema",    metavar="SCHEMA",
                        help="Limit comparison to a specific schema")
    parser.add_argument("--diff-only", action="store_true",
                        help="Print only tables where counts differ or have errors")
    parser.add_argument("--user",      default=DEFAULT_USER)
    parser.add_argument("--port",      type=int, default=DEFAULT_PORT)
    parser.add_argument("--mirrors-report", default=MIRRORS_REPORT)
    parser.add_argument("--no-color",  action="store_true")
    args = parser.parse_args()

    use_color = not args.no_color and sys.stdout.isatty()

    config  = load_config(args.config)
    clusters = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)
    target_host = clusters[0]["endpoint"]

    if args.all:
        sources = config.get("source_rds_endpoints", [])
    else:
        source = find_service(config, args.service)
        if not source:
            print(f"ERROR: service '{args.service}' not found in config", file=sys.stderr)
            sys.exit(1)
        sources = [source]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Row Count Comparison — {now}")
    print(f"Schema  : {args.schema or '(all user schemas)'}")
    print(f"Filter  : {'diff only' if args.diff_only else 'all tables'}")

    total_ok = total_diff = total_err = 0

    for source in sources:
        ok, diff, err = process_service(
            source, target_host, args.port, args.user,
            args.mirrors_report, args.schema, args.diff_only, use_color,
        )
        total_ok   += ok
        total_diff += diff
        total_err  += err

    print(f"\n{'=' * 60}")
    print(f"  TOTAL  OK: {total_ok}  DIFF: {total_diff}  ERR: {total_err}")

    if total_diff > 0 or total_err > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
