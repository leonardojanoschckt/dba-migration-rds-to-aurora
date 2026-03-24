#!/usr/bin/env python3
"""Cutover Step 2 — Advance TARGET sequences to MAX + gap% for a single RDS service.

Looks up the databases for the service in output/peerdb_mirrors.json,
then advances each sequence on the TARGET Aurora to ceil(MAX(col) * (1 + gap/100))
so TARGET IDs won't collide with in-flight SOURCE writes during cutover.

Usage:
    # Dry-run (default) — show what would change
    python scripts/cutover_advance_sequences.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Apply with default 10% gap
    python scripts/cutover_advance_sequences.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply

    # Custom gap
    python scripts/cutover_advance_sequences.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply --gap 20

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import json
import math
import sys

import psycopg2
import yaml


DEFAULT_USER    = "svc_claude"
DEFAULT_PORT    = 5432
DEFAULT_GAP     = 10
MIRRORS_REPORT  = "output/peerdb_mirrors.json"

DISCOVER_SEQUENCES_SQL = """
SELECT
    seq_ns.nspname || '.' || seq.relname  AS seq_fqn,
    tab_ns.nspname                         AS tab_schema,
    tab.relname                            AS tab_name,
    att.attname                            AS col_name
FROM pg_class seq
JOIN pg_namespace seq_ns  ON seq_ns.oid = seq.relnamespace
JOIN pg_depend dep        ON dep.objid = seq.oid
                          AND dep.deptype IN ('a', 'i')
JOIN pg_class tab         ON tab.oid = dep.refobjid
JOIN pg_namespace tab_ns  ON tab_ns.oid = tab.relnamespace
JOIN pg_attribute att     ON att.attrelid = tab.oid
                          AND att.attnum  = dep.refobjsubid
WHERE seq.relkind = 'S'
  AND seq_ns.nspname NOT IN ('pg_catalog', 'information_schema')
  AND tab_ns.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY seq_fqn
"""


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


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
        print(f"ERROR: no databases found for service '{service_name}' in {mirrors_report}",
              file=sys.stderr)
        sys.exit(1)
    return dbs


def advance_sequences(host, port, user, dbname, gap_pct, dry_run):
    multiplier = 1.0 + gap_pct / 100.0
    conn = pg_connect(host, port, dbname, user)
    conn.set_session(autocommit=True)
    results = []

    try:
        with conn.cursor() as cur:
            cur.execute(DISCOVER_SEQUENCES_SQL)
            sequences = cur.fetchall()

        for seq_fqn, tab_schema, tab_name, col_name in sequences:
            # Current sequence state
            with conn.cursor() as cur:
                cur.execute(f'SELECT last_value, is_called FROM {seq_fqn}')
                last_value, is_called = cur.fetchone()

            # MAX of the column
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f'SELECT COALESCE(MAX("{col_name}"), 0) FROM "{tab_schema}"."{tab_name}"'
                    )
                    max_val = cur.fetchone()[0]
                except Exception as e:
                    results.append({
                        "sequence": seq_fqn,
                        "table":    f"{tab_schema}.{tab_name}",
                        "max_val":  None,
                        "gap":      None,
                        "old_val":  last_value,
                        "new_val":  None,
                        "status":   f"ERROR: {e}",
                    })
                    continue

            if max_val == 0:
                new_val        = 1
                is_called_flag = False
                gap_applied    = 0
            else:
                new_val        = math.ceil(max_val * multiplier)
                is_called_flag = True
                gap_applied    = new_val - max_val

            changed = (new_val != last_value) or (is_called_flag != is_called)
            status  = "updated" if changed else "ok (no change)"

            if not dry_run and changed:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT setval({seq_fqn!r}, %s, %s)",
                        (new_val, is_called_flag),
                    )

            results.append({
                "sequence": seq_fqn,
                "table":    f"{tab_schema}.{tab_name}",
                "max_val":  max_val,
                "gap":      gap_applied,
                "old_val":  last_value,
                "new_val":  new_val,
                "status":   ("dry-run: would update" if dry_run and changed else status),
            })
    finally:
        conn.close()

    return results


def print_table(all_results, gap_pct):
    CW = {"db": 28, "seq": 38, "max": 12, "gap": 8, "old": 12, "new": 12, "status": 22}
    sep = "+-" + "-+-".join("-" * w for w in CW.values()) + "-+"

    def row(db, seq, mx, gap, old, new, status):
        return ("| " + " | ".join([
            f"{db:<{CW['db']}}",
            f"{seq:<{CW['seq']}}",
            f"{str(mx):>{CW['max']}}",
            f"{str(gap):>{CW['gap']}}",
            f"{str(old):>{CW['old']}}",
            f"{str(new):>{CW['new']}}",
            f"{status:<{CW['status']}}",
        ]) + " |")

    print()
    print(sep)
    print(row("DATABASE", "SEQUENCE", "MAX(col)", f"+{gap_pct}%",
              "OLD SEQ", "NEW SEQ", "STATUS"))
    print(sep)
    for dbname, r in all_results:
        print(row(
            dbname,
            r["sequence"],
            r["max_val"]  if r["max_val"]  is not None else "N/A",
            r["gap"]      if r["gap"]      is not None else "N/A",
            r["old_val"],
            r["new_val"]  if r["new_val"]  is not None else "N/A",
            r["status"],
        ))
    print(sep)


def main():
    parser = argparse.ArgumentParser(
        description="Cutover Step 2: advance TARGET sequences to MAX + gap%% for one service."
    )
    parser.add_argument("--service", required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml",
                        help="Migration YAML config (default: config/catalog_services_migration.yaml)")
    parser.add_argument("--gap",     type=float, default=DEFAULT_GAP,
                        help=f"Gap %% above MAX(col) (default: {DEFAULT_GAP}%%)")
    parser.add_argument("--apply",   action="store_true",
                        help="Apply to Aurora (default: dry-run)")
    parser.add_argument("--user",    default=DEFAULT_USER)
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--mirrors-report", default=MIRRORS_REPORT)
    args = parser.parse_args()

    dry_run = not args.apply

    config      = load_config(args.config)
    clusters    = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)
    target_host = clusters[0]["endpoint"]

    databases = databases_for_service(args.service, args.mirrors_report)

    print(f"Cutover Step 2 — Advance Sequences")
    print(f"Service : {args.service}")
    print(f"Target  : {target_host}")
    print(f"DBs     : {databases}")
    print(f"Gap     : +{args.gap}%  (new_val = ceil(MAX(col) × {1 + args.gap/100:.4f}))")
    print(f"Mode    : {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 70)

    total_updated = total_errors = 0
    all_results = []

    for dbname in databases:
        print(f"\n  -> {dbname}")
        try:
            results = advance_sequences(target_host, args.port, args.user,
                                        dbname, args.gap, dry_run)
        except Exception as e:
            print(f"  ERROR [{dbname}]: {e}", file=sys.stderr)
            total_errors += 1
            continue

        for r in results:
            all_results.append((dbname, r))
        updated = sum(1 for r in results if "update" in r["status"])
        errors  = sum(1 for r in results if "ERROR"  in r["status"])
        print(f"     sequences: {len(results)}, to update: {updated}, errors: {errors}")
        total_updated += updated
        total_errors  += errors

    if all_results:
        print_table(all_results, args.gap)

    print(f"\n{'=' * 70}")
    print(f"  Gap     : +{args.gap}%")
    print(f"  Updated : {total_updated}")
    print(f"  Errors  : {total_errors}")

    if dry_run:
        print("\n  DRY-RUN — use --apply to execute on Aurora.")

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
