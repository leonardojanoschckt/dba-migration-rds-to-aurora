#!/usr/bin/env python3
"""Advance TARGET Aurora sequences to MAX + gap% to avoid collision with SOURCE.

During a live migration (SOURCE RDS still writing, TARGET Aurora replicating),
setting TARGET sequences to exactly MAX(col) risks collision: SOURCE may have
already issued higher IDs that haven't replicated yet, and the next INSERT on
TARGET would produce a duplicate key.

This script sets each sequence to:

    new_val = ceil( max(col) * (1 + gap/100) )   [default gap = 10%]

so TARGET's next auto-generated ID is comfortably ahead of SOURCE's current
watermark, leaving a buffer zone for in-flight replication.

Usage:
    # Dry-run (default) — shows what would change
    python scripts/advance_sequences.py --config config/migration.yaml

    # Apply with default 10% gap
    python scripts/advance_sequences.py --config config/migration.yaml --apply

    # Apply with custom gap (e.g. 25%)
    python scripts/advance_sequences.py --config config/migration.yaml --apply --gap 25

    # Single database
    python scripts/advance_sequences.py --config config/migration.yaml --apply --database backoffice_adjustments

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import math
import os
import sys

import psycopg2
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
DEFAULT_GAP  = 10          # percent
SYSTEM_DBS   = {"postgres", "template0", "template1", "rdsadmin"}

DISCOVER_SEQUENCES_SQL = """
SELECT
    seq_ns.nspname                          AS seq_schema,
    seq.relname                             AS seq_name,
    tab_ns.nspname                          AS tab_schema,
    tab.relname                             AS tab_name,
    att.attname                             AS col_name,
    att.atttypid::regtype::text             AS col_type
FROM pg_class seq
JOIN pg_namespace seq_ns  ON seq_ns.oid = seq.relnamespace
JOIN pg_depend dep        ON dep.objid = seq.oid
                          AND dep.deptype IN ('a', 'i')   -- serial / identity
JOIN pg_class tab         ON tab.oid = dep.refobjid
JOIN pg_namespace tab_ns  ON tab_ns.oid = tab.relnamespace
JOIN pg_attribute att     ON att.attrelid = tab.oid
                          AND att.attnum  = dep.refobjsubid
WHERE seq.relkind = 'S'
  AND seq_ns.nspname NOT IN ('pg_catalog', 'information_schema')
  AND tab_ns.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY seq_ns.nspname, seq.relname
"""


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def list_target_databases(host, port, user):
    conn = pg_connect(host, port, "postgres", user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datname NOT IN %s AND datistemplate = false
                ORDER BY datname
            """, (tuple(SYSTEM_DBS),))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def advance_sequences(host, port, user, dbname, gap_pct, dry_run=False):
    """Advance all sequences in dbname to MAX + gap%. Returns list of result dicts."""
    multiplier = 1.0 + gap_pct / 100.0
    conn = pg_connect(host, port, dbname, user)
    conn.set_session(autocommit=True)
    results = []

    try:
        with conn.cursor() as cur:
            cur.execute(DISCOVER_SEQUENCES_SQL)
            sequences = cur.fetchall()

        for seq_schema, seq_name, tab_schema, tab_name, col_name, col_type in sequences:
            seq_fqn = f'"{seq_schema}"."{seq_name}"'

            # Current sequence state
            with conn.cursor() as cur:
                cur.execute(f"SELECT last_value, is_called FROM {seq_fqn}")
                last_value, is_called = cur.fetchone()

            # MAX value of the associated column on TARGET
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f'SELECT COALESCE(MAX("{col_name}"), 0) FROM "{tab_schema}"."{tab_name}"'
                    )
                    max_val = cur.fetchone()[0]
                except Exception as e:
                    results.append({
                        "sequence": f"{seq_schema}.{seq_name}",
                        "table":    f"{tab_schema}.{tab_name}",
                        "column":   col_name,
                        "max_val":  None,
                        "gap":      None,
                        "old_val":  last_value,
                        "new_val":  None,
                        "status":   f"ERROR: {e}",
                    })
                    continue

            # Compute target value
            # Empty table → reset to 1 (no gap needed, nothing to collide with)
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
                "sequence": f"{seq_schema}.{seq_name}",
                "table":    f"{tab_schema}.{tab_name}",
                "column":   col_name,
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
    """Print consolidated results table."""
    CW = {
        "db":     30,
        "table":  32,
        "seq":    40,
        "col":    20,
        "max":    12,
        "gap":     8,
        "old":    12,
        "new":    12,
        "status": 22,
    }
    sep = "+-" + "-+-".join("-" * w for w in CW.values()) + "-+"

    def row(db, tbl, seq, col, mx, gap, old, new, status):
        return ("| " + " | ".join([
            f"{db:<{CW['db']}}",
            f"{tbl:<{CW['table']}}",
            f"{seq:<{CW['seq']}}",
            f"{col:<{CW['col']}}",
            f"{str(mx):>{CW['max']}}",
            f"{str(gap):>{CW['gap']}}",
            f"{str(old):>{CW['old']}}",
            f"{str(new):>{CW['new']}}",
            f"{status:<{CW['status']}}",
        ]) + " |")

    print()
    print(sep)
    print(row("DATABASE", "TABLE", "SEQUENCE", "COLUMN",
              "MAX(col)", f"+{gap_pct}%", "OLD SEQ", "NEW SEQ", "STATUS"))
    print(sep)
    for dbname, r in all_results:
        print(row(
            dbname,
            r["table"],
            r["sequence"],
            r["column"],
            r["max_val"]  if r["max_val"]  is not None else "N/A",
            r["gap"]      if r["gap"]      is not None else "N/A",
            r["old_val"],
            r["new_val"]  if r["new_val"]  is not None else "N/A",
            r["status"],
        ))
    print(sep)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Advance TARGET Aurora sequences to MAX(col) + gap%% "
            "to create a safe buffer zone vs SOURCE RDS during live migration."
        )
    )
    parser.add_argument("--config",   default="config/migration.yaml",
                        help="Migration config (default: config/migration.yaml)")
    parser.add_argument("--database", help="Only process this specific database")
    parser.add_argument("--user",     default=DEFAULT_USER)
    parser.add_argument("--port",     type=int, default=DEFAULT_PORT)
    parser.add_argument("--gap",      type=float, default=DEFAULT_GAP,
                        help=f"Gap percentage above MAX(col) (default: {DEFAULT_GAP}%%)")
    parser.add_argument("--apply",    action="store_true",
                        help="Apply changes to Aurora (default: dry-run)")
    args = parser.parse_args()

    dry_run = not args.apply

    with open(args.config) as f:
        config = yaml.safe_load(f)

    aurora_clusters = config.get("target_aurora_clusters", [])
    if not aurora_clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)

    target_host = aurora_clusters[0]["endpoint"]

    print(f"Advance Sequences — TARGET Aurora")
    print(f"Host   : {target_host}")
    print(f"Config : {args.config}")
    print(f"Gap    : +{args.gap}%  (new_val = ceil(MAX(col) * {1 + args.gap/100:.4f}))")
    print(f"Mode   : {'dry-run' if dry_run else 'APPLY'}")
    print("=" * 70)

    try:
        databases = list_target_databases(target_host, args.port, args.user)
    except Exception as e:
        print(f"ERROR listing databases: {e}", file=sys.stderr)
        sys.exit(1)

    if args.database:
        if args.database not in databases:
            print(f"ERROR: '{args.database}' not found on target", file=sys.stderr)
            sys.exit(1)
        databases = [args.database]

    total_updated = total_errors = 0
    all_results = []

    for dbname in sorted(databases):
        try:
            results = advance_sequences(
                target_host, args.port, args.user, dbname, args.gap, dry_run
            )
        except Exception as e:
            print(f"ERROR [{dbname}]: {e}", file=sys.stderr)
            total_errors += 1
            continue

        for r in results:
            all_results.append((dbname, r))
        total_updated += sum(1 for r in results if "update" in r["status"])
        total_errors  += sum(1 for r in results if "ERROR"  in r["status"])

    if all_results:
        print_table(all_results, args.gap)
    else:
        print("\nNo sequences found across all databases.")

    print(f"\n{'=' * 70}")
    print(f"  Gap     : +{args.gap}%")
    print(f"  Updated : {total_updated}")
    print(f"  Errors  : {total_errors}")

    if dry_run:
        print("\nThis was a DRY-RUN. Use --apply to execute on Aurora.")

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
