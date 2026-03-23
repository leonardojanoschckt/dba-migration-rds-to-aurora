#!/usr/bin/env python3
"""Sync sequences to match current MAX column values on TARGET Aurora.

For each database in the config's target Aurora:
  1. Discovers all sequences associated with table columns
     (both serial/bigserial via pg_depend and IDENTITY columns)
  2. For each sequence, sets its value to MAX(column) so the next
     INSERT gets MAX + 1 (or resets to 1 if the table is empty)

Usage:
    python scripts/sync_sequences.py --config config/migration.yaml

    # Dry-run (show what would change without applying)
    python scripts/sync_sequences.py --config config/migration.yaml --dry-run

    # Only one database
    python scripts/sync_sequences.py --config config/migration.yaml --database backoffice_adjustments

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import os
import sys

import psycopg2
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
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


def sync_sequences(host, port, user, dbname, dry_run=False):
    """Sync all sequences in dbname. Returns list of result dicts."""
    conn = pg_connect(host, port, dbname, user)
    conn.set_session(autocommit=True)
    results = []

    try:
        with conn.cursor() as cur:
            cur.execute(DISCOVER_SEQUENCES_SQL)
            sequences = cur.fetchall()

        for seq_schema, seq_name, tab_schema, tab_name, col_name, col_type in sequences:
            seq_fqn = f'"{seq_schema}"."{seq_name}"'
            col_fqn = f'"{tab_schema}"."{tab_name}"."{col_name}"'

            # Get current sequence value
            with conn.cursor() as cur:
                cur.execute(f"SELECT last_value, is_called FROM {seq_fqn}")
                last_value, is_called = cur.fetchone()

            # Get MAX value of the associated column
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
                        "old_val":  last_value,
                        "new_val":  None,
                        "status":   f"ERROR: {e}",
                    })
                    continue

            # Determine target value:
            # setval(seq, max_val, true) → next nextval() = max_val + 1
            # setval(seq, 1, false)      → next nextval() = 1  (empty table)
            if max_val == 0:
                new_val   = 1
                is_called_flag = False
            else:
                new_val        = max_val
                is_called_flag = True

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
                "old_val":  last_value,
                "new_val":  new_val,
                "status":   ("dry-run: would update" if dry_run and changed else status),
            })

    finally:
        conn.close()

    return results


def print_table(all_results):
    """Print a single consolidated table: DATABASE | TABLE | SEQUENCE | COLUMN | OLD VAL | NEW VAL | STATUS."""
    CW = {"db": 35, "table": 35, "seq": 45, "col": 25, "old": 12, "new": 12, "status": 22}
    sep = "+-" + "-+-".join("-" * w for w in CW.values()) + "-+"

    def row(db, tbl, seq, col, old, new, status):
        return ("| " + " | ".join([
            f"{db:<{CW['db']}}",
            f"{tbl:<{CW['table']}}",
            f"{seq:<{CW['seq']}}",
            f"{col:<{CW['col']}}",
            f"{str(old):>{CW['old']}}",
            f"{str(new):>{CW['new']}}",
            f"{status:<{CW['status']}}",
        ]) + " |")

    print()
    print(sep)
    print(row("DATABASE", "TABLE", "SEQUENCE", "COLUMN", "OLD VAL", "NEW VAL", "STATUS"))
    print(sep)
    for dbname, r in all_results:
        print(row(
            dbname, r["table"], r["sequence"], r["column"],
            r["old_val"], r["new_val"] if r["new_val"] is not None else "N/A",
            r["status"],
        ))
    print(sep)


def main():
    parser = argparse.ArgumentParser(
        description="Sync sequences to MAX column values on TARGET Aurora."
    )
    parser.add_argument("--config", default="config/migration.yaml",
                        help="Migration config (default: config/migration.yaml)")
    parser.add_argument("--database", help="Only process this specific database")
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    aurora_clusters = config.get("target_aurora_clusters", [])
    if not aurora_clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)

    target_host = aurora_clusters[0]["endpoint"]

    print(f"Sequence Sync — TARGET Aurora")
    print(f"Host   : {target_host}")
    print(f"Config : {args.config}")
    if args.dry_run:
        print(f"Mode   : dry-run")
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
            results = sync_sequences(target_host, args.port, args.user, dbname, args.dry_run)
        except Exception as e:
            print(f"ERROR [{dbname}]: {e}", file=sys.stderr)
            total_errors += 1
            continue

        for r in results:
            all_results.append((dbname, r))
        total_updated += sum(1 for r in results if "update" in r["status"])
        total_errors  += sum(1 for r in results if "ERROR"  in r["status"])

    if all_results:
        print_table(all_results)
    else:
        print("\nNo sequences found across all databases.")

    print(f"\n{'=' * 70}")
    print(f"  Updated : {total_updated}")
    print(f"  Errors  : {total_errors}")

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
