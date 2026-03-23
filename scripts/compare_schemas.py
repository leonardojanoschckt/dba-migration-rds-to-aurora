#!/usr/bin/env python3
"""Compare schemas SOURCE RDS vs TARGET Aurora using catalog queries.

For each source RDS in config:
  1. Queries pg_catalog on SOURCE and TARGET for each database
  2. Compares tables, columns, sequences, indexes, functions
  3. Reports differences per database and a consolidated summary table

Usage:
    python scripts/compare_schemas.py --config config/migration_microservices1_1.yaml

    # Only one database
    python scripts/compare_schemas.py --config config/migration_microservices1_1.yaml --database backoffice_adjustments

    # Show per-object diff details
    python scripts/compare_schemas.py --config config/migration_microservices1_1.yaml --show-diff

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import sys

import psycopg2
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
SYSTEM_DBS   = {"postgres", "template0", "template1", "rdsadmin"}

# Schemas excluded entirely from comparison
SKIP_SCHEMAS   = {"_peerdb_internal"}

# Objects/tables introduced by RDS internal tooling or version differences — ignored
SKIP_TABLES    = {"awsdms_ddl_audit", "awsdms_changes", "_heartbeat",
                  "pg_stat_statements", "pg_stat_statements_info"}
SKIP_FUNCTIONS = {"pg_stat_statements", "pg_stat_statements_reset",
                  "pg_stat_statements_info", "pg_buffercache_pages",
                  "pg_buffercache_summary", "pg_buffercache_usage_counts"}


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def db_exists(host, port, user, dbname):
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        conn.close()


def list_databases(host, port, user):
    conn = pg_connect(host, port, "postgres", user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datname NOT IN %s AND datistemplate = false
                ORDER BY datname
            """, (tuple(SYSTEM_DBS),))
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def fetch_objects(host, port, user, dbname):
    conn = pg_connect(host, port, dbname, user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:

            skip_schemas_full = tuple({"pg_catalog", "information_schema"} | SKIP_SCHEMAS)

            # Tables (excluding system + RDS internals)
            cur.execute("""
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN %s
                  AND table_name NOT IN %s
                ORDER BY table_schema, table_name
            """, (skip_schemas_full, tuple(SKIP_TABLES)))
            tables = {(r[0], r[1], r[2]) for r in cur.fetchall()}

            # Columns
            cur.execute("""
                SELECT table_schema, table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema NOT IN %s
                  AND table_name NOT IN %s
                ORDER BY table_schema, table_name, ordinal_position
            """, (skip_schemas_full, tuple(SKIP_TABLES)))
            columns = {(r[0], r[1], r[2], r[3]) for r in cur.fetchall()}

            # Sequences
            cur.execute("""
                SELECT sequence_schema, sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema NOT IN %s
                ORDER BY sequence_schema, sequence_name
            """, (skip_schemas_full,))
            sequences = {(r[0], r[1]) for r in cur.fetchall()}

            # Indexes
            cur.execute("""
                SELECT schemaname, tablename, indexname
                FROM pg_indexes
                WHERE schemaname NOT IN %s
                  AND tablename NOT IN %s
                ORDER BY schemaname, tablename, indexname
            """, (skip_schemas_full, tuple(SKIP_TABLES)))
            indexes = {(r[0], r[1], r[2]) for r in cur.fetchall()}

            # Functions (excluding extension functions)
            cur.execute("""
                SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname NOT IN %s
                  AND p.proname NOT IN %s
                ORDER BY n.nspname, p.proname
            """, (tuple({"pg_catalog", "information_schema", "pg_toast"} | SKIP_SCHEMAS),
                  tuple(SKIP_FUNCTIONS)))
            functions = {(r[0], r[1], r[2]) for r in cur.fetchall()}

        return {
            "tables":    tables,
            "columns":   columns,
            "sequences": sequences,
            "indexes":   indexes,
            "functions": functions,
        }
    finally:
        conn.close()


def compare(src, tgt):
    """Compare source and target objects. Returns dict of {type: (missing, extra)}."""
    results = {}
    for key in src:
        missing = sorted(src[key] - tgt[key])
        extra   = sorted(tgt[key] - src[key])
        results[key] = (missing, extra)
    return results


def print_summary_table(rows):
    """Print consolidated results table."""
    CW = {"db": 35, "status": 8, "tables": 8, "cols": 8, "seqs": 8, "idx": 8, "funcs": 8}
    sep = "+-" + "-+-".join("-" * w for w in CW.values()) + "-+"

    def row(db, status, tables, cols, seqs, idx, funcs):
        return ("| " + " | ".join([
            f"{db:<{CW['db']}}",
            f"{status:<{CW['status']}}",
            f"{str(tables):^{CW['tables']}}",
            f"{str(cols):^{CW['cols']}}",
            f"{str(seqs):^{CW['seqs']}}",
            f"{str(idx):^{CW['idx']}}",
            f"{str(funcs):^{CW['funcs']}}",
        ]) + " |")

    print()
    print(sep)
    print(row("DATABASE", "STATUS", "TABLES", "COLUMNS", "SEQS", "INDEXES", "FUNCS"))
    print(sep)
    for r in rows:
        print(row(*r))
    print(sep)


def main():
    parser = argparse.ArgumentParser(
        description="Compare schemas SOURCE RDS vs TARGET Aurora via catalog queries."
    )
    parser.add_argument("--config",    default="config/migration.yaml")
    parser.add_argument("--database",  help="Only compare this specific database")
    parser.add_argument("--user",      default=DEFAULT_USER)
    parser.add_argument("--port",      type=int, default=DEFAULT_PORT)
    parser.add_argument("--show-diff", action="store_true",
                        help="Print per-object differences")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    sources  = config.get("source_rds_endpoints", [])
    clusters = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)

    target_host = clusters[0]["endpoint"]

    print("Schema Comparison — SOURCE RDS vs TARGET Aurora")
    print(f"Config : {args.config}")
    print(f"Target : {target_host}")
    print("=" * 70)

    summary_rows = []
    total_diff = total_ok = total_error = 0

    for src in sources:
        src_host = src["endpoint"]
        src_name = src["name"]

        print(f"\n[ {src_name} ]")

        try:
            databases = list_databases(src_host, args.port, args.user)
        except Exception as e:
            print(f"  ERROR listing databases: {e}", file=sys.stderr)
            total_error += 1
            continue

        if args.database:
            databases = [d for d in databases if d == args.database]
            if not databases:
                continue

        for dbname in databases:
            print(f"  [{dbname}]", end=" ", flush=True)

            if not db_exists(target_host, args.port, args.user, dbname):
                print("TARGET: not found — skipping")
                summary_rows.append((dbname, "NO TGT", "-", "-", "-", "-", "-"))
                total_error += 1
                continue

            try:
                src_obj = fetch_objects(src_host, args.port, args.user, dbname)
                tgt_obj = fetch_objects(target_host, args.port, args.user, dbname)
            except Exception as e:
                print(f"ERROR: {e}", file=sys.stderr)
                summary_rows.append((dbname, "ERROR", "-", "-", "-", "-", "-"))
                total_error += 1
                continue

            diff = compare(src_obj, tgt_obj)

            has_diff = any(missing or extra for missing, extra in diff.values())
            status = "DIFF" if has_diff else "OK"

            def diff_str(key):
                missing, extra = diff[key]
                total = len(missing) + len(extra)
                return f"+{len(extra)}/-{len(missing)}" if total else "OK"

            print(status)

            if args.show_diff and has_diff:
                for key, (missing, extra) in diff.items():
                    if missing or extra:
                        print(f"    [{key.upper()}]")
                        for item in missing:
                            print(f"      MISSING in TARGET : {item}")
                        for item in extra:
                            print(f"      EXTRA in TARGET   : {item}")

            summary_rows.append((
                dbname, status,
                diff_str("tables"), diff_str("columns"),
                diff_str("sequences"), diff_str("indexes"), diff_str("functions"),
            ))

            if has_diff:
                total_diff += 1
            else:
                total_ok += 1

    print_summary_table(summary_rows)

    print(f"\n{'=' * 70}")
    print(f"  OK     : {total_ok}")
    print(f"  DIFF   : {total_diff}")
    print(f"  Errors : {total_error}")

    if total_diff > 0 or total_error > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
