#!/usr/bin/env python3
"""Schema Dump & Verify - Dumps schema-only from each SOURCE RDS and applies to TARGET Aurora.

For each source RDS in migration.yaml:
  1. Lists all non-system databases
  2. pg_dump --schema-only --create for each database
  3. Applies the schema to TARGET Aurora
  4. Compares objects and permissions between SOURCE and TARGET

Usage:
    # Dump, apply and verify all sources
    python scripts/dump_schema.py --config config/migration.yaml

    # Dump only (no apply, no verify)
    python scripts/dump_schema.py --config config/migration.yaml --dump-only

    # Apply + verify from existing dumps
    python scripts/dump_schema.py --config config/migration.yaml --skip-dump

    # Verify only (schemas already applied)
    python scripts/dump_schema.py --config config/migration.yaml --verify-only
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
SYSTEM_DATABASES = {"postgres", "template0", "template1", "rdsadmin"}
SCHEMA_DIR = "output/schemas"
DEFAULT_OUTPUT = "output/schema_report.json"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def list_databases(host, port, user):
    """List all non-system databases on a host."""
    conn = pg_connect(host, port, "postgres", user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datname NOT IN %s
                  AND datistemplate = false
                ORDER BY datname
            """, (tuple(SYSTEM_DATABASES),))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def pg_dump_schema(host, port, user, dbname, output_file):
    """Run pg_dump --schema-only --create and save to file."""
    cmd = [
        "pg_dump",
        "--schema-only",
        "--create",
        "--no-password",
        f"--host={host}",
        f"--port={port}",
        f"--username={user}",
        f"--file={output_file}",
        dbname,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr.strip()}")
    return output_file


def psql_apply(host, port, user, sql_file):
    """Apply a SQL file to target using psql."""
    cmd = [
        "psql",
        "--no-password",
        f"--host={host}",
        f"--port={port}",
        f"--username={user}",
        "--dbname=postgres",
        f"--file={sql_file}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # psql exits 0 even on some errors; check stderr for ERROR lines
    errors = [l for l in result.stderr.splitlines() if l.startswith("ERROR")]
    if result.returncode != 0:
        raise RuntimeError(f"psql failed (exit {result.returncode}): {result.stderr.strip()}")
    return errors  # non-fatal errors (e.g. object already exists)


def fetch_objects(host, port, user, dbname):
    """Fetch all schema objects and their ACLs from a database."""
    conn = pg_connect(host, port, dbname, user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            # Tables & views
            cur.execute("""
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name
            """)
            tables = [{"schema": r[0], "name": r[1], "type": r[2]} for r in cur.fetchall()]

            # Columns
            cur.execute("""
                SELECT table_schema, table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name, ordinal_position
            """)
            columns = [{"schema": r[0], "table": r[1], "column": r[2], "type": r[3], "nullable": r[4]}
                       for r in cur.fetchall()]

            # Sequences
            cur.execute("""
                SELECT sequence_schema, sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY sequence_schema, sequence_name
            """)
            sequences = [{"schema": r[0], "name": r[1]} for r in cur.fetchall()]

            # Functions
            cur.execute("""
                SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                ORDER BY n.nspname, p.proname
            """)
            functions = [{"schema": r[0], "name": r[1], "args": r[2]} for r in cur.fetchall()]

            # Indexes
            cur.execute("""
                SELECT schemaname, tablename, indexname
                FROM pg_indexes
                WHERE schemaname NOT IN ('pg_catalog','information_schema')
                ORDER BY schemaname, tablename, indexname
            """)
            indexes = [{"schema": r[0], "table": r[1], "name": r[2]} for r in cur.fetchall()]

            # Table ACLs (permissions)
            cur.execute("""
                SELECT n.nspname, c.relname, c.relkind,
                       array_to_string(c.relacl, ',') AS acl
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                  AND c.relacl IS NOT NULL
                  AND c.relkind IN ('r','v','S')
                ORDER BY n.nspname, c.relname
            """)
            acls = [{"schema": r[0], "name": r[1], "kind": r[2], "acl": r[3]}
                    for r in cur.fetchall()]

        return {
            "tables": tables,
            "columns": columns,
            "sequences": sequences,
            "functions": functions,
            "indexes": indexes,
            "acls": acls,
        }
    finally:
        conn.close()


def normalize_acl(acl_string):
    """Normalize ACL string to ignore minor version differences.

    - Removes the 'm' (MAINTAIN) privilege added in Aurora PG16+
    - Removes entries where the grantor is 'rdsadmin' (RDS-specific role)
    """
    if not acl_string:
        return acl_string
    normalized = []
    for entry in acl_string.split(","):
        # Skip entries granted by rdsadmin
        if entry.endswith("/rdsadmin"):
            continue
        # Remove the 'm' privilege flag (MAINTAIN, added in PG16 Aurora)
        # ACL format: grantee=privileges/grantor
        if "=" in entry:
            grantee, rest = entry.split("=", 1)
            if "/" in rest:
                privs, grantor = rest.rsplit("/", 1)
                privs = privs.replace("m", "")
                entry = f"{grantee}={privs}/{grantor}"
        normalized.append(entry)
    return ",".join(sorted(normalized))


def is_extension_function(func_name):
    """Return True for functions that belong to PostgreSQL extensions and may
    differ between minor versions (pg_stat_statements, etc.)."""
    extension_prefixes = ("pg_stat_statements",)
    return any(func_name.startswith(p) for p in extension_prefixes)


def compare_objects(src_objs, tgt_objs, dbname):
    """Compare source vs target objects. Returns list of discrepancy dicts."""
    issues = []

    def make_set(items, *keys):
        return {tuple(item[k] for k in keys) for item in items}

    # Tables, sequences, indexes
    for key, fields, label in [
        ("tables",    ["schema", "name", "type"], "table"),
        ("sequences", ["schema", "name"],         "sequence"),
        ("indexes",   ["schema", "table", "name"], "index"),
    ]:
        src_set = make_set(src_objs[key], *fields)
        tgt_set = make_set(tgt_objs[key], *fields)
        for item in sorted(src_set - tgt_set):
            issues.append({"type": "MISSING_IN_TARGET", "object": label, "detail": item, "db": dbname})
        for item in sorted(tgt_set - src_set):
            issues.append({"type": "EXTRA_IN_TARGET", "object": label, "detail": item, "db": dbname})

    # Functions — skip extension functions that differ between minor versions
    src_funcs = {(f["schema"], f["name"], f["args"]) for f in src_objs["functions"]
                 if not is_extension_function(f["name"])}
    tgt_funcs = {(f["schema"], f["name"], f["args"]) for f in tgt_objs["functions"]
                 if not is_extension_function(f["name"])}
    for item in sorted(src_funcs - tgt_funcs):
        issues.append({"type": "MISSING_IN_TARGET", "object": "function", "detail": item, "db": dbname})
    for item in sorted(tgt_funcs - src_funcs):
        issues.append({"type": "EXTRA_IN_TARGET", "object": "function", "detail": item, "db": dbname})

    # ACLs — normalize before comparing, skip extension-owned objects
    SKIP_ACL_OBJECTS = {"pg_stat_statements", "pg_stat_statements_info"}
    src_acls = {(a["schema"], a["name"], a["kind"], normalize_acl(a["acl"]))
                for a in src_objs["acls"] if a["name"] not in SKIP_ACL_OBJECTS}
    tgt_acls = {(a["schema"], a["name"], a["kind"], normalize_acl(a["acl"]))
                for a in tgt_objs["acls"] if a["name"] not in SKIP_ACL_OBJECTS}
    for item in sorted(src_acls - tgt_acls):
        issues.append({"type": "MISSING_IN_TARGET", "object": "acl", "detail": item, "db": dbname})
    for item in sorted(tgt_acls - src_acls):
        issues.append({"type": "EXTRA_IN_TARGET", "object": "acl", "detail": item, "db": dbname})

    # Columns: only check for missing in target, skip extension tables
    SKIP_COL_TABLES = {"pg_stat_statements", "pg_stat_statements_info"}
    src_cols = {(c["schema"], c["table"], c["column"], c["type"])
                for c in src_objs["columns"] if c["table"] not in SKIP_COL_TABLES}
    tgt_cols = {(c["schema"], c["table"], c["column"], c["type"])
                for c in tgt_objs["columns"] if c["table"] not in SKIP_COL_TABLES}
    for item in sorted(src_cols - tgt_cols):
        issues.append({"type": "MISSING_IN_TARGET", "object": "column", "detail": item, "db": dbname})

    return issues


def print_db_result(dbname, src_objs, tgt_objs, issues):
    src = src_objs
    tgt = tgt_objs
    print(f"  {'Object':<12} {'SOURCE':>6}  {'TARGET':>6}  {'Status'}")
    print(f"  {'-'*50}")
    for key, label in [("tables","tables"), ("sequences","seqs"), ("functions","funcs"),
                       ("indexes","indexes"), ("acls","acls")]:
        sc = len(src[key])
        tc = len(tgt[key])
        status = "OK" if sc == tc else f"DIFF ({sc - tc:+d})"
        print(f"  {label:<12} {sc:>6}  {tc:>6}  {status}")

    if issues:
        print(f"\n  Issues ({len(issues)}):")
        for iss in issues[:20]:
            print(f"    [{iss['type']}] {iss['object']}: {iss['detail']}")
        if len(issues) > 20:
            print(f"    ... and {len(issues) - 20} more (see report)")
    else:
        print(f"\n  All objects match.")


def main():
    parser = argparse.ArgumentParser(
        description="Dump schema from SOURCE RDS, apply to TARGET Aurora, verify objects."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--target-aurora", help="Target Aurora cluster name (default: first)")
    parser.add_argument("--schema-dir", default=SCHEMA_DIR)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--dump-only", action="store_true", help="Only dump, skip apply and verify")
    parser.add_argument("--skip-dump", action="store_true", help="Skip dump, apply existing files")
    parser.add_argument("--verify-only", action="store_true", help="Only verify, skip dump and apply")
    args = parser.parse_args()

    config = load_config(args.config)
    sources = config.get("source_rds_endpoints", [])
    aurora_clusters = config.get("target_aurora_clusters", [])

    if args.target_aurora:
        cluster = next((c for c in aurora_clusters if c["name"] == args.target_aurora), None)
        if not cluster:
            print(f"ERROR: Aurora cluster '{args.target_aurora}' not found", file=sys.stderr)
            sys.exit(1)
    else:
        cluster = aurora_clusters[0] if aurora_clusters else None

    if not cluster:
        print("ERROR: No target Aurora cluster in config", file=sys.stderr)
        sys.exit(1)

    target_host = cluster["endpoint"]
    target_name = cluster["name"]

    print(f"Schema Dump & Verify")
    print(f"Target: {target_name}")
    print(f"Sources: {len(sources)}")
    print("=" * 70)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target": {"name": target_name, "endpoint": target_host},
        "databases": [],
        "summary": {"total_dbs": 0, "ok": 0, "with_issues": 0, "failed": 0},
    }

    total_issues = 0

    for src in sources:
        src_host = src["endpoint"]
        src_name = src["name"]
        src_dir = Path(args.schema_dir) / src_name
        src_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n[ {src_name} ]")

        # List databases
        try:
            databases = list_databases(src_host, args.port, args.user)
        except Exception as e:
            print(f"  ERROR listing databases: {e}", file=sys.stderr)
            report["databases"].append({"source": src_name, "error": str(e)})
            report["summary"]["failed"] += 1
            continue

        print(f"  Databases: {', '.join(databases) or '(none)'}")

        for dbname in databases:
            sql_file = str(src_dir / f"{dbname}.sql")
            db_entry = {
                "source": src_name,
                "source_host": src_host,
                "database": dbname,
                "sql_file": sql_file,
                "dump_ok": None,
                "apply_ok": None,
                "apply_warnings": [],
                "issues": [],
                "source_counts": {},
                "target_counts": {},
            }
            print(f"\n  [{dbname}]")

            # Dump
            if not args.skip_dump and not args.verify_only:
                try:
                    pg_dump_schema(src_host, args.port, args.user, dbname, sql_file)
                    size = os.path.getsize(sql_file)
                    print(f"    Dump: OK ({size:,} bytes) -> {sql_file}")
                    db_entry["dump_ok"] = True
                except Exception as e:
                    print(f"    Dump: FAILED — {e}", file=sys.stderr)
                    db_entry["dump_ok"] = False
                    db_entry["error"] = str(e)
                    report["databases"].append(db_entry)
                    report["summary"]["failed"] += 1
                    continue
            else:
                if os.path.exists(sql_file):
                    print(f"    Dump: skipped (using existing {sql_file})")
                    db_entry["dump_ok"] = True
                elif not args.verify_only:
                    print(f"    Dump: file not found {sql_file}", file=sys.stderr)
                    db_entry["dump_ok"] = False
                    report["databases"].append(db_entry)
                    report["summary"]["failed"] += 1
                    continue

            if args.dump_only:
                report["databases"].append(db_entry)
                continue

            # Apply
            if not args.verify_only:
                try:
                    warnings = psql_apply(target_host, args.port, args.user, sql_file)
                    db_entry["apply_ok"] = True
                    db_entry["apply_warnings"] = warnings
                    status = f"OK" + (f" ({len(warnings)} warning(s))" if warnings else "")
                    print(f"    Apply: {status}")
                    for w in warnings[:5]:
                        print(f"      {w}")
                except Exception as e:
                    print(f"    Apply: FAILED — {e}", file=sys.stderr)
                    db_entry["apply_ok"] = False
                    db_entry["error"] = str(e)
                    report["databases"].append(db_entry)
                    report["summary"]["failed"] += 1
                    continue

            # Verify
            try:
                src_objs = fetch_objects(src_host, args.port, args.user, dbname)
                tgt_objs = fetch_objects(target_host, args.port, args.user, dbname)
                issues = compare_objects(src_objs, tgt_objs, dbname)

                db_entry["source_counts"] = {k: len(v) for k, v in src_objs.items()}
                db_entry["target_counts"] = {k: len(v) for k, v in tgt_objs.items()}
                db_entry["issues"] = issues

                print(f"    Verify:")
                print_db_result(dbname, src_objs, tgt_objs, issues)

                total_issues += len(issues)
                if issues:
                    report["summary"]["with_issues"] += 1
                else:
                    report["summary"]["ok"] += 1

            except Exception as e:
                print(f"    Verify: FAILED — {e}", file=sys.stderr)
                db_entry["verify_error"] = str(e)
                report["summary"]["failed"] += 1

            report["summary"]["total_dbs"] += 1
            report["databases"].append(db_entry)

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    s = report["summary"]
    print(f"  Total DBs   : {s['total_dbs']}")
    print(f"  OK          : {s['ok']}")
    print(f"  With issues : {s['with_issues']}")
    print(f"  Failed      : {s['failed']}")
    print(f"  Total issues: {total_issues}")

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport written to: {args.output}")

    if total_issues > 0 or s["failed"] > 0:
        sys.exit(1)
    print("\nAll schemas match.")


if __name__ == "__main__":
    main()
