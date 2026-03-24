#!/usr/bin/env python3
"""Run pg_initial_setup on all databases in the target Aurora cluster.

Connects to the target Aurora, creates cluster-level roles/users once,
then iterates every non-system database and applies schema-level GRANTs
and extensions.

Usage:
    # Dry-run: print SQL only (default)
    python scripts/pg_initial_setup.py --config config/catalog_services_migration.yaml

    # Apply to all databases on target Aurora
    python scripts/pg_initial_setup.py --config config/catalog_services_migration.yaml --apply

    # Single database only
    python scripts/pg_initial_setup.py --config config/catalog_services_migration.yaml --apply --dbname mydb

    # Override target host
    python scripts/pg_initial_setup.py --target-host my-aurora.cluster.rds.amazonaws.com --apply
"""

import argparse
import sys
from datetime import datetime, timezone

import psycopg2
import yaml

DEFAULT_USER  = "svc_claude"
DEFAULT_PORT  = 5432

SYSTEM_DBS = {"postgres", "template0", "template1", "rdsadmin"}

# Schemas excluded from GRANT statements (mirrors the SQL regex filter)
EXCLUDED_SCHEMA_RE = r"(^pg_|^information_schema|^datadog)"

# ---------------------------------------------------------------------------
# Cluster-level setup — run once against the 'postgres' database
# ---------------------------------------------------------------------------

CLUSTER_SETUP_SQL = """\
-- ============================================================
-- CLUSTER-LEVEL: roles, users, memberships
-- ============================================================

-- CREATE ROLE READONLY
DO $$ BEGIN
  CREATE ROLE readonly WITH NOLOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
EXCEPTION WHEN duplicate_object THEN
  RAISE NOTICE 'Role "readonly" already exists, skipping';
END $$;

-- CREATE ROLE READWRITE
DO $$ BEGIN
  CREATE ROLE readwrite WITH NOLOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
EXCEPTION WHEN duplicate_object THEN
  RAISE NOTICE 'Role "readwrite" already exists, skipping';
END $$;

-- CREATE ROLE SYSADMIN
DO $$ BEGIN
  CREATE ROLE sysadmin WITH NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
  RAISE NOTICE 'Role "sysadmin" already exists, skipping';
END $$;

-- CREATE USERS
DO $$ BEGIN CREATE USER "leonardo_janosch"     WITH CREATEDB CREATEROLE PASSWORD 'eaNGBuhUnCdEeBtHQUkjh4KQ'; EXCEPTION WHEN duplicate_object THEN ALTER USER "leonardo_janosch"     PASSWORD 'eaNGBuhUnCdEeBtHQUkjh4KQ'; END $$;
DO $$ BEGIN CREATE USER "svc_claude"           WITH CREATEDB CREATEROLE PASSWORD '5RTfFOUFT2E22GzpK89vRYMs'; EXCEPTION WHEN duplicate_object THEN ALTER USER "svc_claude"           PASSWORD '5RTfFOUFT2E22GzpK89vRYMs'; END $$;
DO $$ BEGIN CREATE USER "svc_scheduler"        WITH PASSWORD 'Wcuw5F7Jx2atbV5gN4ezKa5c';                      EXCEPTION WHEN duplicate_object THEN ALTER USER "svc_scheduler"        PASSWORD 'Wcuw5F7Jx2atbV5gN4ezKa5c'; END $$;
DO $$ BEGIN CREATE USER "svc_redash"           WITH PASSWORD 'cmp7y^wGsz6G35Ku2hQBH4m9';                      EXCEPTION WHEN duplicate_object THEN ALTER USER "svc_redash"           PASSWORD 'cmp7y^wGsz6G35Ku2hQBH4m9'; END $$;
DO $$ BEGIN CREATE USER "svc_datadog"          WITH PASSWORD 'BnhsZEa4xCmeLDnB4P6MN7pU';                      EXCEPTION WHEN duplicate_object THEN ALTER USER "svc_datadog"          PASSWORD 'BnhsZEa4xCmeLDnB4P6MN7pU'; END $$;
DO $$ BEGIN CREATE USER "svc_data_replication" WITH PASSWORD 'MH3fR7T5ycgsxgCmSrtMuEvw';                      EXCEPTION WHEN duplicate_object THEN ALTER USER "svc_data_replication" PASSWORD 'MH3fR7T5ycgsxgCmSrtMuEvw'; END $$;
DO $$ BEGIN CREATE USER "svc_pmm"              WITH PASSWORD 'pcs8T3UFT2E22GzpK7G9V9Jg';                      EXCEPTION WHEN duplicate_object THEN ALTER USER "svc_pmm"              PASSWORD 'pcs8T3UFT2E22GzpK7G9V9Jg'; END $$;

-- GRANT sysadmin membership to admin roles found in cluster
SELECT format('GRANT %I TO sysadmin WITH ADMIN OPTION;', rolname)
  FROM pg_roles
 WHERE rolname ~* '(^admin|^postgres|master)';

-- GRANT users → roles / privileges
GRANT rds_superuser   TO "leonardo_janosch"     WITH ADMIN OPTION;
GRANT rds_superuser   TO "svc_scheduler"        WITH ADMIN OPTION;
GRANT rds_superuser   TO "svc_data_replication" WITH ADMIN OPTION;
GRANT rds_superuser   TO "svc_claude"           WITH ADMIN OPTION;
GRANT sysadmin        TO "leonardo_janosch"     WITH ADMIN OPTION;
GRANT sysadmin        TO "svc_scheduler";
GRANT pg_read_all_data TO "svc_redash";
GRANT readonly        TO "svc_redash";
GRANT pg_read_all_data TO "svc_data_replication";
GRANT readonly        TO "svc_data_replication";
GRANT pg_monitor      TO "svc_datadog";
GRANT pg_monitor      TO "svc_pmm";
GRANT rds_replication TO "svc_data_replication";
"""

# ---------------------------------------------------------------------------
# Per-database, per-schema GRANTs (generated dynamically from pg_namespace)
# ---------------------------------------------------------------------------

# Each tuple: (role_name, list_of_grant_templates)
# Templates use {role} and {schema} placeholders.
PER_SCHEMA_GRANTS = [
    ("readonly", [
        "GRANT USAGE ON SCHEMA {schema} TO {role};",
        "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {role};",
        "GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON SEQUENCES TO {role};",
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {role};",
    ]),
    ("readwrite", [
        "GRANT USAGE ON SCHEMA {schema} TO {role};",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role};",
        "GRANT ALL ON ALL SEQUENCES IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON SEQUENCES TO {role};",
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {role};",
    ]),
    ("sysadmin", [
        "GRANT USAGE ON SCHEMA {schema} TO {role};",
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {role};",
        "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {role};",
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {role};",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {role};",
    ]),
]

PER_DB_EXTENSION_SQL = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def pg_connect(host, user, port=DEFAULT_PORT, dbname="postgres"):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def list_databases(conn):
    """Return all non-system database names on the cluster."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT datname FROM pg_database
             WHERE datistemplate = false
               AND datname NOT IN %s
             ORDER BY datname
        """, (tuple(SYSTEM_DBS),))
        return [row[0] for row in cur.fetchall()]


def list_schemas(conn):
    """Return schemas eligible for GRANTs (excludes pg_*, information_schema, datadog)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname FROM pg_namespace
             WHERE nspname !~ %s
             ORDER BY nspname
        """, (EXCLUDED_SCHEMA_RE,))
        return [row[0] for row in cur.fetchall()]


def fetch_admin_roles(conn):
    """Roles matching the sysadmin membership query (admin|postgres|master)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rolname FROM pg_roles
             WHERE rolname ~* '(^admin|^postgres|master)'
        """)
        return [row[0] for row in cur.fetchall()]


def exec_statements(conn, statements, label, dry_run):
    """Execute a list of SQL statements. Returns (applied, errors)."""
    applied = errors = 0
    with conn.cursor() as cur:
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue
            if dry_run:
                print(f"  [DRY] {stmt}")
                applied += 1
                continue
            try:
                cur.execute(stmt)
                applied += 1
            except Exception as e:
                short = stmt[:80].replace("\n", " ")
                print(f"  [ERROR] {label}: {short!r}: {e}", file=sys.stderr)
                errors += 1
    if not dry_run:
        conn.commit()
    return applied, errors


def build_cluster_statements(conn):
    """Return list of SQL statements for cluster-level setup."""
    stmts = []

    # Static DDL block (roles, users, grants)
    for line in CLUSTER_SETUP_SQL.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            stmts.append(stripped)

    # Dynamic: GRANT admin roles to sysadmin
    admin_roles = fetch_admin_roles(conn)
    for role in admin_roles:
        stmts.append(f'GRANT "{role}" TO sysadmin WITH ADMIN OPTION;')

    return stmts


def build_db_statements(conn, dbname):
    """Return list of SQL statements for a single database."""
    schemas = list_schemas(conn)
    stmts = []
    stmts.append(f"-- Database: {dbname} | schemas: {schemas}")

    for role, templates in PER_SCHEMA_GRANTS:
        stmts.append(f"\n-- Role: {role}")
        for schema in schemas:
            for tpl in templates:
                stmts.append(tpl.format(role=role, schema=schema))

    stmts.append("\n-- Extension")
    stmts.append(PER_DB_EXTENSION_SQL)

    return stmts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Run pg_initial_setup on all databases in target Aurora (uses .pgpass for auth)."
    )
    parser.add_argument("--config",      help="Migration YAML config file")
    parser.add_argument("--target-host", help="Target Aurora endpoint (overrides config)")
    parser.add_argument("--target-aurora", help="Name of target Aurora cluster from config (default: first)")
    parser.add_argument("--user",        default=DEFAULT_USER,
                        help=f"PostgreSQL user (default: {DEFAULT_USER})")
    parser.add_argument("--port",        type=int, default=DEFAULT_PORT)
    parser.add_argument("--dbname",      help="Apply to a single database only (default: all)")
    parser.add_argument("--apply",       action="store_true",
                        help="Apply SQL to Aurora (default: dry-run, prints SQL only)")
    args = parser.parse_args()

    if not args.config and not args.target_host:
        parser.error("Either --config or --target-host is required")

    # Resolve target host
    target_host = args.target_host
    if args.config and not target_host:
        config = load_config(args.config)
        clusters = config.get("target_aurora_clusters", [])
        if args.target_aurora:
            cluster = next((c for c in clusters if c["name"] == args.target_aurora), None)
            if not cluster:
                parser.error(f"Aurora cluster '{args.target_aurora}' not found in config")
        else:
            cluster = clusters[0] if clusters else None
        if cluster:
            target_host = cluster["endpoint"]

    if not target_host:
        parser.error("Could not resolve target Aurora host")

    dry_run = not args.apply
    mode = "DRY-RUN" if dry_run else "APPLY"
    print(f"Target: {target_host}")
    print(f"User:   {args.user}")
    print(f"Mode:   {mode}")
    print("=" * 60)

    total_applied = total_errors = 0

    # ----------------------------------------------------------------
    # 1. Cluster-level setup (connect to postgres db)
    # ----------------------------------------------------------------
    print("\n[1/2] Cluster-level setup (roles, users, memberships)...")
    try:
        conn = pg_connect(target_host, args.user, args.port, "postgres")
        conn.autocommit = True
    except Exception as e:
        print(f"ERROR connecting to {target_host}: {e}", file=sys.stderr)
        sys.exit(1)

    cluster_stmts = build_cluster_statements(conn)
    applied, errors = exec_statements(conn, cluster_stmts, "cluster", dry_run)
    print(f"  Applied: {applied}, Errors: {errors}")
    total_applied += applied
    total_errors  += errors

    # Enumerate target databases
    if args.dbname:
        databases = [args.dbname]
        print(f"\n[2/2] Per-database setup — targeting single db: {args.dbname}")
    else:
        databases = list_databases(conn)
        print(f"\n[2/2] Per-database setup — {len(databases)} database(s): {databases}")

    conn.close()

    # ----------------------------------------------------------------
    # 2. Per-database: schema GRANTs + extension
    # ----------------------------------------------------------------
    for dbname in databases:
        print(f"\n  -> {dbname}")
        try:
            db_conn = pg_connect(target_host, args.user, args.port, dbname)
            db_conn.autocommit = True
        except Exception as e:
            print(f"  [ERROR] Cannot connect to db '{dbname}': {e}", file=sys.stderr)
            total_errors += 1
            continue

        db_stmts = build_db_statements(db_conn, dbname)
        applied, errors = exec_statements(db_conn, db_stmts, dbname, dry_run)
        print(f"     Applied: {applied}, Errors: {errors}")
        total_applied += applied
        total_errors  += errors
        db_conn.close()

    # ----------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print(f"TOTAL Applied: {total_applied}, Errors: {total_errors}")
    if dry_run:
        print("\nThis was a DRY-RUN. Use --apply to execute on Aurora.")
    if total_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
