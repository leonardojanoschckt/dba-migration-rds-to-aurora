#!/usr/bin/env python3
"""Setup _heartbeat table and pg_cron job on Aurora TARGET for each database in config.

For each database:
  1. Connects to TARGET database
  2. Creates _heartbeat table (if not exists) with PK and owned by the DB service user
  3. In TARGET postgres DB: creates pg_cron job to update the heartbeat every minute

Usage:
    python scripts/setup_heartbeat.py --config config/migration.yaml

    # Dry-run
    python scripts/setup_heartbeat.py --config config/migration.yaml --dry-run

    # Only one database
    python scripts/setup_heartbeat.py --config config/migration.yaml --database backoffice_adjustments

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import os
import sys

import psycopg2
import yaml


DEFAULT_USER  = "svc_claude"
DEFAULT_PORT  = 5432
SYSTEM_DBS    = {"postgres", "template0", "template1", "rdsadmin"}
CRON_JOB_PREFIX = "update_heartbeat_"


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def get_db_owner(conn, dbname):
    """Returns the owner role of the given database."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT r.rolname FROM pg_database d "
            "JOIN pg_roles r ON r.oid = d.datdba "
            "WHERE d.datname = %s",
            (dbname,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def list_source_databases(host, port, user):
    """List non-system databases on a SOURCE RDS instance."""
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


def collect_config_databases(config, port, user):
    """Return sorted list of unique databases across all SOURCE instances in config."""
    databases = set()
    for src in config.get("source_rds_endpoints", []):
        host = src["endpoint"]
        name = src["name"]
        try:
            dbs = list_source_databases(host, port, user)
            databases.update(dbs)
        except Exception as e:
            print(f"  WARN: could not list databases from {name}: {e}", file=sys.stderr)
    return sorted(databases)


def setup_heartbeat_table(host, port, user, dbname, dry_run=False):
    """Create _heartbeat table if not exists. Returns (ok, message)."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(autocommit=True)

        # Get DB owner to assign table ownership
        svc_user = get_db_owner(conn, dbname)

        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = '_heartbeat'
            """)
            exists = cur.fetchone() is not None

        if exists:
            conn.close()
            return True, f"already exists (owner lookup: {svc_user})"

        if dry_run:
            conn.close()
            return True, f"dry-run — would create (svc_user={svc_user})"

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE public._heartbeat (
                    id                  int           NOT NULL,
                    last_heartbeat_ts   timestamp     NOT NULL
                )
            """)
            if svc_user:
                cur.execute(f'ALTER TABLE public._heartbeat OWNER TO "{svc_user}"')
            cur.execute("""
                ALTER TABLE public._heartbeat
                    ADD CONSTRAINT "_heartbeat_pkey" PRIMARY KEY (id)
            """)
            cur.execute("""
                INSERT INTO public._heartbeat (id, last_heartbeat_ts)
                VALUES (1, now())
            """)

        conn.close()
        return True, f"created (owner={svc_user})"
    except Exception as e:
        return False, str(e)


def setup_cron_job(host, port, user, dbname, dry_run=False):
    """Create pg_cron job in postgres DB if not exists. Returns (ok, message)."""
    job_name = f"{CRON_JOB_PREFIX}{dbname}"
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(autocommit=True)

        with conn.cursor() as cur:
            # Check if cron job exists
            cur.execute(
                "SELECT 1 FROM cron.job WHERE jobname = %s",
                (job_name,),
            )
            exists = cur.fetchone() is not None

        if exists:
            conn.close()
            return True, f"already exists"

        if dry_run:
            conn.close()
            return True, f"dry-run — would create job '{job_name}'"

        with conn.cursor() as cur:
            cur.execute(
                "SELECT cron.schedule(%s, '*/1 * * * *', "
                "$CRON$ UPDATE _heartbeat SET last_heartbeat_ts = now() WHERE id = 1; $CRON$)",
                (job_name,),
            )
            cur.execute(
                "UPDATE cron.job SET database = %s WHERE jobname = %s",
                (dbname, job_name),
            )

        # Verify
        with conn.cursor() as cur:
            cur.execute(
                "SELECT jobid, jobname, schedule, database FROM cron.job WHERE jobname = %s",
                (job_name,),
            )
            row = cur.fetchone()

        conn.close()
        if row:
            return True, f"created — jobid={row[0]}, db={row[3]}"
        return False, "job not found after creation"
    except Exception as e:
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(
        description="Create _heartbeat table and pg_cron job on Aurora target."
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

    print(f"Heartbeat Setup")
    print(f"Target : {target_host}")
    if args.dry_run:
        print(f"Mode   : dry-run")
    print("=" * 70)

    # Get databases from SOURCE instances defined in config
    databases = collect_config_databases(config, args.port, args.user)
    if not databases:
        print("ERROR: no databases found across source RDS instances", file=sys.stderr)
        sys.exit(1)

    if args.database:
        if args.database not in databases:
            print(f"ERROR: database '{args.database}' not found in any source RDS", file=sys.stderr)
            sys.exit(1)
        databases = [args.database]

    print(f"Databases: {len(databases)}\n")

    summary = {"ok": 0, "skipped": 0, "failed": 0}

    for dbname in sorted(databases):
        print(f"[ {dbname} ]")

        # Step 1: _heartbeat table
        ok, msg = setup_heartbeat_table(target_host, args.port, args.user, dbname, args.dry_run)
        status = "OK" if ok else "FAIL"
        print(f"  _heartbeat table : {status} — {msg}")
        if not ok:
            summary["failed"] += 1
            print()
            continue

        # Step 2: pg_cron job
        ok, msg = setup_cron_job(target_host, args.port, args.user, dbname, args.dry_run)
        status = "OK" if ok else "FAIL"
        print(f"  cron job         : {status} — {msg}")
        if ok:
            summary["ok"] += 1
        else:
            summary["failed"] += 1
        print()

    print("=" * 70)
    print(f"  OK     : {summary['ok']}")
    print(f"  Failed : {summary['failed']}")

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
