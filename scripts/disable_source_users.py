#!/usr/bin/env python3
"""Disable/enable service user connections on SOURCE RDS instances.

For each SOURCE RDS in the config, finds the service accounts that own
databases (same logic as clone_users.py) and runs:
  ALTER ROLE <user> NOLOGIN   (--disable, default)
  ALTER ROLE <user> LOGIN     (--enable, to rollback)

This is the cutover step that prevents applications from reconnecting
to the old RDS after migration to Aurora.

Usage:
    # Dry-run — show which users would be disabled
    python scripts/disable_source_users.py \\
        --config config/migration_microservices1_1.yaml --dry-run

    # Disable login on all service accounts
    python scripts/disable_source_users.py \\
        --config config/migration_microservices1_1.yaml

    # Re-enable login (rollback)
    python scripts/disable_source_users.py \\
        --config config/migration_microservices1_1.yaml --enable

    # Only one RDS
    python scripts/disable_source_users.py \\
        --config config/migration_microservices1_1.yaml \\
        --source backoffice-adjustments-pgsql

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import sys

import psycopg2
import yaml


DEFAULT_USER   = "svc_claude"
DEFAULT_PORT   = 5432
SYSTEM_DATABASES = ("postgres", "template0", "template1", "rdsadmin")

# Extra service accounts to disable regardless of DB ownership
EXTRA_SERVICE_USERS = {"svc_redash"}


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def fetch_db_owners(host, port, user):
    """Return set of rolnames that own at least one non-system database."""
    conn = pg_connect(host, port, "postgres", user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT r.rolname
                FROM pg_database d
                JOIN pg_roles r ON r.oid = d.datdba
                WHERE d.datname NOT IN %s
                  AND d.datistemplate = false
                  AND r.rolcanlogin = true
                ORDER BY r.rolname
            """, (SYSTEM_DATABASES,))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def fetch_login_status(host, port, user, rolname):
    """Return current rolcanlogin value for a role."""
    conn = pg_connect(host, port, "postgres", user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s",
                (rolname,)
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def alter_role_login(host, port, user, rolname, enable, dry_run):
    """
    Set LOGIN or NOLOGIN on rolname.
    Returns (ok: bool, message: str).
    """
    action  = "LOGIN" if enable else "NOLOGIN"
    current = fetch_login_status(host, port, user, rolname)

    if current is None:
        return False, "role not found"

    already = (enable and current) or (not enable and not current)
    if already:
        return True, f"already {'LOGIN' if current else 'NOLOGIN'} — skipped"

    if dry_run:
        return True, f"[dry-run] would ALTER ROLE {rolname} {action}"

    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f'ALTER ROLE "{rolname}" {action}')
        conn.close()
        return True, f"ALTER ROLE {rolname} {action} — OK"
    except Exception as e:
        return False, str(e)


def print_summary_table(rows):
    CW = {"source": 42, "role": 30, "status": 10, "message": 40}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(source, role, status, message):
        return (
            f"| {source:<{CW['source']}} "
            f"| {role:<{CW['role']}} "
            f"| {status:<{CW['status']}} "
            f"| {message:<{CW['message']}} |"
        )

    print()
    print(sep)
    print(row("SOURCE RDS", "ROLE", "STATUS", "MESSAGE"))
    print(sep)
    for r in rows:
        print(row(*r))
    print(sep)


def main():
    parser = argparse.ArgumentParser(
        description="Disable/enable service user logins on SOURCE RDS instances."
    )
    parser.add_argument("--config",  required=True)
    parser.add_argument("--user",    default=DEFAULT_USER)
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--source",  help="Only process this source RDS by name")
    parser.add_argument("--enable",  action="store_true",
                        help="Re-enable login (rollback). Default: disable.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config  = load_config(args.config)
    sources = config.get("source_rds_endpoints", [])

    if args.source:
        sources = [s for s in sources if s["name"] == args.source]
        if not sources:
            print(f"ERROR: source '{args.source}' not found in config", file=sys.stderr)
            sys.exit(1)

    action_label = "ENABLE LOGIN" if args.enable else "DISABLE LOGIN (NOLOGIN)"
    print(f"Source user login — {action_label}")
    print(f"Config  : {args.config}")
    print(f"Sources : {len(sources)}")
    if args.dry_run:
        print("MODE    : dry-run")
    print("=" * 70)

    summary_rows = []
    total_ok = total_skipped = total_errors = 0

    for src in sources:
        src_host = src["endpoint"]
        src_name = src["name"]

        print(f"\n[ {src_name} ]")

        try:
            owners = fetch_db_owners(src_host, args.port, args.user)
        except Exception as e:
            print(f"  ERROR connecting: {e}", file=sys.stderr)
            summary_rows.append((src_name[:42], "-", "ERROR", str(e)[:40]))
            total_errors += 1
            continue

        # Add extra service accounts if they exist on this host
        extra = []
        for extra_user in sorted(EXTRA_SERVICE_USERS):
            if fetch_login_status(src_host, args.port, args.user, extra_user) is not None:
                if extra_user not in owners:
                    extra.append(extra_user)
        owners = owners + extra

        if not owners:
            print(f"  No service accounts found")
            continue

        print(f"  Service accounts: {', '.join(owners)}")

        for rolname in owners:
            ok, msg = alter_role_login(
                src_host, args.port, args.user, rolname,
                enable=args.enable, dry_run=args.dry_run
            )
            status = "OK" if ok else "ERROR"
            skipped = "skipped" in msg or "dry-run" in msg

            if ok and skipped:
                total_skipped += 1
            elif ok:
                total_ok += 1
            else:
                total_errors += 1

            print(f"  {rolname:<30} {status}  {msg}")
            summary_rows.append((src_name[:42], rolname[:30], status, msg[:40]))

    print_summary_table(summary_rows)

    print(f"\n{'=' * 70}")
    print(f"  OK      : {total_ok}")
    print(f"  Skipped : {total_skipped} (already in desired state / dry-run)")
    print(f"  Errors  : {total_errors}")

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
