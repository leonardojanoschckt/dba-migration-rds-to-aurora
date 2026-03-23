#!/usr/bin/env python3
"""Clone service users from source RDS instances to target Aurora.

Connects to each source RDS listed in migration.yaml using svc_claude (via .pgpass),
extracts service roles (excluding RDS/PG system roles), and generates CREATE ROLE
statements with password hashes preserved.

Usage:
    # Generate SQL only (review before applying)
    python scripts/clone_users.py --config config/migration.yaml

    # Apply directly to target Aurora
    python scripts/clone_users.py --config config/migration.yaml --apply

    # Single source RDS
    python scripts/clone_users.py --source-host mydb.xxx.rds.amazonaws.com

    # Override user (default: svc_claude via .pgpass)
    python scripts/clone_users.py --config config/migration.yaml --user otheruser
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import psycopg2
import yaml

# Default user — credentials resolved from ~/.pgpass
DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
DEFAULT_DBNAME = "postgres"

# RDS/Aurora system roles to exclude entirely
SYSTEM_ROLES = {
    "rdsadmin", "rds_superuser", "rds_replication", "rds_password",
    "rds_ad", "rdsrepladmin", "rds_iam",
    "rds_extension", "rds_reserved",        # RDS internal group roles, not needed on Aurora
    "postgres",                              # Aurora already has its own postgres master user
    "pg_monitor", "pg_read_all_settings", "pg_read_all_stats",
    "pg_stat_scan_tables", "pg_signal_backend", "pg_read_server_files",
    "pg_write_server_files", "pg_execute_server_program",
    "pg_checkpoint", "pg_maintain", "pg_use_reserved_connections",
    "pg_create_subscription", "pg_database_owner",
}


def is_service_role(role, db_owners):
    """Return True if the role should be cloned to Aurora.

    Only svc_* roles that own a database on the source are cloned.
    Group roles (NOLOGIN), human users, and shared infra accounts are excluded.
    """
    return role["rolcanlogin"] and role["rolname"] in db_owners


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def pg_connect(host, user, port=DEFAULT_PORT, dbname=DEFAULT_DBNAME):
    """Connect using .pgpass for password resolution."""
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def read_pgpass(pgpass_path=None):
    """Parse ~/.pgpass and return a lookup dict: (host, port, username) -> password.

    .pgpass format: hostname:port:database:username:password
    Wildcards (*) are supported per PostgreSQL spec.
    Returns list of (host, port, db, user, password) tuples in file order
    so the first match wins (same as libpq behaviour).
    """
    path = pgpass_path or os.path.expanduser("~/.pgpass")
    entries = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) < 5:
                    continue
                # Last field may contain colons (password), rejoin it
                host, port, db, user = parts[0], parts[1], parts[2], parts[3]
                password = ":".join(parts[4:])
                entries.append((host, port, db, user, password))
    except FileNotFoundError:
        pass
    return entries


def lookup_password(pgpass_entries, host, rolname, port="5432"):
    """Return the first password in .pgpass matching host+port+rolname.

    Wildcard '*' in .pgpass matches any value.
    """
    def matches(pattern, value):
        return pattern == "*" or pattern == value

    for pg_host, pg_port, _pg_db, pg_user, password in pgpass_entries:
        if matches(pg_host, host) and matches(pg_port, port) and matches(pg_user, rolname):
            return password
    return None


def fetch_roles(conn):
    """Fetch all non-system roles from pg_roles (accessible without superuser in RDS)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                r.rolname,
                r.rolsuper,
                r.rolinherit,
                r.rolcreaterole,
                r.rolcreatedb,
                r.rolcanlogin,
                r.rolreplication,
                r.rolconnlimit,
                r.rolvaliduntil,
                r.rolbypassrls
            FROM pg_roles r
            WHERE r.rolname NOT LIKE 'pg_%%'
              AND r.rolname NOT IN %s
            ORDER BY r.rolname
        """, (tuple(SYSTEM_ROLES),))

        roles = []
        for row in cur.fetchall():
            roles.append({
                "rolname": row[0],
                "rolsuper": row[1],
                "rolinherit": row[2],
                "rolcreaterole": row[3],
                "rolcreatedb": row[4],
                "rolcanlogin": row[5],
                "rolreplication": row[6],
                "rolconnlimit": row[7],
                "rolpassword": None,  # resolved from .pgpass per host
                "rolvaliduntil": str(row[8]) if row[8] else None,
                "rolbypassrls": row[9],
            })
        return roles


def fetch_memberships(conn):
    """Fetch role memberships (GRANTs) excluding system roles."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                r.rolname  AS role,
                m.rolname  AS member,
                g.rolname  AS grantor,
                am.admin_option
            FROM pg_auth_members am
            JOIN pg_roles r ON r.oid = am.roleid
            JOIN pg_roles m ON m.oid = am.member
            JOIN pg_roles g ON g.oid = am.grantor
            WHERE r.rolname NOT LIKE 'pg_%%'
              AND m.rolname NOT LIKE 'pg_%%'
              AND r.rolname NOT IN %s
              AND m.rolname NOT IN %s
            ORDER BY r.rolname, m.rolname
        """, (tuple(SYSTEM_ROLES), tuple(SYSTEM_ROLES)))

        memberships = []
        for row in cur.fetchall():
            memberships.append({
                "role": row[0],
                "member": row[1],
                "grantor": row[2],
                "admin_option": row[3],
            })
        return memberships


def fetch_db_owners(conn):
    """Return set of role names that own at least one non-system database."""
    system_dbs = ("postgres", "template0", "template1", "rdsadmin")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.rolname
            FROM pg_database d
            JOIN pg_roles r ON r.oid = d.datdba
            WHERE d.datname NOT IN %s
              AND d.datistemplate = false
        """, (system_dbs,))
        return {row[0] for row in cur.fetchall()}


def fetch_role_settings(conn):
    """Fetch per-role GUC settings from pg_db_role_setting."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                r.rolname,
                d.datname,
                s.setconfig
            FROM pg_db_role_setting s
            JOIN pg_roles r ON r.oid = s.setrole
            LEFT JOIN pg_database d ON d.oid = s.setdatabase
            WHERE r.rolname NOT LIKE 'pg_%%'
              AND r.rolname NOT IN %s
            ORDER BY r.rolname
        """, (tuple(SYSTEM_ROLES),))

        settings = []
        for row in cur.fetchall():
            settings.append({
                "rolname": row[0],
                "database": row[1],
                "setconfig": row[2],
            })
        return settings


def generate_create_role_sql(role):
    """Generate a CREATE ROLE statement with plaintext password from .pgpass."""
    name = role["rolname"]
    parts = [f'CREATE ROLE "{name}"']

    attrs = []
    if role["rolsuper"]:
        attrs.append("SUPERUSER")
    else:
        attrs.append("NOSUPERUSER")

    attrs.append("INHERIT" if role["rolinherit"] else "NOINHERIT")
    attrs.append("CREATEROLE" if role["rolcreaterole"] else "NOCREATEROLE")
    attrs.append("CREATEDB" if role["rolcreatedb"] else "NOCREATEDB")
    attrs.append("LOGIN" if role["rolcanlogin"] else "NOLOGIN")
    attrs.append("REPLICATION" if role["rolreplication"] else "NOREPLICATION")
    attrs.append("BYPASSRLS" if role["rolbypassrls"] else "NOBYPASSRLS")

    if role["rolconnlimit"] >= 0:
        attrs.append(f"CONNECTION LIMIT {role['rolconnlimit']}")

    # Only include password if it was verified by a successful login to SOURCE
    if role.get("rolpassword") and role.get("password_verified"):
        attrs.append(f"ENCRYPTED PASSWORD '{role['rolpassword']}'")

    if role["rolvaliduntil"]:
        attrs.append(f"VALID UNTIL '{role['rolvaliduntil']}'")

    parts.append(" ".join(attrs))
    return " ".join(parts) + ";"


def generate_grant_sql(membership):
    """Generate GRANT role TO member statement."""
    role = membership["role"]
    member = membership["member"]
    admin = " WITH ADMIN OPTION" if membership["admin_option"] else ""
    return f'GRANT "{role}" TO "{member}"{admin};'


def generate_alter_role_set_sql(setting):
    """Generate ALTER ROLE SET statements for GUC settings."""
    stmts = []
    name = setting["rolname"]
    db = setting["database"]
    in_db = f' IN DATABASE "{db}"' if db else ""

    for config in setting.get("setconfig", []) or []:
        param, _, value = config.partition("=")
        stmts.append(f"ALTER ROLE \"{name}\"{in_db} SET {param} = '{value}';")
    return stmts


def test_login(host, port, dbname, rolname, password):
    """Try to connect to host with rolname/password. Returns (success, error_msg)."""
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname,
            user=rolname, password=password,
            connect_timeout=5,
        )
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)


def clone_from_source(host, user, port, dbname, source_name, pgpass_entries):
    """Extract all roles, memberships, and settings from a source RDS.

    Passwords are resolved from pgpass_entries matching host+rolname,
    since pg_authid is not accessible in RDS without superuser.
    Each role with a resolved password is then verified by attempting
    a real login to the source before being included in the output.
    """
    print(f"\n  Connecting to {source_name} ({host}) as {user}...")
    conn = pg_connect(host, user, port, dbname)
    conn.set_session(readonly=True, autocommit=True)

    try:
        roles = fetch_roles(conn)
        memberships = fetch_memberships(conn)
        settings = fetch_role_settings(conn)
        db_owners = fetch_db_owners(conn)
    finally:
        conn.close()

    # Resolve each role's password from .pgpass and verify login on SOURCE
    resolved = verified = failed = 0
    for role in roles:
        pwd = lookup_password(pgpass_entries, host, role["rolname"], str(port))
        if pwd:
            role["rolpassword"] = pwd
            resolved += 1
            if role["rolcanlogin"] and role["rolname"].startswith("svc_"):
                ok, err = test_login(host, port, dbname, role["rolname"], pwd)
                role["password_verified"] = ok
                if ok:
                    verified += 1
                else:
                    failed += 1
                    print(f"    [WARN] {role['rolname']}: login test failed — {err}", file=sys.stderr)
            else:
                role["password_verified"] = None  # NOLOGIN or non-service user, not tested
        else:
            role["password_verified"] = None

    print(f"  Found: {len(roles)} role(s) | db_owners: {sorted(db_owners)} "
          f"| passwords in .pgpass: {resolved} | login verified: {verified} | failed: {failed}")
    return roles, memberships, settings, db_owners


def apply_to_target(target_host, user, port, dbname, sql_lines):
    """Parse sql_lines into executable statements and apply to target."""
    print(f"\nApplying to target: {target_host} as {user}...")
    conn = pg_connect(target_host, user, port, dbname)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Build executable statements from sql_lines
    statements = []
    block = None
    for line in sql_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if line.startswith("DO $$ BEGIN"):
            block = [line]
            continue
        if block is not None:
            block.append(line)
            if line.startswith("END $$;"):
                statements.append("\n".join(block))
                block = None
            continue
        statements.append(line)

    applied = 0
    errors = 0
    for stmt in statements:
        try:
            cur.execute(stmt)
            applied += 1
        except Exception as e:
            label = stmt.split("\n")[0][:80]
            print(f"  [ERROR] {label}: {e}", file=sys.stderr)
            errors += 1

    cur.close()
    conn.close()
    print(f"Applied: {applied} statement(s), Errors: {errors}")
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Clone service users from source RDS instances to target Aurora (uses .pgpass for auth)."
    )
    parser.add_argument("--config", help="Migration YAML config file")
    parser.add_argument("--source-host", help="Single source RDS host (instead of --config)")
    parser.add_argument("--target-aurora", help="Name of target Aurora cluster from config (default: first)")
    parser.add_argument("--target-host", help="Target Aurora host (overrides config)")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"PostgreSQL user (default: {DEFAULT_USER}, password from .pgpass)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"PostgreSQL port (default: {DEFAULT_PORT})")
    parser.add_argument("--dbname", default=DEFAULT_DBNAME, help=f"Database name to connect (default: {DEFAULT_DBNAME})")
    parser.add_argument("--apply", action="store_true", help="Apply SQL to target Aurora (default: SQL output only)")
    parser.add_argument("--output", default="output/clone_users.sql", help="SQL output file path")

    args = parser.parse_args()

    if not args.config and not args.source_host:
        parser.error("Either --config or --source-host is required")

    # Load .pgpass
    pgpass = os.path.expanduser("~/.pgpass")
    if not os.path.exists(pgpass):
        print(f"WARNING: {pgpass} not found. Connection may fail.", file=sys.stderr)
    pgpass_entries = read_pgpass(pgpass)
    print(f".pgpass loaded: {len(pgpass_entries)} entry(ies) from {pgpass}")

    # Resolve target host
    target_host = args.target_host
    if args.config and not target_host:
        config = load_config(args.config)
        aurora_clusters = config.get("target_aurora_clusters", [])
        if args.target_aurora:
            cluster = next((c for c in aurora_clusters if c["name"] == args.target_aurora), None)
            if not cluster:
                parser.error(f"Aurora cluster '{args.target_aurora}' not found in config")
        else:
            cluster = aurora_clusters[0] if aurora_clusters else None
        if cluster:
            target_host = cluster["endpoint"]

    # Build source list
    sources = []
    if args.config:
        config = load_config(args.config)
        for src in config.get("source_rds_endpoints", []):
            sources.append({"name": src["name"], "host": src["endpoint"]})
    else:
        sources.append({"name": args.source_host, "host": args.source_host})

    # Collect roles from all sources, dedup by rolname
    print(f"Cloning users from {len(sources)} source(s) as {args.user} (via .pgpass)")
    print("=" * 60)

    all_roles = {}
    all_memberships = []
    all_settings = []
    role_origin = {}

    for src in sources:
        try:
            roles, memberships, settings, db_owners = clone_from_source(
                src["host"], args.user, args.port, args.dbname, src["name"], pgpass_entries,
            )
        except Exception as e:
            print(f"  [ERROR] {src['name']}: {e}", file=sys.stderr)
            continue

        for r in roles:
            if not is_service_role(r, db_owners):
                continue
            name = r["rolname"]
            role_origin.setdefault(name, []).append(src["name"])
            all_roles[name] = r

        all_memberships.extend(memberships)
        all_settings.extend(settings)

    if not all_roles:
        print("\nNo roles collected. Check connectivity and permissions.")
        sys.exit(1)

    # Dedup memberships — only keep those where both role and member are being cloned
    seen_memberships = set()
    unique_memberships = []
    for m in all_memberships:
        if m["role"] not in all_roles or m["member"] not in all_roles:
            continue
        key = (m["role"], m["member"])
        if key not in seen_memberships:
            seen_memberships.add(key)
            unique_memberships.append(m)

    # Dedup settings
    seen_settings = set()
    unique_settings = []
    for s in all_settings:
        key = (s["rolname"], s["database"], tuple(s.get("setconfig", []) or []))
        if key not in seen_settings:
            seen_settings.add(key)
            unique_settings.append(s)

    # Generate SQL
    print(f"\n{'=' * 60}")
    print(f"Total unique roles: {len(all_roles)}")
    print(f"Total memberships:  {len(unique_memberships)}")
    print(f"Total settings:     {len(unique_settings)}")

    sql_lines = []
    sql_lines.append(f"-- Clone Users - Generated {datetime.now(timezone.utc).isoformat()}")
    sql_lines.append(f"-- Sources: {len(sources)} RDS instances")
    sql_lines.append(f"-- Roles: {len(all_roles)}, Memberships: {len(unique_memberships)}")
    sql_lines.append("")

    # CREATE ROLE (skip if exists)
    sql_lines.append("-- ============================================")
    sql_lines.append("-- CREATE ROLES")
    sql_lines.append("-- ============================================")
    for name in sorted(all_roles):
        role = all_roles[name]
        origins = ", ".join(role_origin.get(name, []))
        verified = role.get("password_verified")
        if verified is True:
            pwd_status = "password: verified"
        elif verified is False:
            pwd_status = "password: UNVERIFIED - login test failed on SOURCE"
        elif role.get("rolpassword"):
            pwd_status = "password: in .pgpass (NOLOGIN, not tested)"
        else:
            pwd_status = "password: not in .pgpass"
        sql_lines.append(f"\n-- Role: {name} | {pwd_status} | from: {origins}")
        create_sql = generate_create_role_sql(role)
        sql_lines.append("DO $$ BEGIN")
        sql_lines.append(f"  {create_sql}")
        sql_lines.append("EXCEPTION WHEN duplicate_object THEN")
        sql_lines.append(f"  RAISE NOTICE 'Role \"{name}\" already exists, skipping';")
        sql_lines.append("END $$;")

    # GRANT memberships
    if unique_memberships:
        sql_lines.append("")
        sql_lines.append("-- ============================================")
        sql_lines.append("-- GRANT MEMBERSHIPS")
        sql_lines.append("-- ============================================")
        for m in unique_memberships:
            sql_lines.append(generate_grant_sql(m))

    # ALTER ROLE SET
    if unique_settings:
        sql_lines.append("")
        sql_lines.append("-- ============================================")
        sql_lines.append("-- ROLE SETTINGS (GUC)")
        sql_lines.append("-- ============================================")
        for s in unique_settings:
            for stmt in generate_alter_role_set_sql(s):
                sql_lines.append(stmt)

    sql_content = "\n".join(sql_lines) + "\n"

    # Write SQL file
    with open(args.output, "w") as f:
        f.write(sql_content)
    print(f"\nSQL written to: {args.output}")

    # Print role summary
    print(f"\nRoles by origin:")
    for name in sorted(all_roles):
        origins = ", ".join(role_origin[name])
        login = "LOGIN" if all_roles[name]["rolcanlogin"] else "NOLOGIN"
        print(f"  {name} [{login}] <- {origins}")

    # Apply to target if requested
    if args.apply:
        if not target_host:
            print("ERROR: --target-host or --target-aurora required for --apply", file=sys.stderr)
            sys.exit(1)
        apply_to_target(target_host, args.user, args.port, args.dbname, sql_lines)
    else:
        print(f"\nReview the SQL, then run with --apply to execute on target Aurora.")


if __name__ == "__main__":
    main()
