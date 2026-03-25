#!/usr/bin/env python3
"""Update ~/.pgpass with Aurora endpoint and CNAME entries for all service accounts.

For each database in the TARGET Aurora (derived from config):
  - Discovers the DB owner (service user)
  - Looks up the password from the existing pgpass (wildcard or specific entry)
  - Generates entries for every Aurora host (endpoint + CNAMEs) x every service user

Wraps entries in a labeled section so re-runs replace instead of duplicate.

Usage:
    python scripts/update_pgpass.py --config config/migration.yaml

    # Dry-run (print what would be written)
    python scripts/update_pgpass.py --config config/migration.yaml --dry-run

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import psycopg2
import yaml


PGPASS_DEFAULT = os.path.expanduser("~/.pgpass")
CONNECT_USER   = "svc_claude"
DEFAULT_PORT   = 5432
SYSTEM_DBS     = {"postgres", "template0", "template1", "rdsadmin"}


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def read_pgpass(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return f.readlines()


def get_password(pgpass_lines, username):
    """Find password for a user. Checks specific entries first, then wildcards."""
    wildcard_pwd = None
    for line in pgpass_lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        parts = stripped.split(":")
        if len(parts) < 5:
            continue
        host, port, db, user = parts[0], parts[1], parts[2], parts[3]
        password = ":".join(parts[4:])
        if user != username:
            continue
        if host == "*" and port == "*" and db == "*":
            wildcard_pwd = password  # keep looking for more specific
        else:
            return password  # specific entry takes priority
    return wildcard_pwd


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def list_source_databases(host, port, user):
    """Return [(datname, owner)] from a SOURCE RDS instance."""
    conn = pg_connect(host, port, "postgres", user)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.datname, r.rolname AS owner
                FROM pg_database d
                JOIN pg_roles r ON r.oid = d.datdba
                WHERE d.datname NOT IN %s
                  AND d.datistemplate = false
                ORDER BY d.datname
            """, (tuple(SYSTEM_DBS),))
            return cur.fetchall()
    finally:
        conn.close()


def collect_config_databases(config, port, user):
    """Return sorted unique [(datname, owner)] across all SOURCE instances in config."""
    seen = {}
    for src in config.get("source_rds_endpoints", []):
        host = src["endpoint"]
        name = src["name"]
        try:
            rows = list_source_databases(host, port, user)
            for dbname, owner in rows:
                seen.setdefault(dbname, owner)  # first source wins
        except Exception as e:
            print(f"  WARN: could not list databases from {name}: {e}", file=sys.stderr)
    return sorted(seen.items())


def build_entries(config_path, clusters, db_entries, pgpass_lines):
    """Build pgpass lines to append.

    Returns (lines, warns).
    """
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    config = os.path.splitext(os.path.basename(config_path))[0]
    lines  = [f"\n# UPDATE {now} — {config}\n"]
    warns  = []

    # Collect all hosts per cluster
    clusters_hosts = []
    for cluster in clusters:
        hosts = [cluster["endpoint"]] + (cluster.get("cnames") or [])
        clusters_hosts.append(hosts)

    entries = []
    for dbname, svc_user in sorted(db_entries):
        pwd = get_password(pgpass_lines, svc_user)
        if not pwd:
            warns.append(f"  WARN: no password found for {svc_user} (db={dbname}) — skipped")
            continue
        for hosts in clusters_hosts:
            for host in hosts:
                entries.append(f"{host}:{DEFAULT_PORT}:{dbname}:{svc_user}:{pwd}\n")

    entries.sort()
    lines.extend(entries)

    return lines, warns


def main():
    parser = argparse.ArgumentParser(
        description="Update ~/.pgpass with Aurora entries for all service accounts."
    )
    parser.add_argument("--config", default="config/migration.yaml",
                        help="Migration config (default: config/migration.yaml)")
    parser.add_argument("--pgpass", default=PGPASS_DEFAULT,
                        help=f"Path to pgpass file (default: {PGPASS_DEFAULT})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config   = load_config(args.config)
    clusters = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)

    target_host   = clusters[0]["endpoint"]
    pgpass_lines  = read_pgpass(args.pgpass)

    print(f"Config  : {args.config}")
    print(f"Target  : {target_host}")
    print(f"Pgpass  : {args.pgpass}")
    if args.dry_run:
        print(f"Mode    : dry-run")
    print()

    # Discover databases and their owners from SOURCE instances in config
    print("Discovering databases from SOURCE RDS instances in config...")
    db_entries = collect_config_databases(config, args.port, CONNECT_USER)
    if not db_entries:
        print("ERROR: no databases found across source RDS instances", file=sys.stderr)
        sys.exit(1)

    for dbname, owner in db_entries:
        pwd = get_password(pgpass_lines, owner)
        status = "password found" if pwd else "NO PASSWORD in pgpass"
        print(f"  {dbname:<35} owner={owner:<45} {status}")

    print()
    new_lines, warns = build_entries(
        args.config, clusters, db_entries, pgpass_lines
    )

    total_hosts   = sum(1 + len(c.get("cnames") or []) for c in clusters)
    total_entries = sum(1 for l in new_lines if not l.startswith("#") and l.strip())
    print(f"Clusters : {len(clusters)}")
    print(f"Hosts    : {total_hosts} (endpoints + CNAMEs)")
    print(f"Databases: {len(db_entries)}")
    print(f"Entries  : {total_entries}")

    for w in warns:
        print(w, file=sys.stderr)

    if args.dry_run:
        print("\n--- DRY RUN: entries that would be appended ---")
        print("".join(new_lines), end="")
        return

    with open(args.pgpass, "a") as f:
        f.writelines(new_lines)
    os.chmod(args.pgpass, 0o600)

    print(f"\nDone — {total_entries} entries appended to {args.pgpass}")

    # Load peers report to resolve source host per database
    peers_report = "output/peerdb_peers.json"
    src_host_by_db = {}
    try:
        import json
        with open(peers_report) as f:
            peers_data = json.load(f)
        for p in peers_data.get("peers", []):
            if p["kind"] == "source":
                src_host_by_db[p["database"]] = p["host"]
    except FileNotFoundError:
        pass

    # Verify connections — SOURCE and TARGET
    print("\nVerifying connections...")
    fresh_pgpass = read_pgpass(args.pgpass)
    primary_host = clusters[0]["endpoint"]

    CW = {"db": 35, "user": 45, "auth": 8}
    sep    = "+-" + "-+-".join("-" * w for w in [CW["db"], CW["user"], CW["auth"], CW["auth"]]) + "-+"
    header = ("| " + " | ".join([
        f"{'DATABASE':<{CW['db']}}",
        f"{'USERNAME':<{CW['user']}}",
        f"{'SRC AUTH':^{CW['auth']}}",
        f"{'TGT AUTH':^{CW['auth']}}",
    ]) + " |")

    print(sep)
    print(header)
    print(sep)

    conn_ok = conn_fail = 0
    for dbname, svc_user in sorted(db_entries):
        pwd = get_password(fresh_pgpass, svc_user)
        if not pwd:
            continue

        # SOURCE check
        src_host = src_host_by_db.get(dbname, "")
        if src_host:
            try:
                c = psycopg2.connect(host=src_host, port=args.port, dbname=dbname,
                                     user=svc_user, password=pwd, connect_timeout=5)
                c.close()
                src_status = "OK"
            except Exception:
                src_status = "FAIL"
        else:
            src_status = "N/A"

        # TARGET check
        try:
            c = psycopg2.connect(host=primary_host, port=args.port, dbname=dbname,
                                 user=svc_user, password=pwd, connect_timeout=5)
            c.close()
            tgt_status = "OK"
            conn_ok += 1
        except Exception:
            tgt_status = "FAIL"
            conn_fail += 1

        print("| " + " | ".join([
            f"{dbname:<{CW['db']}}",
            f"{svc_user:<{CW['user']}}",
            f"{src_status:^{CW['auth']}}",
            f"{tgt_status:^{CW['auth']}}",
        ]) + " |")

    print(sep)
    print(f"\n  OK  : {conn_ok}")
    print(f"  FAIL: {conn_fail}")
    if conn_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
