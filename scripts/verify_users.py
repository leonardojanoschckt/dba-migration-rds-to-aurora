#!/usr/bin/env python3
"""Verify service user connectivity on SOURCE RDS and TARGET Aurora.

For each source RDS in migration.yaml, finds all svc_* users with a password
in .pgpass and tests login against both SOURCE and TARGET. Produces a
per-user/per-host report and exits non-zero if any TARGET check fails.

Usage:
    python scripts/verify_users.py --config config/migration.yaml
    python scripts/verify_users.py --config config/migration.yaml --output output/verify_users.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import psycopg2
import yaml


DEFAULT_PORT = 5432
DEFAULT_DBNAME = "postgres"
DEFAULT_OUTPUT = "output/verify_users.json"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def read_pgpass(pgpass_path):
    entries = []
    try:
        with open(pgpass_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) < 5:
                    continue
                host, port, db, user = parts[0], parts[1], parts[2], parts[3]
                password = ":".join(parts[4:])
                entries.append((host, port, db, user, password))
    except FileNotFoundError:
        pass
    return entries


def lookup_password(pgpass_entries, host, rolname, port="5432"):
    def matches(pattern, value):
        return pattern == "*" or pattern == value

    for pg_host, pg_port, _pg_db, pg_user, password in pgpass_entries:
        if matches(pg_host, host) and matches(pg_port, port) and matches(pg_user, rolname):
            return password
    return None


def test_login(host, port, dbname, user, password):
    """Returns (ok: bool, error: str | None)."""
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname,
            user=user, password=password,
            connect_timeout=5,
        )
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)


def get_service_users_for_host(pgpass_entries, host, port="5432"):
    """Return all svc_* users that have a .pgpass entry for this host."""
    seen = {}
    for pg_host, pg_port, _pg_db, pg_user, password in pgpass_entries:
        def matches(pattern, value):
            return pattern == "*" or pattern == value

        if pg_user.startswith("svc_") and matches(pg_host, host) and matches(pg_port, port):
            if pg_user not in seen:
                seen[pg_user] = password
    return seen


def main():
    parser = argparse.ArgumentParser(
        description="Verify svc_* user connectivity on SOURCE RDS and TARGET Aurora."
    )
    parser.add_argument("--config", required=True, help="Migration YAML config file")
    parser.add_argument("--target-aurora", help="Name of target Aurora cluster (default: first)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--dbname", default=DEFAULT_DBNAME)
    parser.add_argument("--pgpass", default="~/.pgpass")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    import os
    pgpass_path = os.path.expanduser(args.pgpass)
    pgpass_entries = read_pgpass(pgpass_path)
    print(f".pgpass loaded: {len(pgpass_entries)} entry(ies)")

    config = load_config(args.config)
    sources = config.get("source_rds_endpoints", [])
    aurora_clusters = config.get("target_aurora_clusters", [])

    if args.target_aurora:
        cluster = next((c for c in aurora_clusters if c["name"] == args.target_aurora), None)
        if not cluster:
            print(f"ERROR: Aurora cluster '{args.target_aurora}' not found in config", file=sys.stderr)
            sys.exit(1)
    else:
        cluster = aurora_clusters[0] if aurora_clusters else None

    if not cluster:
        print("ERROR: No target Aurora cluster found in config", file=sys.stderr)
        sys.exit(1)

    target_host = cluster["endpoint"]
    target_name = cluster["name"]
    port = str(args.port)

    print(f"Target Aurora: {target_name}")
    print(f"  {target_host}")
    print(f"Sources: {len(sources)} RDS instances")
    print("=" * 70)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target": {"name": target_name, "endpoint": target_host},
        "results": [],
        "summary": {"total": 0, "source_ok": 0, "target_ok": 0, "target_failed": 0},
    }

    grand_source_ok = grand_source_fail = 0
    grand_target_ok = grand_target_fail = 0

    for src in sources:
        src_host = src["endpoint"]
        src_name = src["name"]
        users = get_service_users_for_host(pgpass_entries, src_host, port)

        if not users:
            print(f"\n{src_name}: no svc_* users in .pgpass — skipped")
            continue

        print(f"\n{src_name} ({len(users)} user(s))")

        for username, password in sorted(users.items()):
            src_ok, src_err = test_login(src_host, args.port, args.dbname, username, password)
            tgt_ok, tgt_err = test_login(target_host, args.port, args.dbname, username, password)

            src_icon = "OK" if src_ok else "FAIL"
            tgt_icon = "OK" if tgt_ok else "FAIL"
            print(f"  {username:<45} SOURCE: {src_icon:<4}  TARGET: {tgt_icon}")
            if not src_ok:
                print(f"    [SOURCE ERROR] {src_err}")
            if not tgt_ok:
                print(f"    [TARGET ERROR] {tgt_err}")

            if src_ok:
                grand_source_ok += 1
            else:
                grand_source_fail += 1
            if tgt_ok:
                grand_target_ok += 1
            else:
                grand_target_fail += 1

            report["results"].append({
                "source_name": src_name,
                "source_host": src_host,
                "user": username,
                "source_ok": src_ok,
                "source_error": src_err,
                "target_ok": tgt_ok,
                "target_error": tgt_err,
            })

    total = grand_source_ok + grand_source_fail
    report["summary"] = {
        "total_checks": total,
        "source_ok": grand_source_ok,
        "source_failed": grand_source_fail,
        "target_ok": grand_target_ok,
        "target_failed": grand_target_fail,
    }

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total checks : {total}")
    print(f"  Source OK    : {grand_source_ok}  /  Failed: {grand_source_fail}")
    print(f"  Target OK    : {grand_target_ok}  /  Failed: {grand_target_fail}")

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport written to: {args.output}")

    if grand_target_fail > 0:
        print(f"\nERROR: {grand_target_fail} user(s) cannot connect to TARGET — review before cutover.", file=sys.stderr)
        sys.exit(1)

    print("\nAll TARGET checks passed.")


if __name__ == "__main__":
    main()
