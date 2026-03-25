#!/usr/bin/env python3
"""Monitor connections SOURCE vs TARGET — per database, user and status.

Shows a side-by-side table per database:
  ENDPOINT | DATABASE | USER | STATE | SOURCE | TARGET

Usage:
    python scripts/monitor_connections.py --config config/migration_microservices1_1.yaml

    # Auto-refresh every N seconds
    python scripts/monitor_connections.py --config config/migration_microservices1_1.yaml --watch 10

    # Single database
    python scripts/monitor_connections.py --config config/migration_microservices1_1.yaml --database backoffice_adjustments

Environment:
    Uses ~/.pgpass for authentication (user: svc_claude)
"""

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
SYSTEM_DBS   = {"postgres", "template0", "template1", "rdsadmin"}


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=5,
    )


def fetch_connections(host, port, user, dbname):
    """Return {(usename, state): count} for the given database."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    usename,
                    COALESCE(state, 'unknown') AS state,
                    count(*)::int
                FROM pg_stat_activity
                WHERE datname = %s
                GROUP BY usename, state
                ORDER BY usename, state
            """, (dbname,))
            rows = cur.fetchall()
        conn.close()
        return {(row[0], row[1]): row[2] for row in rows}
    except Exception as e:
        return {"_error": str(e)}


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


def render(config, user, port, db_filter, use_color):
    sources = config.get("source_rds_endpoints", [])
    target  = config.get("target_aurora_clusters", [{}])[0]
    tgt_host = target["endpoint"]
    tgt_name = target.get("name", tgt_host)

    # Column widths
    CW = {"host": 44, "db": 28, "user": 24, "state": 24, "src": 8, "tgt": 8}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(host, db, usr, state, src, tgt):
        return (
            f"| {host:<{CW['host']}} "
            f"| {db:<{CW['db']}} "
            f"| {usr:<{CW['user']}} "
            f"| {state:<{CW['state']}} "
            f"| {str(src):^{CW['src']}} "
            f"| {str(tgt):^{CW['tgt']}} |"
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\nConnection Monitor  [{now}]")
    print(sep)
    print(row("SOURCE ENDPOINT", "DATABASE", "USER", "STATE", "SOURCE", "TARGET"))
    print(sep)

    total_src = total_tgt = 0

    for src in sources:
        src_host = src["endpoint"]
        src_name = src["name"]

        try:
            databases = list_databases(src_host, port, user)
        except Exception as e:
            print(row(src_name[:CW["host"]], f"ERROR: {e}"[:CW["db"]], "-", "-", "-", "-"))
            print(sep)
            continue

        if db_filter:
            databases = [d for d in databases if d == db_filter]

        for dbname in databases:
            src_conns = fetch_connections(src_host, port, user, dbname)
            tgt_conns = fetch_connections(tgt_host, port, user, dbname)

            if "_error" in src_conns and "_error" in tgt_conns:
                print(row(src_name[:CW["host"]], dbname[:CW["db"]], "ERROR", "-", "-", "-"))
                print(sep)
                continue

            # Build unified set of (user, state) keys
            keys = sorted(set(src_conns.keys()) | set(tgt_conns.keys()))
            if not keys:
                print(row(src_name[:CW["host"]], dbname[:CW["db"]], "(no connections)", "", "-", "-"))
                print(sep)
                continue

            for i, (uname, state) in enumerate(keys):
                src_count = src_conns.get((uname, state), 0)
                tgt_count = tgt_conns.get((uname, state), 0)
                total_src += src_count
                total_tgt += tgt_count

                host_col = src_name[:CW["host"]] if i == 0 else ""
                db_col   = dbname[:CW["db"]]     if i == 0 else ""

                src_val = src_count if src_count else "-"
                tgt_val = tgt_count if tgt_count else "-"

                # Colorize non-zero TARGET connections (means apps still hitting old RDS)
                if use_color and src_count > 0:
                    src_str = f"\033[33m{src_count}\033[0m"
                elif src_count:
                    src_str = str(src_count)
                else:
                    src_str = "-"

                if use_color and tgt_count > 0:
                    tgt_str = f"\033[32m{tgt_count}\033[0m"
                elif tgt_count:
                    tgt_str = str(tgt_count)
                else:
                    tgt_str = "-"

                print(row(host_col, db_col,
                          str(uname)[:CW["user"]],
                          state[:CW["state"]],
                          src_str, tgt_str))
            print(sep)

    print(f"  TOTAL CONNECTIONS — SOURCE: {total_src}  |  TARGET: {total_tgt}")


def main():
    parser = argparse.ArgumentParser(
        description="Show connections SOURCE vs TARGET per database, user and state."
    )
    parser.add_argument("--config",   required=True)
    parser.add_argument("--user",     default=DEFAULT_USER)
    parser.add_argument("--port",     type=int, default=DEFAULT_PORT)
    parser.add_argument("--database", help="Filter to a single database")
    parser.add_argument("--watch",    type=int, metavar="SECONDS",
                        help="Refresh interval in seconds")
    parser.add_argument("--no-color", action="store_true")
    args = parser.parse_args()

    config    = load_config(args.config)
    use_color = not args.no_color and sys.stdout.isatty()

    if args.watch:
        try:
            while True:
                if use_color:
                    print("\033[2J\033[H", end="")
                render(config, args.user, args.port, args.database, use_color)
                print(f"\n  Refreshing every {args.watch}s — Ctrl+C to stop")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        render(config, args.user, args.port, args.database, use_color)


if __name__ == "__main__":
    main()
