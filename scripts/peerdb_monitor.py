#!/usr/bin/env python3
"""PeerDB - Monitor dashboard.

Shows per mirror:
  - TARGET database
  - PeerDB mirror status
  - SOURCE replication slot lag (bytes)
  - SOURCE connections by user
  - TARGET connections by user

Usage:
    python scripts/peerdb_monitor.py

    # Refresh every N seconds
    python scripts/peerdb_monitor.py --watch 10

Environment variables:
    PEERDB_API_URL        e.g. http://10.210.13.211:3000
    PEERDB_AUTH_HEADER    e.g. Authorization: Basic OnBlZXJkYg==
"""

import argparse
import json
import os


def _load_dotenv():
    """Load .env from project root (two levels up from this script) into os.environ."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

_load_dotenv()

import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
PEERS_REPORT = "output/peerdb_peers.json"

# Slot name convention (matches peerdb_create_mirrors.py)
def slot_name_for(dbname):
    return f"aurora_slot_{dbname}_001"


def peerdb_session(api_url, auth_header):
    session = requests.Session()
    if ":" in auth_header:
        name, value = auth_header.split(":", 1)
        session.headers.update({name.strip(): value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


def config_mirror_names(config, peers_data):
    """Return set of mirror names that belong to sources defined in the config."""
    source_endpoints = {src["endpoint"] for src in config.get("source_rds_endpoints", [])}
    databases = {
        p["database"]
        for p in peers_data.get("peers", [])
        if p.get("kind") == "source" and p.get("host") in source_endpoints
    }
    return {f"mirror_{db}_001" for db in databases}


def list_mirrors(session):
    resp = session.get(f"{session.base_url}/api/v1/mirrors/list")
    resp.raise_for_status()
    return resp.json().get("mirrors", [])


def get_mirror_state(session, flow_job_name):
    try:
        resp = session.post(
            f"{session.base_url}/api/v1/mirrors/status",
            json={"flowJobName": flow_job_name},
        )
        if resp.status_code not in (200, 201):
            return "UNKNOWN"
        return resp.json().get("currentFlowState", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=5,
    )


def get_slot_lag(host, port, user, slot_name):
    """Returns (lag_bytes, lag_pretty) or (None, 'N/A')."""
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_pretty
                FROM pg_replication_slots
                WHERE slot_name = %s
            """, (slot_name,))
            row = cur.fetchone()
        conn.close()
        if row:
            return row[0], row[1]
        return None, "no slot"
    except Exception as e:
        return None, f"err"


def get_connections(host, port, user, dbname):
    """Returns list of (usename, count) sorted by count desc."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT usename, count(*)::int
                FROM pg_stat_activity
                WHERE datname = %s
                GROUP BY usename
                ORDER BY count(*) DESC
            """, (dbname,))
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def fmt_connections(rows):
    """Returns list of strings, one per user."""
    if not rows:
        return ["none"]
    return [f"{u}:{n}" for u, n in rows]


def fmt_state(state):
    short = {
        "STATUS_RUNNING":    "RUNNING",
        "STATUS_SNAPSHOT":   "SNAPSHOT",
        "STATUS_PAUSED":     "PAUSED",
        "STATUS_TERMINATED": "TERMINATED",
        "STATUS_SETUP":      "SETUP",
        "UNKNOWN":           "UNKNOWN",
    }
    return short.get(state, state.replace("STATUS_", ""))


STATE_COLOR = {
    "STATUS_RUNNING":    "\033[32m",   # green
    "STATUS_SNAPSHOT":   "\033[36m",   # cyan
    "STATUS_PAUSED":     "\033[33m",   # yellow
    "STATUS_TERMINATED": "\033[31m",   # red
    "UNKNOWN":           "\033[90m",   # gray
}
RESET = "\033[0m"


def colorize(state, text):
    color = STATE_COLOR.get(state, "")
    return f"{color}{text}{RESET}" if color else text


def render(session, peers_by_db, allowed_mirrors, user, port, use_color):
    mirrors = [m for m in list_mirrors(session) if m["name"] in allowed_mirrors]

    CW = {
        "db":     24,
        "status": 10,
        "lag":    10,
        "conn":   30,
    }

    def pad(text, width, align="left"):
        """Pad plain text to width, ignoring ANSI escape codes."""
        import re
        plain = re.sub(r"\033\[[0-9;]*m", "", text)
        pad_n = max(0, width - len(plain))
        if align == "right":
            return " " * pad_n + text
        return text + " " * pad_n

    def fmt_row(db, status, lag, src, tgt):
        return (
            f"| {pad(db, CW['db'])} "
            f"| {pad(status, CW['status'])} "
            f"| {pad(lag, CW['lag'], 'right')} "
            f"| {pad(src, CW['conn'])} "
            f"| {pad(tgt, CW['conn'])} |"
        )

    header = fmt_row("DATABASE", "STATUS", "SLOT LAG", "SRC CONNECTIONS", "TGT CONNECTIONS")
    sep    = "+" + "+".join("-" * (w + 2) for w in [CW["db"], CW["status"], CW["lag"], CW["conn"], CW["conn"]]) + "+"

    def data_lines(dbname, state_str, lag_p, src_lines, tgt_lines):
        n = max(len(src_lines), len(tgt_lines), 1)
        for i in range(n):
            yield fmt_row(
                dbname    if i == 0 else "",
                state_str if i == 0 else "",
                lag_p     if i == 0 else "",
                src_lines[i] if i < len(src_lines) else "",
                tgt_lines[i] if i < len(tgt_lines) else "",
            )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\nPeerDB Monitor  [{now}]  mirrors={len(mirrors)}")
    print(sep)
    print(header)
    print(sep)

    for mirror in sorted(mirrors, key=lambda m: m["name"]):
        name        = mirror["name"]
        source_peer = mirror["sourceName"]

        src      = peers_by_db.get(source_peer, {})
        src_host = src.get("host", "")
        dbname   = src.get("database", "")
        tgt_host = src.get("target_host", "")

        state     = get_mirror_state(session, name)
        state_str = fmt_state(state)
        if use_color:
            state_str = colorize(state, state_str)

        slot     = slot_name_for(dbname)
        _, lag_p = get_slot_lag(src_host, port, user, slot) if src_host else (None, "N/A")

        src_lines = fmt_connections(get_connections(src_host, port, user, dbname) if src_host else [])
        tgt_lines = fmt_connections(get_connections(tgt_host, port, user, dbname) if tgt_host else [])

        for line in data_lines(dbname or name, state_str, lag_p, src_lines, tgt_lines):
            print(line)
        print(sep)


def main():
    parser = argparse.ArgumentParser(description="PeerDB mirror monitoring dashboard.")
    parser.add_argument("--config", default="config/migration.yaml",
                        help="Migration config file (default: config/migration.yaml)")
    parser.add_argument("--watch", type=int, metavar="SECONDS",
                        help="Refresh interval in seconds (default: run once)")
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--peers-report", default=PEERS_REPORT)
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--api-url", default=os.environ.get("PEERDB_API_URL", ""))
    parser.add_argument("--auth-header", default=os.environ.get("PEERDB_AUTH_HEADER", ""))
    args = parser.parse_args()

    if not args.api_url:
        print("ERROR: --api-url or PEERDB_API_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_header:
        print("ERROR: --auth-header or PEERDB_AUTH_HEADER is required", file=sys.stderr)
        sys.exit(1)

    with open(args.config) as f:
        config = yaml.safe_load(f)

    try:
        with open(args.peers_report) as f:
            peers_data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {args.peers_report} not found", file=sys.stderr)
        sys.exit(1)

    allowed_mirrors = config_mirror_names(config, peers_data)

    # Build lookup: source_peer_name -> {host, database, target_host}
    # First pass: collect target hosts per (rds_source, database)
    target_hosts = {}
    for p in peers_data.get("peers", []):
        if p["kind"] == "target":
            target_hosts[(p["rds_source"], p["database"])] = p["host"]

    peers_by_db = {}
    for p in peers_data.get("peers", []):
        if p["kind"] == "source":
            key = (p["rds_source"], p["database"])
            peers_by_db[p["peer_name"]] = {
                "host":        p["host"],
                "database":    p["database"],
                "target_host": target_hosts.get(key, ""),
            }

    session  = peerdb_session(args.api_url, args.auth_header)
    use_color = not args.no_color and sys.stdout.isatty()

    if args.watch:
        try:
            while True:
                if use_color:
                    print("\033[2J\033[H", end="")  # clear screen
                render(session, peers_by_db, allowed_mirrors, args.user, args.port, use_color)
                print(f"\n  Refreshing every {args.watch}s — Ctrl+C to stop")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        render(session, peers_by_db, allowed_mirrors, args.user, args.port, use_color)


if __name__ == "__main__":
    main()
