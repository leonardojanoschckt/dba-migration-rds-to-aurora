#!/usr/bin/env python3
"""PeerDB - Drop all mirrors and delete their databases on Aurora target.

For each mirror in PeerDB:
  1. Terminates the mirror (dropMirrorStats=True)
  2. Waits for termination
  3. Drops the replication slot on the source RDS
  4. Drops the database on the Aurora target

Usage:
    python scripts/peerdb_drop_all.py

    # Dry-run (no changes)
    python scripts/peerdb_drop_all.py --dry-run

    # Drop only a specific mirror
    python scripts/peerdb_drop_all.py --mirror mirror_backoffice_adjustments_001

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


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def peerdb_session(api_url, auth_header):
    session = requests.Session()
    if ":" in auth_header:
        name, value = auth_header.split(":", 1)
        session.headers.update({name.strip(): value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


def list_mirrors(session):
    resp = session.get(f"{session.base_url}/api/v1/mirrors/list")
    resp.raise_for_status()
    return resp.json().get("mirrors", [])


def config_mirror_names(config, peers_data):
    """Return set of mirror names that belong to sources defined in the config."""
    source_endpoints = {src["endpoint"] for src in config.get("source_rds_endpoints", [])}
    databases = {
        p["database"]
        for p in peers_data.get("peers", [])
        if p.get("kind") == "source" and p.get("host") in source_endpoints
    }
    return {f"mirror_{db}_001" for db in databases}


def get_mirror_state(session, flow_job_name):
    resp = session.post(
        f"{session.base_url}/api/v1/mirrors/status",
        json={"flowJobName": flow_job_name},
    )
    if resp.status_code == 404:
        return "STATUS_UNKNOWN"
    if resp.status_code not in (200, 201):
        return "STATUS_UNKNOWN"
    return resp.json().get("currentFlowState", "STATUS_UNKNOWN")


def drop_mirror(session, flow_job_name, dry_run=False):
    """Send terminate + dropMirrorStats. Returns (ok, message)."""
    if dry_run:
        return True, "dry-run"
    resp = session.post(
        f"{session.base_url}/api/v1/mirrors/state_change",
        json={
            "flowJobName": flow_job_name,
            "requestedFlowState": 6,
            "dropMirrorStats": True,
            "skipDestinationDrop": False,
        },
    )
    if resp.status_code in (200, 201):
        return True, resp.json()
    return False, f"HTTP {resp.status_code}: {resp.text[:300]}"


def wait_terminated(session, flow_job_name, timeout=300, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = get_mirror_state(session, flow_job_name)
        if state in ("STATUS_TERMINATED", "STATUS_UNKNOWN"):
            return True, state
        print(f"      waiting (state={state})...")
        time.sleep(interval)
    return False, "TIMEOUT"


def drop_slot(host, port, user, dbname, slot_name, dry_run=False):
    """Drop replication slot on source. Returns (ok, message)."""
    if dry_run:
        return True, "dry-run"
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(%s) "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name, slot_name),
            )
        conn.close()
        return True, "dropped"
    except Exception as e:
        return False, str(e)


def drop_database(host, port, user, dbname, dry_run=False):
    """Drop database on target Aurora. Returns (ok, message)."""
    if dry_run:
        return True, "dry-run"
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            # Terminate all active connections to the database
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
            """, (dbname,))

            # Wait until all connections are gone (Aurora needs a moment)
            for attempt in range(20):
                cur.execute("""
                    SELECT count(*) FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                """, (dbname,))
                active = cur.fetchone()[0]
                if active == 0:
                    break
                print(f"      waiting for {active} connection(s) to close...")
                time.sleep(2)
                # Re-terminate any new connections that slipped in
                cur.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                """, (dbname,))

            cur.execute(f'ALTER DATABASE "{dbname}" OWNER TO "{user}"')
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        conn.close()
        return True, "dropped"
    except Exception as e:
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(
        description="Drop all PeerDB mirrors and their Aurora target databases."
    )
    parser.add_argument("--config", default="config/migration.yaml",
                        help="Migration config file (default: config/migration.yaml)")
    parser.add_argument("--mirror", help="Only drop this specific mirror by name")
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--peers-report", default=PEERS_REPORT)
    parser.add_argument("--api-url", default=os.environ.get("PEERDB_API_URL", ""))
    parser.add_argument("--auth-header", default=os.environ.get("PEERDB_AUTH_HEADER", ""))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.api_url:
        print("ERROR: --api-url or PEERDB_API_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_header:
        print("ERROR: --auth-header or PEERDB_AUTH_HEADER is required", file=sys.stderr)
        sys.exit(1)

    # Load peers report to resolve source host + dbname per mirror
    try:
        with open(args.peers_report) as f:
            peers_data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {args.peers_report} not found — run peerdb_create_peers.py first", file=sys.stderr)
        sys.exit(1)

    # Load config for Aurora target host
    with open(args.config) as f:
        config = yaml.safe_load(f)
    aurora_clusters = config.get("target_aurora_clusters", [])
    target_host = aurora_clusters[0]["endpoint"] if aurora_clusters else None
    if not target_host:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)

    # Build lookup: peer_name -> {host, database}
    peer_info = {p["peer_name"]: p for p in peers_data.get("peers", [])}

    session = peerdb_session(args.api_url, args.auth_header)

    print(f"PeerDB Drop All Mirrors")
    print(f"API    : {args.api_url}")
    print(f"Target : {target_host}")
    if args.dry_run:
        print("MODE   : dry-run")
    print("=" * 70)

    allowed = config_mirror_names(config, peers_data)
    mirrors = [m for m in list_mirrors(session) if m["name"] in allowed]
    if args.mirror:
        mirrors = [m for m in mirrors if m["name"] == args.mirror]
        if not mirrors:
            print(f"ERROR: mirror '{args.mirror}' not found in PeerDB", file=sys.stderr)
            sys.exit(1)

    print(f"Mirrors found: {len(mirrors)}\n")

    summary = {"dropped": 0, "failed": 0}

    for mirror in mirrors:
        flow_name   = mirror["name"]
        source_peer = mirror["sourceName"]

        # Resolve source host + dbname from peers report
        src = peer_info.get(source_peer, {})
        src_host = src.get("host", "")
        dbname   = src.get("database", "")

        # Slot name follows the convention in peerdb_create_mirrors.py
        slot_name = f"aurora_slot_{dbname}_001"

        print(f"[ {flow_name} ]")
        print(f"  Source peer : {source_peer}")
        print(f"  Database    : {dbname}")
        print(f"  Slot        : {slot_name}")

        # Step 1: terminate mirror
        state = get_mirror_state(session, flow_name)
        if state in ("STATUS_TERMINATED", "STATUS_UNKNOWN"):
            print(f"  Mirror      : already terminated ({state})")
        else:
            print(f"  Mirror      : terminating (current={state})...")
            ok, msg = drop_mirror(session, flow_name, dry_run=args.dry_run)
            if not ok:
                print(f"  ERROR terminating mirror: {msg}", file=sys.stderr)
                summary["failed"] += 1
                print()
                continue
            ok_term, term_state = wait_terminated(session, flow_name)
            if not ok_term:
                print(f"  ERROR: mirror did not terminate (state={term_state})", file=sys.stderr)
                summary["failed"] += 1
                print()
                continue
            print(f"  Mirror      : terminated ({term_state})")

        # Step 2: drop replication slot on source
        if src_host and dbname:
            ok, msg = drop_slot(src_host, args.port, args.user, dbname, slot_name, dry_run=args.dry_run)
            if ok:
                print(f"  Slot        : {msg}")
            else:
                print(f"  Slot        : WARN — {msg}", file=sys.stderr)
        else:
            print(f"  Slot        : SKIP — source info not found in peers report", file=sys.stderr)

        # Step 3: drop database on Aurora target
        if dbname:
            print(f"  DB drop     : dropping {dbname} on Aurora...")
            ok, msg = drop_database(target_host, args.port, args.user, dbname, dry_run=args.dry_run)
            if ok:
                print(f"  DB drop     : {msg}")
                summary["dropped"] += 1
            else:
                print(f"  DB drop     : ERROR — {msg}", file=sys.stderr)
                summary["failed"] += 1
        else:
            print(f"  DB drop     : SKIP — dbname unknown", file=sys.stderr)
            summary["failed"] += 1

        print()

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Dropped : {summary['dropped']}")
    print(f"  Failed  : {summary['failed']}")

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
