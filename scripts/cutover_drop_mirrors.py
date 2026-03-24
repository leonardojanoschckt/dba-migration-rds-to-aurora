#!/usr/bin/env python3
"""Cutover Step 7 — Drop PeerDB mirrors and clean up SOURCE slot + publication.

For each mirror belonging to a service:
  1. Drops the PeerDB mirror (waits for STATUS_TERMINATED)
  2. Drops the replication slot on SOURCE
  3. Drops the publication on SOURCE

Run this AFTER traffic has been verified on TARGET (Step 5) and Redash
has been updated (Step 6). Once this step completes, replication is
fully stopped and SOURCE is clean.

Usage:
    # Dry-run — show what would be dropped
    python scripts/cutover_drop_mirrors.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Apply — drop mirrors, slots and publications
    python scripts/cutover_drop_mirrors.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply

Environment:
    PEERDB_API_URL      e.g. http://10.210.13.211:3000
    PEERDB_AUTH_HEADER  e.g. Authorization: Basic OnBlZXJkYg==
"""

import argparse
import json
import os
import sys
import time


def _load_dotenv():
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

import psycopg2
import requests
import yaml


DEFAULT_USER   = "svc_claude"
DEFAULT_PORT   = 5432
MIRRORS_REPORT = "output/peerdb_mirrors.json"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def find_service(config, service_name):
    for src in config.get("source_rds_endpoints", []):
        if src["name"] == service_name:
            return src
    return None


def mirrors_for_service(service_name, mirrors_report):
    try:
        with open(mirrors_report) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {mirrors_report} not found", file=sys.stderr)
        sys.exit(1)
    mirrors = [m for m in data.get("mirrors", []) if m.get("source_rds") == service_name]
    if not mirrors:
        print(f"ERROR: no mirrors found for service '{service_name}'", file=sys.stderr)
        sys.exit(1)
    return mirrors


def peerdb_session(api_url, auth_header):
    session = requests.Session()
    if ":" in auth_header:
        name, value = auth_header.split(":", 1)
        session.headers.update({name.strip(): value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


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


def drop_mirror(session, flow_job_name):
    """Send drop request to PeerDB. Returns (ok, message)."""
    resp = session.post(
        f"{session.base_url}/api/v1/mirrors/state_change",
        json={
            "flowJobName":      flow_job_name,
            "requestedFlowState": 6,
            "dropMirrorStats":  True,
            "skipDestinationDrop": False,
        },
    )
    if resp.status_code in (200, 201):
        return True, "drop requested"
    return False, f"HTTP {resp.status_code}: {resp.text[:200]}"


def wait_terminated(session, flow_job_name, timeout=300, interval=5):
    """Poll until STATUS_TERMINATED / STATUS_UNKNOWN or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = get_mirror_state(session, flow_job_name)
        if state in ("STATUS_TERMINATED", "STATUS_UNKNOWN"):
            return True, state
        print(f"      waiting for termination (state={state})...")
        time.sleep(interval)
    return False, "TIMEOUT"


def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=10,
    )


def drop_slot(host, port, user, dbname, slot_name):
    """Drop replication slot on SOURCE. Returns (ok, message)."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(%s) "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name, slot_name),
            )
            found = cur.rowcount > 0
        conn.close()
        if found:
            return True, "dropped"
        return True, "not found (already gone)"
    except Exception as e:
        return False, str(e)[:120]


def drop_publication(host, port, user, dbname, pub_name):
    """Drop publication on SOURCE. Returns (ok, message)."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pub_name,))
            found = cur.fetchone() is not None
            if found:
                cur.execute(f'DROP PUBLICATION IF EXISTS "{pub_name}"')
        conn.close()
        if found:
            return True, "dropped"
        return True, "not found (already gone)"
    except Exception as e:
        return False, str(e)[:120]


def main():
    parser = argparse.ArgumentParser(
        description="Cutover Step 7: drop PeerDB mirrors and clean up SOURCE slot + publication."
    )
    parser.add_argument("--service", required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml")
    parser.add_argument("--apply",   action="store_true",
                        help="Execute drops (default: dry-run)")
    parser.add_argument("--user",    default=DEFAULT_USER)
    parser.add_argument("--port",    type=int, default=DEFAULT_PORT)
    parser.add_argument("--mirrors-report", default=MIRRORS_REPORT)
    parser.add_argument("--api-url",
                        default=os.environ.get("PEERDB_API_URL", ""))
    parser.add_argument("--auth-header",
                        default=os.environ.get("PEERDB_AUTH_HEADER", ""))
    parser.add_argument("--wait-timeout", type=int, default=300,
                        help="Seconds to wait for mirror termination (default: 300)")
    args = parser.parse_args()

    if not args.api_url:
        print("ERROR: --api-url or PEERDB_API_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_header:
        print("ERROR: --auth-header or PEERDB_AUTH_HEADER is required", file=sys.stderr)
        sys.exit(1)

    dry_run = not args.apply
    config  = load_config(args.config)
    source  = find_service(config, args.service)
    if not source:
        print(f"ERROR: service '{args.service}' not found in config", file=sys.stderr)
        sys.exit(1)

    src_host = source["endpoint"]
    mirrors  = mirrors_for_service(args.service, args.mirrors_report)
    session  = peerdb_session(args.api_url, args.auth_header)

    print(f"Cutover Step 7 — Drop PeerDB Mirrors")
    print(f"Service : {args.service}")
    print(f"SOURCE  : {src_host}")
    print(f"Mirrors : {len(mirrors)}")
    print(f"Mode    : {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    errors = 0

    for m in mirrors:
        flow_name = m["flow_job_name"]
        slot_name = m["replication_slot"]
        pub_name  = m["publication"]
        dbname    = m["database"]

        print(f"\n  Mirror  : {flow_name}")
        print(f"  DB      : {dbname}")
        print(f"  Slot    : {slot_name}")
        print(f"  Pub     : {pub_name}")

        # Check current state
        state = get_mirror_state(session, flow_name)
        print(f"  State   : {state}")

        if dry_run:
            print(f"  DRY-RUN — would drop mirror, slot and publication")
            continue

        # Step 1: drop mirror (skip if already gone)
        if state not in ("STATUS_TERMINATED", "STATUS_UNKNOWN"):
            ok, msg = drop_mirror(session, flow_name)
            if not ok:
                print(f"  ERROR dropping mirror: {msg}", file=sys.stderr)
                errors += 1
                continue
            print(f"  Mirror  : drop requested — waiting for termination...")
            ok_term, term_state = wait_terminated(session, flow_name, timeout=args.wait_timeout)
            if not ok_term:
                print(f"  ERROR: mirror did not terminate (state={term_state})", file=sys.stderr)
                errors += 1
                continue
            print(f"  Mirror  : terminated ({term_state})")
        else:
            print(f"  Mirror  : already gone — skipping drop")

        # Step 2: drop replication slot on SOURCE
        ok, msg = drop_slot(src_host, args.port, args.user, dbname, slot_name)
        status = "OK" if ok else "ERROR"
        print(f"  Slot    : {status} — {msg}")
        if not ok:
            errors += 1

        # Step 3: drop publication on SOURCE
        ok, msg = drop_publication(src_host, args.port, args.user, dbname, pub_name)
        status = "OK" if ok else "ERROR"
        print(f"  Pub     : {status} — {msg}")
        if not ok:
            errors += 1

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"  DRY-RUN — use --apply to drop mirrors, slots and publications.")
    else:
        print(f"  Done. Errors: {errors}")

    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
