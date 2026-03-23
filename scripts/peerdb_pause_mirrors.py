#!/usr/bin/env python3
"""PeerDB - Pause all running mirrors.

Usage:
    python scripts/peerdb_pause_mirrors.py

    # Only a specific mirror
    python scripts/peerdb_pause_mirrors.py --mirror mirror_backoffice_adjustments_001

Environment variables:
    PEERDB_API_URL        e.g. http://10.210.13.211:3000
    PEERDB_AUTH_HEADER    e.g. Authorization: Basic OnBlZXJkYg==
"""

import argparse
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

import json

import requests
import yaml


PAUSABLE_STATES = {"STATUS_RUNNING", "STATUS_SNAPSHOT"}
PEERS_REPORT    = "output/peerdb_peers.json"


def config_mirror_names(config, peers_data):
    """Return set of mirror names that belong to sources defined in the config."""
    source_endpoints = {src["endpoint"] for src in config.get("source_rds_endpoints", [])}
    databases = {
        p["database"]
        for p in peers_data.get("peers", [])
        if p.get("kind") == "source" and p.get("host") in source_endpoints
    }
    return {f"mirror_{db}_001" for db in databases}


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


def get_mirror_state(session, flow_job_name):
    resp = session.post(
        f"{session.base_url}/api/v1/mirrors/status",
        json={"flowJobName": flow_job_name},
    )
    if resp.status_code not in (200, 201):
        return "STATUS_UNKNOWN"
    return resp.json().get("currentFlowState", "STATUS_UNKNOWN")


def pause_mirror(session, flow_job_name):
    resp = session.post(
        f"{session.base_url}/api/v1/mirrors/state_change",
        json={"flowJobName": flow_job_name, "requestedFlowState": "STATUS_PAUSED"},
    )
    if resp.status_code in (200, 201):
        return True, resp.json()
    return False, f"HTTP {resp.status_code}: {resp.text[:300]}"


def wait_paused(session, flow_job_name, timeout=120, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = get_mirror_state(session, flow_job_name)
        if state == "STATUS_PAUSED":
            return True, state
        if state in ("STATUS_TERMINATED", "STATUS_UNKNOWN"):
            return False, state
        print(f"      waiting (state={state})...")
        time.sleep(interval)
    return False, "TIMEOUT"


def main():
    parser = argparse.ArgumentParser(description="Pause all PeerDB mirrors.")
    parser.add_argument("--config", default="config/migration.yaml",
                        help="Migration config file (default: config/migration.yaml)")
    parser.add_argument("--mirror", help="Only pause this specific mirror")
    parser.add_argument("--no-wait", action="store_true",
                        help="Don't wait for confirmation of paused state")
    parser.add_argument("--peers-report", default=PEERS_REPORT)
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
        print(f"ERROR: {args.peers_report} not found — run peerdb_create_peers.py first", file=sys.stderr)
        sys.exit(1)

    session = peerdb_session(args.api_url, args.auth_header)

    allowed = config_mirror_names(config, peers_data)
    mirrors = [m for m in list_mirrors(session) if m["name"] in allowed]
    if args.mirror:
        mirrors = [m for m in mirrors if m["name"] == args.mirror]
        if not mirrors:
            print(f"ERROR: mirror '{args.mirror}' not found", file=sys.stderr)
            sys.exit(1)

    print(f"PeerDB Pause Mirrors")
    print(f"Mirrors found: {len(mirrors)}")
    print("=" * 60)

    summary = {"paused": 0, "skipped": 0, "failed": 0}

    for mirror in mirrors:
        name = mirror["name"]
        state = get_mirror_state(session, name)
        print(f"\n[ {name} ] (state={state})")

        if state not in PAUSABLE_STATES:
            print(f"  SKIP — not in a pausable state")
            summary["skipped"] += 1
            continue

        ok, msg = pause_mirror(session, name)
        if not ok:
            print(f"  ERROR: {msg}", file=sys.stderr)
            summary["failed"] += 1
            continue

        if args.no_wait:
            print(f"  pause requested")
            summary["paused"] += 1
            continue

        ok_pause, final_state = wait_paused(session, name)
        if ok_pause:
            print(f"  PAUSED")
            summary["paused"] += 1
        else:
            print(f"  WARN: did not reach PAUSED (state={final_state})", file=sys.stderr)
            summary["failed"] += 1

    print(f"\n{'=' * 60}")
    print(f"  Paused  : {summary['paused']}")
    print(f"  Skipped : {summary['skipped']}")
    print(f"  Failed  : {summary['failed']}")

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
