#!/usr/bin/env python3
"""Cutover Step 1a — Pause PeerDB mirrors for a single RDS service.

Looks up mirrors in output/peerdb_mirrors.json by source_rds name,
then pauses each one and waits for STATUS_PAUSED confirmation.

Usage:
    python scripts/cutover_pause_mirrors.py \\
        --service bo-risk-monitoring-engine-pgsql-prd

    # Don't wait for confirmation
    python scripts/cutover_pause_mirrors.py \\
        --service bo-risk-monitoring-engine-pgsql-prd --no-wait

Environment variables:
    PEERDB_API_URL       e.g. http://10.210.13.211:3000
    PEERDB_AUTH_HEADER   e.g. Authorization: Basic OnBlZXJkYg==
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

import requests

PAUSABLE_STATES = {"STATUS_RUNNING", "STATUS_SNAPSHOT"}
MIRRORS_REPORT  = "output/peerdb_mirrors.json"


def peerdb_session(api_url, auth_header):
    session = requests.Session()
    if ":" in auth_header:
        name, value = auth_header.split(":", 1)
        session.headers.update({name.strip(): value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


def mirrors_for_service(service_name, mirrors_report):
    try:
        with open(mirrors_report) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {mirrors_report} not found — run peerdb_create_mirrors.py first",
              file=sys.stderr)
        sys.exit(1)

    mirrors = [m for m in data.get("mirrors", []) if m.get("source_rds") == service_name]
    if not mirrors:
        print(f"ERROR: no mirrors found for service '{service_name}' in {mirrors_report}",
              file=sys.stderr)
        sys.exit(1)
    return mirrors


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
    parser = argparse.ArgumentParser(
        description="Cutover Step 1a: pause PeerDB mirrors for a single RDS service."
    )
    parser.add_argument("--service", required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--no-wait", action="store_true",
                        help="Don't wait for STATUS_PAUSED confirmation")
    parser.add_argument("--mirrors-report", default=MIRRORS_REPORT)
    parser.add_argument("--api-url",     default=os.environ.get("PEERDB_API_URL", ""))
    parser.add_argument("--auth-header", default=os.environ.get("PEERDB_AUTH_HEADER", ""))
    args = parser.parse_args()

    if not args.api_url:
        print("ERROR: --api-url or PEERDB_API_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_header:
        print("ERROR: --auth-header or PEERDB_AUTH_HEADER is required", file=sys.stderr)
        sys.exit(1)

    mirrors = mirrors_for_service(args.service, args.mirrors_report)
    session = peerdb_session(args.api_url, args.auth_header)

    print(f"PeerDB Pause Mirrors — {args.service}")
    print(f"Mirrors : {[m['flow_job_name'] for m in mirrors]}")
    print("=" * 60)

    summary = {"paused": 0, "skipped": 0, "failed": 0}

    for m in mirrors:
        name = m["flow_job_name"]
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
            print(f"  PAUSED ✓")
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
