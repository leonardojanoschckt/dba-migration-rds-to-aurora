#!/usr/bin/env python3
"""Export currently running PeerDB mirrors to output/peerdb_mirrors.json.

Queries the PeerDB API for all mirrors, enriches each one with peer
metadata (source_rds, database names) from peerdb_peers.json, and
writes the result in the same format expected by the cutover scripts.

Usage:
    python scripts/peerdb_export_mirrors.py

    # Custom output path
    python scripts/peerdb_export_mirrors.py --output output/peerdb_mirrors.json

Environment:
    PEERDB_API_URL      e.g. http://10.210.13.211:3000
    PEERDB_AUTH_HEADER  e.g. Authorization: Basic OnBlZXJkYg==
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone


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


DEFAULT_OUTPUT  = "output/peerdb_mirrors.json"
PEERS_REPORT    = "output/peerdb_peers.json"


def peerdb_session(api_url, auth_header):
    session = requests.Session()
    if ":" in auth_header:
        name, value = auth_header.split(":", 1)
        session.headers.update({name.strip(): value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


def list_mirrors(session):
    """Return list of mirror summaries from GET /api/v1/mirrors/list."""
    resp = session.get(f"{session.base_url}/api/v1/mirrors/list", timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    return data.get("mirrors", [])


def get_mirror_state(session, flow_job_name):
    """Return currentFlowState string."""
    try:
        resp = session.post(
            f"{session.base_url}/api/v1/mirrors/status",
            json={"flowJobName": flow_job_name},
            timeout=15,
        )
        if resp.status_code != 200:
            return "STATUS_UNKNOWN"
        return resp.json().get("currentFlowState", "STATUS_UNKNOWN")
    except Exception:
        return "STATUS_UNKNOWN"


def load_peers_report(path):
    """Build lookup: peer_name -> {rds_source, database, kind}."""
    try:
        with open(path) as f:
            data = json.load(f)
        lookup = {}
        for p in data.get("peers", []):
            lookup[p["peer_name"]] = p
        return lookup
    except FileNotFoundError:
        return {}


STATE_MAP = {
    "STATUS_RUNNING":    "running",
    "STATUS_PAUSED":     "paused",
    "STATUS_SNAPSHOT":   "snapshot",
    "STATUS_TERMINATED": "terminated",
    "STATUS_UNKNOWN":    "unknown",
}


def extract_mirror_record(m, state, peers_lookup):
    """Build a mirror record from mirrors/list entry + state."""
    flow_name   = m.get("name", "")
    source_peer = m.get("sourceName", "")
    target_peer = m.get("destinationName", "")

    # Derive database from target peer: aurora-microservices1--{dbname}
    # Peer names use hyphens; actual DB names use underscores
    database = target_peer.split("--", 1)[1].replace("-", "_") if "--" in target_peer else ""

    # Slot and publication follow naming convention used by peerdb_create_mirrors.py
    pub_name  = f"aurora_pub_{database}_001"  if database else ""
    slot_name = f"aurora_slot_{database}_001" if database else ""

    # Derive source_rds from peers report
    source_rds = ""
    peer_info  = peers_lookup.get(source_peer, {})
    if peer_info:
        source_rds = peer_info.get("rds_source", "")

    status_str = STATE_MAP.get(state, state.lower().replace("status_", ""))

    return {
        "flow_job_name":    flow_name,
        "database":         database,
        "source_rds":       source_rds,
        "source_peer":      source_peer,
        "target_peer":      target_peer,
        "publication":      pub_name,
        "replication_slot": slot_name,
        "tables":           0,
        "initial_snapshot": True,
        "status":           status_str,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Export active PeerDB mirrors to peerdb_mirrors.json."
    )
    parser.add_argument("--output",      default=DEFAULT_OUTPUT)
    parser.add_argument("--peers-report", default=PEERS_REPORT)
    parser.add_argument("--api-url",     default=os.environ.get("PEERDB_API_URL", ""))
    parser.add_argument("--auth-header", default=os.environ.get("PEERDB_AUTH_HEADER", ""))
    args = parser.parse_args()

    if not args.api_url:
        print("ERROR: --api-url or PEERDB_API_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_header:
        print("ERROR: --auth-header or PEERDB_AUTH_HEADER is required", file=sys.stderr)
        sys.exit(1)

    session      = peerdb_session(args.api_url, args.auth_header)
    peers_lookup = load_peers_report(args.peers_report)

    if not peers_lookup:
        print(f"WARN: peers report not found at {args.peers_report} — source_rds will be empty",
              file=sys.stderr)

    print(f"Fetching mirrors from {args.api_url}...")
    try:
        mirrors_list = list_mirrors(session)
    except Exception as e:
        print(f"ERROR fetching mirrors: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(mirrors_list)} mirror(s) — fetching state...")

    records = []
    for m in mirrors_list:
        flow_name = m.get("name", "")
        if not flow_name:
            continue

        state  = get_mirror_state(session, flow_name)
        record = extract_mirror_record(m, state, peers_lookup)
        db     = record["database"] or "(unknown)"
        src    = record["source_rds"] or record["source_peer"]
        print(f"  {flow_name:<45}  db={db:<30}  src={src:<50}  status={record['status']}")
        records.append(record)

    # Sort by flow_job_name for stable output
    records.sort(key=lambda r: r["flow_job_name"])

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mirrors": records,
    }

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nWrote {len(records)} mirror(s) to {args.output}")


if __name__ == "__main__":
    main()
