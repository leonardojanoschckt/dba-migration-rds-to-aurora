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
    # API may return {"mirrors": [...]} or directly a list
    if isinstance(data, list):
        return data
    return data.get("mirrors", [])


def get_mirror_status(session, flow_job_name):
    """Return full mirror status including config."""
    resp = session.post(
        f"{session.base_url}/api/v1/mirrors/status",
        json={"flowJobName": flow_job_name},
        timeout=15,
    )
    if resp.status_code != 200:
        return {}
    return resp.json()


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


def extract_mirror_record(flow_name, status_resp, peers_lookup):
    """Build a mirror record in the peerdb_mirrors.json format."""
    state  = status_resp.get("currentFlowState", "STATUS_UNKNOWN")
    config = status_resp.get("cdcFlowConfigProto", {}) or {}

    source_peer = config.get("sourceName", "")
    target_peer = config.get("destinationName", "")
    pub_name    = config.get("publicationName", "")
    slot_name   = config.get("replicationSlotName", "")
    table_maps  = config.get("tableMappings", []) or []

    # Derive database from target peer name convention:
    # aurora-microservices1--{dbname}  →  dbname
    database = ""
    if "--" in target_peer:
        database = target_peer.split("--", 1)[1]

    # Derive source_rds from peers report
    source_rds = ""
    peer_info  = peers_lookup.get(source_peer, {})
    if peer_info:
        source_rds = peer_info.get("rds_source", "")

    # Normalize state to simple string
    state_map = {
        "STATUS_RUNNING":    "running",
        "STATUS_PAUSED":     "paused",
        "STATUS_SNAPSHOT":   "snapshot",
        "STATUS_TERMINATED": "terminated",
        "STATUS_UNKNOWN":    "unknown",
    }
    status_str = state_map.get(state, state.lower().replace("status_", ""))

    return {
        "flow_job_name":    flow_name,
        "database":         database,
        "source_rds":       source_rds,
        "source_peer":      source_peer,
        "target_peer":      target_peer,
        "publication":      pub_name,
        "replication_slot": slot_name,
        "tables":           len(table_maps),
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

    print(f"Found {len(mirrors_list)} mirror(s) — fetching details...")

    records = []
    for m in mirrors_list:
        flow_name = m.get("flowJobName") or m.get("flow_job_name") or m.get("name", "")
        if not flow_name:
            continue

        try:
            status_resp = get_mirror_status(session, flow_name)
        except Exception as e:
            print(f"  WARN: could not fetch status for {flow_name}: {e}", file=sys.stderr)
            status_resp = {}

        record = extract_mirror_record(flow_name, status_resp, peers_lookup)
        state  = record["status"]
        db     = record["database"] or "(unknown)"
        src    = record["source_rds"] or record["source_peer"]
        print(f"  {flow_name:<45}  db={db:<30}  src={src}  status={state}")
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
