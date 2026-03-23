#!/usr/bin/env python3
"""PeerDB - Create Peers for SOURCE RDS instances and TARGET Aurora.

Reads migration.yaml, lists all databases on each source RDS and creates
one PeerDB peer per database (source + target). Credentials are resolved
from ~/.pgpass.

Usage:
    python scripts/peerdb_create_peers.py --config config/migration.yaml

    # Dry-run (print what would be created, no API calls)
    python scripts/peerdb_create_peers.py --config config/migration.yaml --dry-run

    # Use a specific PeerDB user (default: svc_claude)
    python scripts/peerdb_create_peers.py --config config/migration.yaml --user svc_claude

Environment variables (override flags):
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
from datetime import datetime, timezone

import psycopg2
import requests
import yaml


DEFAULT_USER = "svc_claude"
DEFAULT_PORT = 5432
DEFAULT_DBNAME = "postgres"
PEER_TYPE_POSTGRES = 3
SYSTEM_DATABASES = {"postgres", "template0", "template1", "rdsadmin"}


# ── helpers ──────────────────────────────────────────────────────────────────

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


def lookup_password(pgpass_entries, host, user, port="5432"):
    def matches(pattern, value):
        return pattern == "*" or pattern == value
    for pg_host, pg_port, _db, pg_user, password in pgpass_entries:
        if matches(pg_host, host) and matches(pg_port, port) and matches(pg_user, user):
            return password
    return None


def list_databases(host, port, user):
    conn = psycopg2.connect(
        host=host, port=port, dbname=DEFAULT_DBNAME, user=user,
        connect_timeout=10,
    )
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datname NOT IN %s AND datistemplate = false
                ORDER BY datname
            """, (tuple(SYSTEM_DATABASES),))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ── PeerDB API ────────────────────────────────────────────────────────────────

def peerdb_session(api_url, auth_header):
    """Return a requests.Session pre-configured for PeerDB."""
    session = requests.Session()
    # Parse "Authorization: Basic ..." into a header dict
    if ":" in auth_header:
        header_name, header_value = auth_header.split(":", 1)
        session.headers.update({header_name.strip(): header_value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


def peer_exists(session, peer_name):
    """Check if a peer already exists by listing peers."""
    resp = session.get(f"{session.base_url}/api/v1/peers")
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    data = resp.json()
    peers = data.get("peers", [])
    return any(p.get("name") == peer_name for p in peers)


def create_peer(session, peer_name, host, port, user, password, dbname, dry_run=False):
    """Create a PostgreSQL peer in PeerDB. Returns (created: bool, message: str)."""
    payload = {
        "peer": {
            "name": peer_name,
            "type": PEER_TYPE_POSTGRES,
            "postgres_config": {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": dbname,
            },
        }
    }

    if dry_run:
        return True, "dry-run"

    resp = session.post(
        f"{session.base_url}/api/v1/peers/create",
        json=payload,
    )

    if resp.status_code in (200, 201):
        return True, resp.json()
    else:
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"


def peer_name_for(kind, rds_name, dbname):
    """Generate a consistent peer name.

    Examples:
        source: rds-backoffice-adjustments-pgsql--backoffice_adjustments
        target: aurora-ckt-microservices1--backoffice_adjustments
    """
    # Sanitize: replace dots and underscores-heavy names with hyphens
    safe_rds = rds_name.replace("_", "-")
    safe_db = dbname.replace("_", "-")
    if kind == "source":
        return f"rds-{safe_rds}--{safe_db}"
    else:
        return f"aurora-{safe_rds}--{safe_db}"


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Create PeerDB peers for SOURCE RDS and TARGET Aurora."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--user", default=DEFAULT_USER,
                        help="PostgreSQL user for PeerDB peers")
    parser.add_argument("--password", default=os.environ.get("PEERDB_PG_PASSWORD", ""),
                        help="Password for PeerDB peers (overrides .pgpass lookup)")
    parser.add_argument("--discovery-user", default="svc_claude",
                        help="PostgreSQL user for DB discovery via .pgpass (default: svc_claude)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--target-aurora", help="Aurora cluster name (default: first)")
    parser.add_argument("--pgpass", default="~/.pgpass")
    parser.add_argument("--api-url", default=os.environ.get("PEERDB_API_URL", ""),
                        help="PeerDB API URL (or set PEERDB_API_URL)")
    parser.add_argument("--auth-header",
                        default=os.environ.get("PEERDB_AUTH_HEADER", ""),
                        help="PeerDB auth header (or set PEERDB_AUTH_HEADER)")
    parser.add_argument("--output", default="output/peerdb_peers.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print peers that would be created without calling the API")
    args = parser.parse_args()

    if not args.api_url:
        print("ERROR: --api-url or PEERDB_API_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.auth_header:
        print("ERROR: --auth-header or PEERDB_AUTH_HEADER is required", file=sys.stderr)
        sys.exit(1)

    pgpass_path = os.path.expanduser(args.pgpass)
    pgpass_entries = read_pgpass(pgpass_path)
    print(f".pgpass loaded: {len(pgpass_entries)} entry(ies)")

    config = load_config(args.config)
    sources = config.get("source_rds_endpoints", [])
    aurora_clusters = config.get("target_aurora_clusters", [])

    if args.target_aurora:
        cluster = next((c for c in aurora_clusters if c["name"] == args.target_aurora), None)
        if not cluster:
            print(f"ERROR: Aurora cluster '{args.target_aurora}' not found", file=sys.stderr)
            sys.exit(1)
    else:
        cluster = aurora_clusters[0] if aurora_clusters else None

    if not cluster:
        print("ERROR: No target Aurora cluster in config", file=sys.stderr)
        sys.exit(1)

    target_host = cluster["endpoint"]
    target_name = cluster["name"]
    target_short = target_name.replace("ckt-", "").replace("-aurora-pgsql-prd", "")

    session = peerdb_session(args.api_url, args.auth_header)

    print(f"PeerDB API: {args.api_url}")
    print(f"Target Aurora: {target_name}")
    print(f"Sources: {len(sources)}")
    if args.dry_run:
        print("MODE: dry-run (no API calls)")
    print("=" * 70)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target": target_name,
        "peers": [],
        "summary": {"created": 0, "skipped": 0, "failed": 0},
    }

    # --password flag overrides .pgpass lookup for all peers
    fixed_pwd = args.password if args.password else None

    target_pwd = fixed_pwd or lookup_password(pgpass_entries, target_host, args.user, str(args.port))
    if not target_pwd:
        print(f"WARNING: no password for TARGET {target_host} / {args.user}", file=sys.stderr)

    for src in sources:
        src_host = src["endpoint"]
        src_name = src["name"]

        print(f"\n[ {src_name} ]")

        src_pwd = fixed_pwd or lookup_password(pgpass_entries, src_host, args.user, str(args.port))
        if not src_pwd:
            print(f"  WARNING: no password for {args.user}@{src_host} — skipping", file=sys.stderr)
            continue

        try:
            databases = list_databases(src_host, args.port, args.discovery_user)
        except Exception as e:
            print(f"  ERROR listing databases: {e}", file=sys.stderr)
            continue

        print(f"  Databases: {', '.join(databases)}")

        for dbname in databases:
            src_peer = peer_name_for("source", src_name, dbname)
            tgt_peer = peer_name_for("target", target_short, dbname)

            # ── SOURCE peer ──
            print(f"\n  [{dbname}]")
            print(f"    Source peer : {src_peer}")

            if not args.dry_run and peer_exists(session, src_peer):
                print(f"    Source      : already exists — skipped")
                report["summary"]["skipped"] += 1
            else:
                ok, msg = create_peer(
                    session, src_peer,
                    src_host, args.port, args.user, src_pwd, dbname,
                    dry_run=args.dry_run,
                )
                status = "created" if ok else "FAILED"
                print(f"    Source      : {status}" + (f" — {msg}" if not ok else ""))
                if ok:
                    report["summary"]["created"] += 1
                else:
                    report["summary"]["failed"] += 1

            report["peers"].append({
                "kind": "source", "peer_name": src_peer,
                "host": src_host, "database": dbname,
                "rds_source": src_name,
            })

            # ── TARGET peer ──
            print(f"    Target peer : {tgt_peer}")

            if not args.dry_run and peer_exists(session, tgt_peer):
                print(f"    Target      : already exists — skipped")
                report["summary"]["skipped"] += 1
            else:
                ok, msg = create_peer(
                    session, tgt_peer,
                    target_host, args.port, args.user, target_pwd or "", dbname,
                    dry_run=args.dry_run,
                )
                status = "created" if ok else "FAILED"
                print(f"    Target      : {status}" + (f" — {msg}" if not ok else ""))
                if ok:
                    report["summary"]["created"] += 1
                else:
                    report["summary"]["failed"] += 1

            report["peers"].append({
                "kind": "target", "peer_name": tgt_peer,
                "host": target_host, "database": dbname,
                "rds_source": src_name,
            })

    s = report["summary"]
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Created : {s['created']}")
    print(f"  Skipped : {s['skipped']} (already existed)")
    print(f"  Failed  : {s['failed']}")

    # Merge with existing report so multiple config runs don't overwrite each other
    if os.path.exists(args.output):
        with open(args.output) as f:
            existing = json.load(f)
        existing_names = {p["peer_name"] for p in existing.get("peers", [])}
        for p in report["peers"]:
            if p["peer_name"] not in existing_names:
                existing["peers"].append(p)
        existing["generated_at"] = report["generated_at"]
        report = existing

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport written to: {args.output}")

    if s["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
