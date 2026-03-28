#!/usr/bin/env python3
"""List all Redash data sources with their hostname and connection details.

Useful to validate that all PostgreSQL data sources were updated to point
to Aurora endpoints after DNS cutover.

Usage:
    python scripts/redash_list_datasources.py

    # Filter only PostgreSQL data sources
    python scripts/redash_list_datasources.py --type pg

    # Show only sources still pointing to old RDS pattern
    python scripts/redash_list_datasources.py --filter-host rds.amazonaws.com

    # Export to CSV
    python scripts/redash_list_datasources.py --csv output/redash_datasources.csv

Environment variables:
    REDASH_URL        e.g. https://redash.conekta.com
    REDASH_API_KEY    Admin API key from Redash profile settings
"""

import argparse
import csv
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


def get_session(api_key):
    s = requests.Session()
    s.headers.update({"Authorization": f"Key {api_key}"})
    return s


def list_datasources(session, base_url):
    resp = session.get(f"{base_url}/api/data_sources", timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_datasource(session, base_url, ds_id):
    """Fetch full datasource detail including options (host, port, dbname)."""
    resp = session.get(f"{base_url}/api/data_sources/{ds_id}", timeout=15)
    if resp.status_code == 200:
        return resp.json()
    return None


def extract_host(ds):
    """Extract hostname from data source options (varies by type)."""
    options = ds.get("options") or {}
    # PostgreSQL / MySQL / others
    host = (
        options.get("host") or
        options.get("server") or
        options.get("Host") or
        ""
    )
    port = options.get("port") or options.get("Port") or ""
    dbname = options.get("dbname") or options.get("db") or options.get("database") or ""
    return str(host), str(port), str(dbname)


def classify_host(host, aurora_pattern="aurora", rds_pattern="rds.amazonaws.com"):
    """Classify host as AURORA, RDS, or OTHER."""
    if not host:
        return "N/A"
    h = host.lower()
    if aurora_pattern in h:
        return "AURORA"
    if rds_pattern in h:
        return "RDS"
    return "OTHER"


def main():
    parser = argparse.ArgumentParser(
        description="List Redash data sources with hostname for Aurora migration validation."
    )
    parser.add_argument("--url",         default=os.environ.get("REDASH_URL", ""),
                        help="Redash base URL (or set REDASH_URL)")
    parser.add_argument("--api-key",     default=os.environ.get("REDASH_API_KEY", ""),
                        help="Redash admin API key (or set REDASH_API_KEY)")
    parser.add_argument("--type",        default="",
                        help="Filter by type substring, e.g. 'pg', 'postgres', 'mysql'")
    parser.add_argument("--filter-host", default="",
                        help="Only show sources whose host contains this string")
    parser.add_argument("--csv",         default="",
                        help="Export results to CSV file")
    args = parser.parse_args()

    if not args.url:
        print("ERROR: --url or REDASH_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.api_key:
        print("ERROR: --api-key or REDASH_API_KEY is required", file=sys.stderr)
        sys.exit(1)

    base_url = args.url.rstrip("/")
    session  = get_session(args.api_key)

    print(f"Redash Data Sources")
    print(f"URL  : {base_url}")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Time : {now}")
    print("=" * 100)

    try:
        datasources = list_datasources(session, base_url)
    except Exception as e:
        print(f"ERROR fetching data sources: {e}", file=sys.stderr)
        sys.exit(1)

    # Filter by type
    if args.type:
        datasources = [d for d in datasources if args.type.lower() in d.get("type", "").lower()]

    print(f"Fetching details for {len(datasources)} data source(s)...\n")

    # Enrich with full detail (host is only in individual endpoint)
    rows = []
    for ds in sorted(datasources, key=lambda d: d.get("name", "").lower()):
        detail = get_datasource(session, base_url, ds["id"])
        ds_full = detail if detail else ds
        host, port, dbname = extract_host(ds_full)
        status = classify_host(host)

        if args.filter_host and args.filter_host.lower() not in host.lower():
            continue

        rows.append({
            "id":     ds.get("id", ""),
            "name":   ds.get("name", ""),
            "type":   ds.get("type", ""),
            "host":   host,
            "port":   port,
            "dbname": dbname,
            "status": status,
        })

    if not rows:
        print("No data sources found matching the filters.")
        sys.exit(0)

    # Print table
    CW = {"id": 6, "name": 45, "type": 18, "host": 70, "port": 6, "dbname": 25, "status": 8}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(r):
        return (
            f"| {str(r['id']):<{CW['id']}} "
            f"| {r['name']:<{CW['name']}} "
            f"| {r['type']:<{CW['type']}} "
            f"| {r['host']:<{CW['host']}} "
            f"| {r['port']:<{CW['port']}} "
            f"| {r['dbname']:<{CW['dbname']}} "
            f"| {r['status']:<{CW['status']}} |"
        )

    print(sep)
    print(row({"id": "ID", "name": "NAME", "type": "TYPE",
               "host": "HOST", "port": "PORT", "dbname": "DATABASE", "status": "STATUS"}))
    print(sep)

    counts = {"AURORA": 0, "RDS": 0, "OTHER": 0, "N/A": 0}
    for r in rows:
        print(row(r))
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    print(sep)
    print(f"\n  Total   : {len(rows)}")
    print(f"  AURORA  : {counts['AURORA']}")
    print(f"  RDS     : {counts['RDS']}  {'⚠ still on RDS' if counts['RDS'] else ''}")
    print(f"  OTHER   : {counts['OTHER']}")
    print(f"  N/A     : {counts['N/A']} (no host configured)")

    # CSV export
    if args.csv:
        os.makedirs(os.path.dirname(args.csv) if os.path.dirname(args.csv) else ".", exist_ok=True)
        with open(args.csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "type", "host", "port", "dbname", "status"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n  CSV written to: {args.csv}")

    if counts["RDS"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
