#!/usr/bin/env python3
"""Cutover Step 6 — Update Redash datasources from SOURCE to TARGET.

Queries the Redash API to find all datasources whose host matches the
SOURCE RDS endpoint or any of its CNAMEs, then updates them to point
to the TARGET Aurora endpoint.

Usage:
    # Dry-run — list matching datasources
    python scripts/cutover_update_redash.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Apply — update matched datasources to TARGET
    python scripts/cutover_update_redash.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply

    # Check all services at once
    python scripts/cutover_update_redash.py \\
        --config config/catalog_services_migration.yaml \\
        --all

Environment:
    REDASH_URL      e.g. https://redash.conekta.io
    REDASH_API_KEY  Redash admin API key
"""

import argparse
import os
import sys


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
import yaml


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def find_service(config, service_name):
    for src in config.get("source_rds_endpoints", []):
        if src["name"] == service_name:
            return src
    return None


def redash_session(base_url, api_key):
    s = requests.Session()
    s.headers.update({"Authorization": f"Key {api_key}"})
    s.base_url = base_url.rstrip("/")
    return s


def list_datasources(session):
    resp = session.get(f"{session.base_url}/api/data_sources", timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_datasource(session, ds_id):
    resp = session.get(f"{session.base_url}/api/data_sources/{ds_id}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def update_datasource(session, ds_id, payload):
    resp = session.post(
        f"{session.base_url}/api/data_sources/{ds_id}",
        json=payload,
        timeout=10,
    )
    if resp.status_code in (200, 201):
        return True, resp.json()
    return False, f"HTTP {resp.status_code}: {resp.text[:300]}"


def host_matches(host_value, source_endpoint, cnames):
    """Return (matched, matched_via) if host_value matches source or any CNAME."""
    if not host_value:
        return False, None
    host_value = host_value.strip().rstrip(".")
    if source_endpoint and source_endpoint in host_value:
        return True, f"endpoint:{source_endpoint}"
    for cname in cnames:
        if cname in host_value:
            return True, f"cname:{cname}"
    return False, None


def find_matches(session, source_endpoint, cnames):
    """Fetch all datasources and return those matching source/cnames."""
    all_ds = list_datasources(session)
    matches = []

    for ds in all_ds:
        ds_id   = ds["id"]
        ds_name = ds.get("name", "?")
        ds_type = ds.get("type", "?")

        # The list endpoint may omit options; fetch full detail
        try:
            full = get_datasource(session, ds_id)
        except Exception as e:
            print(f"  WARN: could not fetch datasource {ds_id} ({ds_name}): {e}",
                  file=sys.stderr)
            continue

        options = full.get("options", {})
        host    = options.get("host") or options.get("server") or ""

        matched, via = host_matches(host, source_endpoint, cnames)
        if matched:
            matches.append({
                "id":      ds_id,
                "name":    ds_name,
                "type":    ds_type,
                "host":    host,
                "via":     via,
                "options": options,
                "full":    full,
            })

    return matches


def print_match(m, new_host=None, status=""):
    print(f"\n  [{m['id']}] {m['name']}  (type={m['type']})")
    print(f"       Host    : {m['host']}")
    print(f"       Matched : {m['via']}")
    if new_host:
        print(f"       New host: {new_host}")
    if status:
        print(f"       Status  : {status}")


def process_service(session, source, aurora_endpoint, dry_run):
    source_endpoint = source["endpoint"]
    cnames          = source.get("cnames", [])
    service_name    = source["name"]

    print(f"\n{'─' * 60}")
    print(f"Service : {service_name}")
    print(f"FROM    : {source_endpoint}")
    print(f"TO      : {aurora_endpoint}")
    print(f"CNAMEs  : {cnames}")

    matches = find_matches(session, source_endpoint, cnames)

    if not matches:
        print("  (no matching datasources found)")
        return 0, 0, 0

    print(f"  Found {len(matches)} matching datasource(s):")

    updated = skipped = errors = 0

    for m in matches:
        current_host = m["host"]

        # Already pointing to Aurora?
        if aurora_endpoint in current_host:
            print_match(m, status="SKIP — already pointing to Aurora")
            skipped += 1
            continue

        if dry_run:
            print_match(m, new_host=aurora_endpoint, status="DRY-RUN — would update")
            updated += 1
            continue

        # Build updated payload — preserve all existing options, only change host
        new_options = {**m["options"], "host": aurora_endpoint}
        payload     = {**m["full"], "options": new_options}
        # Remove read-only fields Redash rejects
        for field in ("id", "groups", "view_only", "syntax"):
            payload.pop(field, None)

        ok, result = update_datasource(session, m["id"], payload)
        if ok:
            print_match(m, new_host=aurora_endpoint, status="UPDATED ✓")
            updated += 1
        else:
            print_match(m, new_host=aurora_endpoint, status=f"ERROR: {result}")
            errors += 1

    return updated, skipped, errors


def main():
    parser = argparse.ArgumentParser(
        description="Cutover Step 6: update Redash datasources from SOURCE to TARGET Aurora."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--service",
                       help="Single RDS service name")
    group.add_argument("--all", action="store_true",
                       help="Check all services in config")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml")
    parser.add_argument("--apply",   action="store_true",
                        help="Apply updates to Redash (default: dry-run)")
    parser.add_argument("--redash-url",
                        default=os.environ.get("REDASH_URL", ""),
                        help="Redash base URL (or set REDASH_URL)")
    parser.add_argument("--api-key",
                        default=os.environ.get("REDASH_API_KEY", ""),
                        help="Redash API key (or set REDASH_API_KEY)")
    parser.add_argument("--target-host",
                        default="ckt-microservices1-aurora-pgsql-prd-reader.conekta.com",
                        help="Target host for Redash datasources (default: Aurora reader CNAME)")
    args = parser.parse_args()

    if not args.redash_url:
        print("ERROR: --redash-url or REDASH_URL is required", file=sys.stderr)
        sys.exit(1)
    if not args.api_key:
        print("ERROR: --api-key or REDASH_API_KEY is required", file=sys.stderr)
        sys.exit(1)

    dry_run = not args.apply

    config   = load_config(args.config)
    clusters = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)
    aurora_endpoint = clusters[0]["endpoint"]

    print(f"Cutover Step 6 — Update Redash Datasources")
    print(f"Redash  : {args.redash_url}")
    print(f"TARGET  : {args.target_host}")
    print(f"Mode    : {'DRY-RUN' if dry_run else 'APPLY'}")

    try:
        session = redash_session(args.redash_url, args.api_key)
    except Exception as e:
        print(f"ERROR creating session: {e}", file=sys.stderr)
        sys.exit(1)

    # Build service list
    if args.all:
        sources = config.get("source_rds_endpoints", [])
    else:
        source = find_service(config, args.service)
        if not source:
            print(f"ERROR: service '{args.service}' not found in config", file=sys.stderr)
            sys.exit(1)
        sources = [source]

    total_updated = total_skipped = total_errors = 0

    for source in sources:
        upd, skip, err = process_service(session, source, args.target_host, dry_run)
        total_updated += upd
        total_skipped += skip
        total_errors  += err

    print(f"\n{'=' * 60}")
    print(f"  {'Would update' if dry_run else 'Updated'} : {total_updated}")
    print(f"  Skipped               : {total_skipped}")
    print(f"  Errors                : {total_errors}")

    if dry_run:
        print("\n  DRY-RUN — use --apply to update Redash datasources.")

    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
