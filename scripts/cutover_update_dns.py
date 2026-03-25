#!/usr/bin/env python3
"""Cutover Step 3 — Update Route53 CNAME records to point to TARGET Aurora.

For the given service, reads all CNAMEs from the config and updates each
Route53 record to point to the Aurora cluster endpoint instead of the
source RDS endpoint.

Supports multiple hosted zones (e.g. conekta.com and conekta.io).
Dry-run by default — shows current value → new value before applying.

Usage:
    # Dry-run — show what would change
    python scripts/cutover_update_dns.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Apply
    python scripts/cutover_update_dns.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply

    # Custom TTL (default: 60)
    python scripts/cutover_update_dns.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --apply --ttl 300
"""

import argparse
import sys

import boto3
import botocore.exceptions
import yaml


DEFAULT_USER    = "svc_claude"
DEFAULT_PROFILE = "prd"
DEFAULT_REGION  = "us-east-1"


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def find_service(config, service_name):
    for src in config.get("source_rds_endpoints", []):
        if src["name"] == service_name:
            return src
    return None


def list_hosted_zones(client):
    """Return all hosted zones as list of {id, name}."""
    zones = []
    paginator = client.get_paginator("list_hosted_zones")
    for page in paginator.paginate():
        for z in page["HostedZones"]:
            zones.append({
                "id":   z["Id"].split("/")[-1],
                "name": z["Name"].rstrip("."),
            })
    return zones


def best_zone_for(hostname, zones):
    """Return the hosted zone whose name is the longest suffix match for hostname."""
    hostname = hostname.rstrip(".")
    best = None
    for z in zones:
        zone_name = z["name"]
        if hostname == zone_name or hostname.endswith("." + zone_name):
            if best is None or len(zone_name) > len(best["name"]):
                best = z
    return best


def get_current_record(client, zone_id, hostname):
    """Return (value, ttl) of the CNAME record, or (None, None) if not found."""
    fqdn = hostname.rstrip(".") + "."
    try:
        resp = client.list_resource_record_sets(
            HostedZoneId=zone_id,
            StartRecordName=fqdn,
            StartRecordType="CNAME",
            MaxItems="1",
        )
        for rr in resp.get("ResourceRecordSets", []):
            if rr["Name"].rstrip(".") == hostname.rstrip(".") and rr["Type"] == "CNAME":
                value = rr["ResourceRecords"][0]["Value"].rstrip(".")
                return value, rr["TTL"]
    except Exception:
        pass
    return None, None


def upsert_cname(client, zone_id, hostname, new_value, ttl, comment=""):
    """Upsert a CNAME record in Route53."""
    fqdn     = hostname.rstrip(".") + "."
    new_fqdn = new_value.rstrip(".") + "."
    change = {
        "Action": "UPSERT",
        "ResourceRecordSet": {
            "Name": fqdn,
            "Type": "CNAME",
            "TTL":  ttl,
            "ResourceRecords": [{"Value": new_fqdn}],
        },
    }
    kwargs = {"HostedZoneId": zone_id, "ChangeBatch": {"Changes": [change]}}
    if comment:
        kwargs["ChangeBatch"]["Comment"] = comment
    resp = client.change_resource_record_sets(**kwargs)
    return resp["ChangeInfo"]["Id"], resp["ChangeInfo"]["Status"]


def main():
    parser = argparse.ArgumentParser(
        description="Cutover Step 3: update Route53 CNAMEs to point to TARGET Aurora."
    )
    parser.add_argument("--service", required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--config",  default="config/catalog_services_migration.yaml",
                        help="Migration YAML config (default: config/catalog_services_migration.yaml)")
    parser.add_argument("--apply",   action="store_true",
                        help="Apply DNS changes (default: dry-run)")
    parser.add_argument("--ttl",     type=int, default=60,
                        help="TTL in seconds (default: 60)")
    parser.add_argument("--aws-profile", default=DEFAULT_PROFILE)
    parser.add_argument("--region",      default=DEFAULT_REGION)
    args = parser.parse_args()

    dry_run = not args.apply

    config = load_config(args.config)

    source = find_service(config, args.service)
    if not source:
        print(f"ERROR: service '{args.service}' not found in config", file=sys.stderr)
        sys.exit(1)

    clusters = config.get("target_aurora_clusters", [])
    if not clusters:
        print("ERROR: no target_aurora_clusters in config", file=sys.stderr)
        sys.exit(1)

    source_endpoint = source["endpoint"]
    aurora_endpoint = clusters[0]["endpoint"]
    cnames          = source.get("cnames", [])

    if not cnames:
        print(f"ERROR: no CNAMEs defined for service '{args.service}'", file=sys.stderr)
        sys.exit(1)

    print(f"Cutover Step 3 — Update DNS")
    print(f"Service : {args.service}")
    print(f"FROM    : {source_endpoint}")
    print(f"TO      : {aurora_endpoint}")
    print(f"CNAMEs  : {cnames}")
    print(f"Mode    : {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 70)

    try:
        session = boto3.Session(profile_name=args.aws_profile, region_name=args.region)
        r53     = session.client("route53")
    except Exception as e:
        print(f"ERROR creating boto3 session: {e}", file=sys.stderr)
        sys.exit(1)

    # Load all hosted zones once
    try:
        zones = list_hosted_zones(r53)
    except botocore.exceptions.ClientError as e:
        print(f"ERROR listing hosted zones: {e}", file=sys.stderr)
        sys.exit(1)

    total_ok = total_skip = total_err = 0

    for hostname in cnames:
        print(f"\n  {hostname}")

        zone = best_zone_for(hostname, zones)
        if not zone:
            print(f"    ERROR: no hosted zone found for '{hostname}'", file=sys.stderr)
            total_err += 1
            continue

        print(f"    Zone  : {zone['name']}  ({zone['id']})")

        current_val, current_ttl = get_current_record(r53, zone["id"], hostname)
        ttl = args.ttl

        if current_val is None:
            print(f"    Current: (record not found)")
        else:
            # Classify current target
            if source_endpoint in current_val:
                points_to = "→ SOURCE"
            elif aurora_endpoint in current_val:
                points_to = "→ AURORA (already updated)"
            else:
                points_to = f"→ {current_val}"
            print(f"    Current: {current_val}  [{points_to}]  TTL={current_ttl}")

        print(f"    New    : {aurora_endpoint}  TTL={ttl}")

        # Skip if already pointing to Aurora
        if current_val and aurora_endpoint in current_val:
            print(f"    SKIP — already pointing to Aurora")
            total_skip += 1
            continue

        if dry_run:
            print(f"    DRY-RUN — would update CNAME")
            total_ok += 1
            continue

        try:
            change_id, status = upsert_cname(
                r53, zone["id"], hostname, aurora_endpoint, ttl,
                comment=f"Cutover {args.service} → Aurora",
            )
            print(f"    UPDATED ✓  (change={change_id.split('/')[-1]}, status={status})")
            total_ok += 1
        except botocore.exceptions.ClientError as e:
            print(f"    ERROR: {e}", file=sys.stderr)
            total_err += 1

    print(f"\n{'=' * 70}")
    print(f"  {'Would update' if dry_run else 'Updated'} : {total_ok}")
    print(f"  Skipped               : {total_skip}")
    print(f"  Errors                : {total_err}")

    if dry_run:
        print("\n  DRY-RUN — use --apply to execute changes in Route53.")

    if total_err > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
