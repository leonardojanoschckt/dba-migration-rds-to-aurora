#!/usr/bin/env python3
"""Sanitize ~/.pgpass — backup, deduplicate and reorganize into sections.

Sections (in order):
  1. WILDCARD      — entries with host = *
  2. PRD CNAME     — production custom hostnames (conekta.com / conekta.io)
  3. PRD ENDPOINT  — production RDS/Aurora AWS endpoints
  4. STG CNAME     — staging custom hostnames
  5. STG ENDPOINT  — staging RDS/Aurora AWS endpoints
  6. OTHER         — anything that doesn't match above patterns

Deduplication: for the same (host, port, db, user) key, keeps the LAST entry
(most recently added wins, consistent with how pgpass entries accumulate).

Usage:
    # Dry-run — show what would be written without changing anything
    python scripts/sanitize_pgpass.py --dry-run

    # Apply
    python scripts/sanitize_pgpass.py

    # Custom pgpass path
    python scripts/sanitize_pgpass.py --pgpass ~/.pgpass
"""

import argparse
import os
import shutil
from datetime import datetime, timezone


PGPASS_DEFAULT = os.path.expanduser("~/.pgpass")

# Patterns to classify entries
PRD_MARKERS  = ["prd", "prod", "aurora"]
STG_MARKERS  = ["stg", "staging", "sandbox", "dev"]
CNAME_SUFFIXES   = [".conekta.com", ".conekta.io"]
ENDPOINT_SUFFIXES = [".rds.amazonaws.com"]


def parse_pgpass(path):
    """Return list of dicts with keys: host, port, db, user, password, raw, comment."""
    entries = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            raw = line.rstrip("\n")
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                entries.append({"kind": "comment", "raw": raw})
                continue
            parts = stripped.split(":")
            if len(parts) < 5:
                entries.append({"kind": "comment", "raw": f"# [invalid line {lineno}] {raw}"})
                continue
            host = parts[0]
            port = parts[1]
            db   = parts[2]
            user = parts[3]
            password = ":".join(parts[4:])
            entries.append({
                "kind":     "entry",
                "host":     host,
                "port":     port,
                "db":       db,
                "user":     user,
                "password": password,
                "raw":      raw,
            })
    return entries


def classify(host):
    """Return (env, kind) where env in PRD/STG/WILDCARD/OTHER, kind in CNAME/ENDPOINT/OTHER."""
    if host == "*":
        return "WILDCARD", "OTHER"

    h = host.lower()

    is_cname    = any(h.endswith(s) for s in CNAME_SUFFIXES)
    is_endpoint = any(h.endswith(s) for s in ENDPOINT_SUFFIXES)
    kind = "CNAME" if is_cname else ("ENDPOINT" if is_endpoint else "OTHER")

    is_prd = any(m in h for m in PRD_MARKERS)
    is_stg = any(m in h for m in STG_MARKERS)

    if is_prd and not is_stg:
        env = "PRD"
    elif is_stg and not is_prd:
        env = "STG"
    elif is_prd and is_stg:
        env = "STG"   # stg takes precedence when both match
    else:
        env = "OTHER"

    return env, kind


def deduplicate(entries):
    """Deduplicate by (host, port, db, user) — last entry wins."""
    seen = {}
    for e in entries:
        if e["kind"] != "entry":
            continue
        key = (e["host"], e["port"], e["db"], e["user"])
        seen[key] = e   # overwrite → last wins
    return seen


def build_output(seen):
    """Organize deduplicated entries into sections."""
    sections = {
        "WILDCARD":     [],
        "PRD CNAME":    [],
        "PRD ENDPOINT": [],
        "STG CNAME":    [],
        "STG ENDPOINT": [],
        "OTHER":        [],
    }

    for e in seen.values():
        env, kind = classify(e["host"])
        if env == "WILDCARD":
            sections["WILDCARD"].append(e)
        elif env == "PRD" and kind == "CNAME":
            sections["PRD CNAME"].append(e)
        elif env == "PRD" and kind == "ENDPOINT":
            sections["PRD ENDPOINT"].append(e)
        elif env == "STG" and kind == "CNAME":
            sections["STG CNAME"].append(e)
        elif env == "STG" and kind == "ENDPOINT":
            sections["STG ENDPOINT"].append(e)
        else:
            sections["OTHER"].append(e)

    # Sort entries within each section
    for key in sections:
        sections[key].sort(key=lambda e: (e["host"], e["db"], e["user"]))

    return sections


def render(sections, config_note=""):
    """Render final pgpass content as string."""
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        f"# pgpass — sanitized {now}",
    ]
    if config_note:
        lines.append(f"# {config_note}")
    lines.append("")

    for section_name, entries in sections.items():
        if not entries:
            continue
        lines.append(f"# {'=' * 60}")
        lines.append(f"# {section_name}")
        lines.append(f"# {'=' * 60}")
        for e in entries:
            lines.append(f"{e['host']}:{e['port']}:{e['db']}:{e['user']}:{e['password']}")
        lines.append("")

    return "\n".join(lines) + "\n"


def print_summary(original_entries, seen, sections):
    total_original = sum(1 for e in original_entries if e["kind"] == "entry")
    total_deduped  = len(seen)
    removed        = total_original - total_deduped

    print(f"\nSummary")
    print(f"{'=' * 60}")
    print(f"  Original entries : {total_original}")
    print(f"  Duplicates removed : {removed}")
    print(f"  Final entries    : {total_deduped}")
    print()
    for section_name, entries in sections.items():
        if entries:
            print(f"  {section_name:<16} : {len(entries)}")


def main():
    parser = argparse.ArgumentParser(
        description="Sanitize ~/.pgpass — backup, deduplicate and reorganize into sections."
    )
    parser.add_argument("--pgpass",  default=PGPASS_DEFAULT)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print result without modifying the file")
    args = parser.parse_args()

    if not os.path.exists(args.pgpass):
        print(f"ERROR: {args.pgpass} not found")
        return

    # Parse
    original_entries = parse_pgpass(args.pgpass)
    seen             = deduplicate(original_entries)
    sections         = build_output(seen)
    output           = render(sections)

    print_summary(original_entries, seen, sections)

    if args.dry_run:
        print(f"\n{'=' * 60}")
        print("DRY-RUN — output that would be written:")
        print(f"{'=' * 60}\n")
        print(output)
        return

    # Backup
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{args.pgpass}.{ts}.bak"
    shutil.copy2(args.pgpass, backup_path)
    print(f"\n  Backup : {backup_path}")

    # Write
    with open(args.pgpass, "w") as f:
        f.write(output)
    os.chmod(args.pgpass, 0o600)

    print(f"  Written: {args.pgpass}")
    print("\nDone.")


if __name__ == "__main__":
    main()
