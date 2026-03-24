#!/usr/bin/env python3
"""Cutover Dashboard — single-microservice migration monitor.

Displays 6 live panels for one RDS microservice during Aurora cutover:

  [1] DNS     — CNAME record name / type / current value / points-to
  [2] PeerDB  — Mirror status / sync interval / rows synced / slot lag
  [3] Conns   — SOURCE vs TARGET pg_stat_activity by state
  [4] Seqs    — SOURCE vs TARGET sequence last_value + delta
  [5] Datadog — APM error rate (requires DD_API_KEY + DD_APP_KEY env vars)
  [6] ECS     — ECS service status: desired / running / pending tasks + deployment

Usage:
    # Run once
    python scripts/cutover_dashboard.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml

    # Auto-refresh every 15 s
    python scripts/cutover_dashboard.py \\
        --service bo-risk-monitoring-engine-pgsql-prd \\
        --config  config/catalog_services_migration.yaml \\
        --watch 15

Environment:
    PEERDB_API_URL       e.g. http://10.210.13.211:3000
    PEERDB_AUTH_HEADER   e.g. Authorization: Basic OnBlZXJkYg==
    DD_API_KEY           Datadog API key  (optional)
    DD_APP_KEY           Datadog APP key  (optional)
    DD_SITE              Datadog site     (default: datadoghq.com)
    AWS_PROFILE          AWS profile for ECS describe_services (default: env default)
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg2
import boto3
import botocore.exceptions
import requests
import yaml


# ---------------------------------------------------------------------------
# Bootstrap .env
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------

DEFAULT_USER        = "svc_claude"
DEFAULT_PORT        = 5432
PEERS_REPORT        = "output/peerdb_peers.json"
MIRRORS_REPORT      = "output/peerdb_mirrors.json"
DISCOVERY_REPORT    = "output/discovery_report.json"
SYSTEM_DBS          = {"postgres", "template0", "template1", "rdsadmin"}

# ANSI
C_GREEN  = "\033[32m"
C_YELLOW = "\033[33m"
C_RED    = "\033[31m"
C_CYAN   = "\033[36m"
C_GRAY   = "\033[90m"
C_BOLD   = "\033[1m"
C_RESET  = "\033[0m"

STATE_COLOR = {
    "STATUS_RUNNING":    C_GREEN,
    "STATUS_SNAPSHOT":   C_CYAN,
    "STATUS_PAUSED":     C_YELLOW,
    "STATUS_TERMINATED": C_RED,
    "UNKNOWN":           C_GRAY,
}


def colorize(color, text):
    return f"{color}{text}{C_RESET}"


def plain_len(text):
    """Length of text after stripping ANSI escape codes."""
    return len(re.sub(r"\033\[[0-9;]*m", "", text))


def rpad(text, width):
    """Right-pad text to visual width, accounting for ANSI codes."""
    return text + " " * max(0, width - plain_len(text))


def lpad(text, width):
    return " " * max(0, width - plain_len(text)) + text


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def find_service(config, service_name):
    """Return the source RDS entry from config matching service_name."""
    for src in config.get("source_rds_endpoints", []):
        if src["name"] == service_name:
            return src
    return None


def load_mirrors_for_service(service_name):
    """Load peerdb_mirrors.json and return mirrors for this service."""
    try:
        with open(MIRRORS_REPORT) as f:
            data = json.load(f)
        return [m for m in data.get("mirrors", []) if m.get("source_rds") == service_name]
    except FileNotFoundError:
        return []


def load_peers_report():
    try:
        with open(PEERS_REPORT) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"peers": []}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def pg_connect(host, port, dbname, user):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user,
        connect_timeout=5,
    )


def list_databases(host, port, user):
    """List non-system databases on a given host."""
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datname FROM pg_database
                WHERE datname NOT IN %s AND datistemplate = false
                ORDER BY datname
            """, (tuple(SYSTEM_DBS),))
            dbs = [row[0] for row in cur.fetchall()]
        conn.close()
        return dbs
    except Exception as e:
        return []


def get_connections_by_db(host, port, user):
    """Returns dict: {datname -> {state -> count}} for non-system databases."""
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    datname,
                    COALESCE(state, 'unknown') AS state,
                    count(*)::int
                FROM pg_stat_activity
                WHERE datname NOT IN %s
                  AND pid <> pg_backend_pid()
                GROUP BY datname, state
                ORDER BY datname, count(*) DESC
            """, (tuple(SYSTEM_DBS),))
            rows = cur.fetchall()
        conn.close()
        result = {}
        for datname, state, count in rows:
            result.setdefault(datname, {})[state] = count
        return result
    except Exception as e:
        return {"__error__": str(e)[:60]}


def get_sequences(host, port, user, dbname):
    """Returns dict: {schema.seq_name -> last_value}."""
    try:
        conn = pg_connect(host, port, dbname, user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            # Discover sequences with their last_value
            cur.execute("""
                SELECT
                    n.nspname || '.' || s.relname AS seq_fqn,
                    p.last_value
                FROM pg_class s
                JOIN pg_namespace n ON n.oid = s.relnamespace
                JOIN LATERAL pg_sequence_last_value(s.oid) p(last_value) ON true
                WHERE s.relkind = 'S'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY seq_fqn
            """)
            rows = cur.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    except Exception:
        # pg_sequence_last_value is PG14+; fall back to information_schema
        try:
            conn = pg_connect(host, port, dbname, user)
            conn.set_session(readonly=True, autocommit=True)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sequence_schema || '.' || sequence_name
                    FROM information_schema.sequences
                    ORDER BY 1
                """)
                names = [row[0] for row in cur.fetchall()]
            results = {}
            for fqn in names:
                schema, name = fqn.split(".", 1)
                try:
                    with conn.cursor() as cur:
                        cur.execute(f'SELECT last_value FROM "{schema}"."{name}"')
                        results[fqn] = cur.fetchone()[0]
                except Exception:
                    results[fqn] = None
            conn.close()
            return results
        except Exception as e:
            return {"error": str(e)[:60]}


# ---------------------------------------------------------------------------
# DNS helpers
# ---------------------------------------------------------------------------

def resolve_cname(hostname):
    """Return the CNAME target of a hostname, or None on failure."""
    try:
        result = subprocess.run(
            ["dig", "+short", "+time=3", "+tries=1", "CNAME", hostname],
            capture_output=True, text=True, timeout=5,
        )
        value = result.stdout.strip().rstrip(".")
        return value if value else None
    except Exception:
        pass
    # Fallback: host command
    try:
        result = subprocess.run(
            ["host", "-t", "CNAME", hostname],
            capture_output=True, text=True, timeout=5,
        )
        m = re.search(r"is an alias for (.+)\.", result.stdout)
        return m.group(1).strip() if m else None
    except Exception:
        return None


def classify_cname(value, source_endpoint, aurora_endpoint):
    """Returns (label, color) based on where the CNAME points."""
    if not value:
        return "UNRESOLVED", C_RED
    if source_endpoint and source_endpoint in value:
        return "→ SOURCE", C_YELLOW
    if aurora_endpoint and aurora_endpoint in value:
        return "→ AURORA", C_GREEN
    return f"→ ?", C_GRAY


# ---------------------------------------------------------------------------
# PeerDB helpers
# ---------------------------------------------------------------------------

def peerdb_session(api_url, auth_header):
    session = requests.Session()
    if auth_header and ":" in auth_header:
        name, value = auth_header.split(":", 1)
        session.headers.update({name.strip(): value.strip()})
    session.headers.update({"Content-Type": "application/json"})
    session.base_url = api_url.rstrip("/")
    return session


def get_mirror_status(session, flow_job_name):
    """Returns full status dict from PeerDB API."""
    try:
        resp = session.post(
            f"{session.base_url}/api/v1/mirrors/status",
            json={"flowJobName": flow_job_name},
            timeout=5,
        )
        if resp.status_code in (200, 201):
            return resp.json()
        return {}
    except Exception:
        return {}


def extract_mirror_details(status_data):
    """Extract sync_interval_s, rows_synced from PeerDB status response."""
    sync_interval = None
    rows_synced   = None

    # PeerDB CDC flow config
    cdc_config = status_data.get("cdcFlowConfigProto") or {}
    if isinstance(cdc_config, dict):
        sync_interval = cdc_config.get("idleTimeoutSeconds") or cdc_config.get("maxBatchSize")

    # Stats — field names vary across PeerDB versions
    for stats_key in ("cdcFlowStats", "flowStats", "stats"):
        stats = status_data.get(stats_key) or {}
        if isinstance(stats, dict):
            for field in ("totalRowsSynced", "rowsSynced", "numRowsSynced", "rows_synced"):
                val = stats.get(field)
                if val is not None:
                    rows_synced = val
                    break
        if rows_synced is not None:
            break

    return sync_interval, rows_synced


def get_slot_lag(host, port, user, slot_name):
    """Returns (lag_bytes, lag_pretty) or (None, 'N/A')."""
    try:
        conn = pg_connect(host, port, "postgres", user)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
                FROM pg_replication_slots
                WHERE slot_name = %s
            """, (slot_name,))
            row = cur.fetchone()
        conn.close()
        if row:
            return row[0], row[1]
        return None, "no slot"
    except Exception:
        return None, "err"


# ---------------------------------------------------------------------------
# Datadog helper
# ---------------------------------------------------------------------------

def get_dd_apm_errors(service_name, dd_api_key, dd_app_key, dd_site="datadoghq.com"):
    """Returns (error_rate_str, total_errors_str) or (None, error_msg)."""
    try:
        now_s  = int(time.time())
        from_s = now_s - 300  # last 5 min
        url = f"https://api.{dd_site}/api/v1/query"
        resp = requests.get(
            url,
            params={
                "from":  from_s,
                "to":    now_s,
                "query": f"sum:trace.web.request.errors{{service:{service_name}}}.as_rate()",
            },
            headers={
                "DD-API-KEY":         dd_api_key,
                "DD-APPLICATION-KEY": dd_app_key,
            },
            timeout=8,
        )
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"
        data   = resp.json()
        series = data.get("series", [])
        if not series:
            return "0.00/s", "no data"
        points = [p[1] for p in series[0].get("pointlist", []) if p[1] is not None]
        avg = sum(points) / len(points) if points else 0
        last = points[-1] if points else 0
        return f"{last:.2f}/s", f"avg 5m: {avg:.2f}/s"
    except Exception as e:
        return None, str(e)[:60]


# ---------------------------------------------------------------------------
# ECS helpers
# ---------------------------------------------------------------------------

def load_ecs_services_for(service_name, discovery_report_path=DISCOVERY_REPORT):
    """Return list of {cluster, service, service_arn} for a given RDS service name."""
    try:
        with open(discovery_report_path) as f:
            report = json.load(f)
    except FileNotFoundError:
        return []

    endpoint_data = report.get("endpoints", {}).get(service_name, {})
    matches = endpoint_data.get("matches", [])

    # Deduplicate by service_arn
    seen = set()
    services = []
    for m in matches:
        arn = m.get("service_arn", "")
        if arn and arn not in seen:
            seen.add(arn)
            services.append({
                "cluster":     m.get("cluster", ""),
                "service":     m.get("service", ""),
                "service_arn": arn,
            })
    return services


def get_ecs_service_status(cluster, service_arn, region="us-east-1", profile=None):
    """Query ECS describe_services and return a status dict."""
    try:
        session = boto3.Session(profile_name=profile, region_name=region)
        client  = session.client("ecs")
        resp    = client.describe_services(cluster=cluster, services=[service_arn])
        svcs    = resp.get("services", [])
        if not svcs:
            return {"error": "not found"}
        svc = svcs[0]

        # Active deployment
        deployments  = svc.get("deployments", [])
        primary      = next((d for d in deployments if d["status"] == "PRIMARY"), None)
        deploy_status = primary.get("rolloutState", primary.get("status", "?")) if primary else "?"

        return {
            "status":          svc.get("status", "?"),
            "desired":         svc.get("desiredCount", 0),
            "running":         svc.get("runningCount", 0),
            "pending":         svc.get("pendingCount", 0),
            "task_definition": svc.get("taskDefinition", "").split("/")[-1],
            "deploy_status":   deploy_status,
        }
    except botocore.exceptions.NoCredentialsError:
        return {"error": "no AWS credentials"}
    except botocore.exceptions.ClientError as e:
        return {"error": str(e)[:50]}
    except Exception as e:
        return {"error": str(e)[:50]}


# ---------------------------------------------------------------------------
# Render sections
# ---------------------------------------------------------------------------

def section_header(n, title, use_color):
    label = f"[{n}] {title}"
    if use_color:
        label = colorize(C_BOLD, label)
    return label


def render_dns(source, cnames, aurora_endpoint, use_color):
    lines = []
    CW = {"name": 45, "type": 6, "value": 62, "status": 10}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(name, typ, val, status_text, status_color=""):
        sc = status_color if use_color else ""
        return (
            f"| {rpad(name, CW['name'])} "
            f"| {rpad(typ,  CW['type'])} "
            f"| {rpad(val,  CW['value'])} "
            f"| {rpad(sc + status_text + (C_RESET if sc else ''), CW['status'])} |"
        )

    lines.append(sep)
    lines.append(row("RECORD NAME", "TYPE", "CURRENT VALUE", "POINTS TO"))
    lines.append(sep)

    source_endpoint = source.get("endpoint", "")

    for cname in cnames:
        value  = resolve_cname(cname) or ""
        label, color = classify_cname(value, source_endpoint, aurora_endpoint)
        val_display = (value[:60] + "…") if len(value) > 62 else value
        lines.append(row(cname, "CNAME", val_display, label, color))

    lines.append(sep)
    return lines


def render_peerdb(session, mirrors, source_host, user, port, use_color):
    lines = []
    CW = {"mirror": 38, "status": 10, "interval": 10, "rows": 14, "lag": 14}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(mirror, status, interval, rows, lag, status_color=""):
        sc = status_color if use_color else ""
        return (
            f"| {rpad(mirror,   CW['mirror'])} "
            f"| {rpad(sc + status + (C_RESET if sc else ''), CW['status'])} "
            f"| {lpad(interval, CW['interval'])} "
            f"| {lpad(rows,     CW['rows'])} "
            f"| {lpad(lag,      CW['lag'])} |"
        )

    lines.append(sep)
    lines.append(row("MIRROR", "STATUS", "SYNC INTV", "ROWS SYNCED", "SLOT LAG"))
    lines.append(sep)

    if not mirrors:
        lines.append(row("(no mirrors found)", "", "", "", ""))
        lines.append(sep)
        return lines

    for m in mirrors:
        flow_name = m["flow_job_name"]
        slot_name = m["replication_slot"]

        status_data    = get_mirror_status(session, flow_name) if session else {}
        state          = status_data.get("currentFlowState", "UNKNOWN")
        state_short    = state.replace("STATUS_", "")
        state_color    = STATE_COLOR.get(state, C_GRAY)
        sync_iv, rows  = extract_mirror_details(status_data)
        _, lag_pretty  = get_slot_lag(source_host, port, user, slot_name) if source_host else (None, "N/A")

        interval_str = f"{sync_iv}s" if sync_iv is not None else "N/A"
        rows_str     = f"{rows:,}"   if isinstance(rows, int) else "N/A"

        lines.append(row(flow_name, state_short, interval_str, rows_str, lag_pretty, state_color))

    lines.append(sep)
    return lines


def render_connections(source_host, target_host, user, port, use_color):
    lines = []

    src_data = get_connections_by_db(source_host, port, user) if source_host else {}
    tgt_data = get_connections_by_db(target_host, port, user) if target_host else {}

    STATE_SHORT = {
        "active":                          "active",
        "idle":                            "idle",
        "idle in transaction":             "idle in txn",
        "idle in transaction (aborted)":   "idle in txn (abrt)",
        "fastpath function call":          "fastpath",
        "disabled":                        "disabled",
        "unknown":                         "unknown",
    }

    CW = {"db": 28, "state": 20, "src": 8, "tgt": 8}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"
    mid = sep

    def row(db, state, src, tgt, state_color=""):
        sc = state_color if use_color else ""
        return (
            f"| {rpad(db,    CW['db'])} "
            f"| {rpad(sc + state + (C_RESET if sc else ''), CW['state'])} "
            f"| {lpad(src,   CW['src'])} "
            f"| {lpad(tgt,   CW['tgt'])} |"
        )

    lines.append(sep)
    lines.append(row("DATABASE", "STATE", "SOURCE", "TARGET"))
    lines.append(sep)

    # Connection error
    if "__error__" in src_data or "__error__" in tgt_data:
        err = src_data.get("__error__") or tgt_data.get("__error__", "")
        lines.append(row("ERROR", err[:20], "", ""))
        lines.append(sep)
        return lines

    all_dbs = sorted(set(list(src_data.keys()) + list(tgt_data.keys())))

    if not all_dbs:
        lines.append(row("(no connections)", "", "0", "0"))
    else:
        total_src = total_tgt = 0
        for db in all_dbs:
            src_states = src_data.get(db, {})
            tgt_states = tgt_data.get(db, {})
            all_states = sorted(
                set(list(src_states.keys()) + list(tgt_states.keys())),
                key=lambda s: (s != "active", s != "idle", s),
            )
            first = True
            for state in all_states:
                src_n = src_states.get(state, 0)
                tgt_n = tgt_states.get(state, 0)
                total_src += src_n
                total_tgt += tgt_n

                label = STATE_SHORT.get(state, state[:20])
                s_color = ""
                if use_color:
                    if state == "active":
                        s_color = C_GREEN
                    elif "aborted" in state or state == "__error__":
                        s_color = C_RED
                    elif "transaction" in state:
                        s_color = C_YELLOW

                db_col = db if first else ""
                lines.append(row(db_col, label, str(src_n), str(tgt_n), s_color))
                first = False

            lines.append(mid)

    # Total footer
    total_src = sum(
        n for db_states in src_data.values() if isinstance(db_states, dict)
        for n in db_states.values()
    )
    total_tgt = sum(
        n for db_states in tgt_data.values() if isinstance(db_states, dict)
        for n in db_states.values()
    )
    lines.append(row("TOTAL", "", str(total_src), str(total_tgt)))
    lines.append(sep)
    return lines


def render_sequences(source_host, target_host, databases, user, port, use_color):
    lines = []
    CW = {"seq": 48, "src": 14, "tgt": 14, "delta": 14}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(seq, src, tgt, delta, delta_color=""):
        dc = delta_color if use_color else ""
        return (
            f"| {rpad(seq,   CW['seq'])} "
            f"| {lpad(src,   CW['src'])} "
            f"| {lpad(tgt,   CW['tgt'])} "
            f"| {lpad(dc + delta + (C_RESET if dc else ''), CW['delta'])} |"
        )

    lines.append(sep)
    lines.append(row("SEQUENCE  (db.schema.name)", "SOURCE", "TARGET", "DELTA"))
    lines.append(sep)

    any_seq = False
    for dbname in sorted(databases):
        src_seqs = get_sequences(source_host, port, user, dbname) if source_host else {}
        tgt_seqs = get_sequences(target_host, port, user, dbname) if target_host else {}

        if "error" in src_seqs or "error" in tgt_seqs:
            err = src_seqs.get("error") or tgt_seqs.get("error") or ""
            lines.append(row(f"{dbname}: ERROR — {err[:38]}", "", "", ""))
            any_seq = True
            continue

        all_seqs = sorted(set(list(src_seqs.keys()) + list(tgt_seqs.keys())))
        for seq_fqn in all_seqs:
            any_seq = True
            display = f"{dbname}.{seq_fqn}"
            display = (display[:46] + "…") if len(display) > 48 else display

            sv = src_seqs.get(seq_fqn)
            tv = tgt_seqs.get(seq_fqn)

            src_str = f"{sv:,}" if sv is not None else "N/A"
            tgt_str = f"{tv:,}" if tv is not None else "N/A"

            if sv is not None and tv is not None:
                delta   = tv - sv
                d_str   = f"+{delta:,}" if delta >= 0 else f"{delta:,}"
                # color: green = target ahead (good), red = target behind (bad)
                d_color = C_GREEN if delta >= 0 else C_RED
            else:
                d_str   = "N/A"
                d_color = C_GRAY

            lines.append(row(display, src_str, tgt_str, d_str, d_color))

    if not any_seq:
        lines.append(row("(no sequences found)", "", "", ""))

    lines.append(sep)
    return lines


def render_ecs(service_name, ecs_services, region, aws_profile, use_color):
    lines = []
    CW = {"cluster": 30, "service": 38, "status": 8, "deploy": 14, "desired": 7, "running": 7, "pending": 7}
    sep = "+" + "+".join("-" * (w + 2) for w in CW.values()) + "+"

    def row(cluster, service, status, deploy, desired, running, pending,
            status_color="", deploy_color=""):
        sc = status_color if use_color else ""
        dc = deploy_color if use_color else ""
        return (
            f"| {rpad(cluster, CW['cluster'])} "
            f"| {rpad(service, CW['service'])} "
            f"| {rpad(sc + status + (C_RESET if sc else ''), CW['status'])} "
            f"| {rpad(dc + deploy + (C_RESET if dc else ''), CW['deploy'])} "
            f"| {lpad(desired, CW['desired'])} "
            f"| {lpad(running, CW['running'])} "
            f"| {lpad(pending, CW['pending'])} |"
        )

    lines.append(sep)
    lines.append(row("CLUSTER", "ECS SERVICE", "STATUS", "DEPLOYMENT",
                     "DESIRED", "RUNNING", "PENDING"))
    lines.append(sep)

    if not ecs_services:
        lines.append(row("(no ECS services found in discovery report)", "", "", "", "", "", ""))
        lines.append(sep)
        return lines

    for svc in ecs_services:
        info = get_ecs_service_status(svc["cluster"], svc["service_arn"], region, aws_profile)

        if "error" in info:
            lines.append(row(
                svc["cluster"][:30], svc["service"][:38],
                "ERR", info["error"][:14], "", "", "",
                status_color=C_RED,
            ))
            continue

        status  = info["status"]
        desired = str(info["desired"])
        running = str(info["running"])
        pending = str(info["pending"])
        deploy  = info["deploy_status"][:14]

        # Color logic
        s_color = C_GREEN if status == "ACTIVE" else C_YELLOW
        healthy = info["running"] == info["desired"] and info["pending"] == 0
        d_color = C_GREEN if healthy else (C_YELLOW if info["pending"] > 0 else C_RED)

        lines.append(row(
            svc["cluster"][:30], svc["service"][:38],
            status, deploy, desired, running, pending,
            status_color=s_color, deploy_color=d_color,
        ))

    lines.append(sep)
    return lines


def render_datadog(service_name, use_color):
    lines = []
    dd_api_key = os.environ.get("DD_API_KEY", "")
    dd_app_key = os.environ.get("DD_APP_KEY", "")
    dd_site    = os.environ.get("DD_SITE", "datadoghq.com")

    if not dd_api_key or not dd_app_key:
        lines.append("  DD_API_KEY / DD_APP_KEY not set — skipping Datadog section.")
        return lines

    rate, detail = get_dd_apm_errors(service_name, dd_api_key, dd_app_key, dd_site)

    if rate is None:
        msg = f"  ERROR querying Datadog: {detail}"
        lines.append(colorize(C_RED, msg) if use_color else msg)
    else:
        color = C_RED if rate != "0.00/s" else C_GREEN
        rate_str = colorize(color, rate) if use_color else rate
        lines.append(f"  Error rate (last 5 min): {rate_str}  [{detail}]")
        lines.append(f"  Service: {service_name}  |  Site: {dd_site}")

    return lines


# ---------------------------------------------------------------------------
# Full render
# ---------------------------------------------------------------------------

def render(service_name, source, cnames, source_host, target_host,
           aurora_endpoint, mirrors, session, databases,
           ecs_services, region, aws_profile,
           user, port, use_color):

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    width = 80

    title = f" CUTOVER DASHBOARD — {service_name} "
    bar   = "═" * ((width - len(re.sub(r'\033\[[0-9;]*m', '', title))) // 2)
    header = f"{bar}{title}{bar}"
    if use_color:
        header = colorize(C_BOLD, header)

    print(f"\n{header}")
    print(f"  {now}\n")

    # [1] DNS
    print(section_header(1, "DNS RECORDS", use_color))
    for line in render_dns(source, cnames, aurora_endpoint, use_color):
        print(line)
    print()

    # [2] PeerDB
    print(section_header(2, "PEERDB MIRRORS", use_color))
    for line in render_peerdb(session, mirrors, source_host, user, port, use_color):
        print(line)
    print()

    # [3] Connections
    print(section_header(3, "CONNECTIONS  SOURCE vs TARGET", use_color))
    for line in render_connections(source_host, target_host, user, port, use_color):
        print(line)
    print()

    # [4] Sequences
    print(section_header(4, f"SEQUENCES  SOURCE vs TARGET  [{', '.join(databases)}]", use_color))
    for line in render_sequences(source_host, target_host, databases, user, port, use_color):
        print(line)
    print()

    # [5] Datadog
    print(section_header(5, "DATADOG APM", use_color))
    for line in render_datadog(service_name, use_color):
        print(line)
    print()

    # [6] ECS
    print(section_header(6, "ECS SERVICES", use_color))
    for line in render_ecs(service_name, ecs_services, region, aws_profile, use_color):
        print(line)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cutover dashboard — monitor one microservice during RDS→Aurora migration."
    )
    parser.add_argument("--service",  required=True,
                        help="RDS service name, e.g. bo-risk-monitoring-engine-pgsql-prd")
    parser.add_argument("--config",   default="config/catalog_services_migration.yaml",
                        help="Migration YAML config (default: config/catalog_services_migration.yaml)")
    parser.add_argument("--watch",    type=int, metavar="SECONDS",
                        help="Auto-refresh interval in seconds (default: run once)")
    parser.add_argument("--user",     default=DEFAULT_USER)
    parser.add_argument("--port",     type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--api-url",
                        default=os.environ.get("PEERDB_API_URL", ""),
                        help="PeerDB API URL (or set PEERDB_API_URL)")
    parser.add_argument("--auth-header",
                        default=os.environ.get("PEERDB_AUTH_HEADER", ""),
                        help="PeerDB auth header (or set PEERDB_AUTH_HEADER)")
    parser.add_argument("--peers-report",      default=PEERS_REPORT)
    parser.add_argument("--mirrors-report",    default=MIRRORS_REPORT)
    parser.add_argument("--discovery-report",  default=DISCOVERY_REPORT)
    parser.add_argument("--region",            default="us-east-1",
                        help="AWS region for ECS queries (default: us-east-1)")
    parser.add_argument("--aws-profile",       default=None,
                        help="AWS profile for ECS queries (default: env default)")
    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    source = find_service(config, args.service)
    if not source:
        print(f"ERROR: service '{args.service}' not found in {args.config}", file=sys.stderr)
        print("Available services:")
        for s in config.get("source_rds_endpoints", []):
            print(f"  {s['name']}")
        sys.exit(1)

    source_host    = source["endpoint"]
    cnames         = source.get("cnames", [])
    aurora_clusters = config.get("target_aurora_clusters", [])
    aurora_endpoint = aurora_clusters[0]["endpoint"] if aurora_clusters else ""
    target_host     = aurora_endpoint

    # ECS services for this RDS
    ecs_services = load_ecs_services_for(args.service, args.discovery_report)
    if not ecs_services:
        print(f"WARNING: no ECS services found in {args.discovery_report} for '{args.service}'",
              file=sys.stderr)

    # Mirrors for this service
    mirrors = load_mirrors_for_service(args.service)
    if not mirrors:
        print(f"WARNING: no mirrors found in {args.mirrors_report} for '{args.service}'",
              file=sys.stderr)

    # PeerDB session
    session = None
    if args.api_url and args.auth_header:
        session = peerdb_session(args.api_url, args.auth_header)
    else:
        print("WARNING: PEERDB_API_URL / PEERDB_AUTH_HEADER not set — PeerDB panel will be empty.",
              file=sys.stderr)

    # Discover databases on source
    databases = list_databases(source_host, args.port, args.user)
    if not databases:
        # Fallback: infer from mirror list
        databases = [m["database"] for m in mirrors] or ["postgres"]

    use_color = not args.no_color and sys.stdout.isatty()

    if args.watch:
        try:
            while True:
                if use_color:
                    print("\033[2J\033[H", end="")  # clear screen
                render(
                    args.service, source, cnames, source_host, target_host,
                    aurora_endpoint, mirrors, session, databases,
                    ecs_services, args.region, args.aws_profile,
                    args.user, args.port, use_color,
                )
                print(f"\n  Refreshing every {args.watch}s — Ctrl+C to stop")
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        render(
            args.service, source, cnames, source_host, target_host,
            aurora_endpoint, mirrors, session, databases,
            args.user, args.port, use_color,
        )


if __name__ == "__main__":
    main()
