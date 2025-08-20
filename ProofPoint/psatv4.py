#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import base64
import logging
import configparser
from typing import Optional, Tuple, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("proofpoint_api.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# =========================
# API constants
# =========================
BASE_URL = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"
REPORT_TYPES = ["users", "phishing"]  # enable more later if needed

# =========================
# DB schema (fixed) – table -> {column: pg_type}
# Adjust as needed to match exactly what you want to store.
# =========================
DB_SCHEMAS: Dict[str, Dict[str, str]] = {
    "Mod_ThreatAwareness_Phishing": {
        # identifiers / types
        "id": "TEXT",
        "type": "TEXT",
        # event fields
        "eventtype": "TEXT",
        "eventtimestamp": "TIMESTAMPTZ",
        "senttimestamp": "TIMESTAMPTZ",
        # campaign metadata
        "campaigntype": "TEXT",
        "campaignname": "TEXT",
        "campaignstatus": "TEXT",
        "campaignstartdate": "TIMESTAMPTZ",
        "campaignenddate": "TIMESTAMPTZ",
        # template
        "templatename": "TEXT",
        "templatesubject": "TEXT",
        # flags
        "assessmentsarchived": "BOOLEAN",
        "autoenrollment": "BOOLEAN",
        # user identity
        "sso_id": "TEXT",
        "useremailaddress": "TEXT",
        "userfirstname": "TEXT",
        "userlastname": "TEXT",
        "useractiveflag": "BOOLEAN",
        "userdeleteddate": "TIMESTAMPTZ",
        # optional raw usertags (JSON-encoded as text)
        "usertags": "TEXT",
        # row hash for update-on-change
        "row_hash": "TEXT",
    },
    "Mod_ThreatAwareness_Users": {
        "id": "TEXT",
        "type": "TEXT",
        "useremailaddress": "TEXT",
        "userfirstname": "TEXT",
        "userlastname": "TEXT",
        "sso_id": "TEXT",
        "department_1": "TEXT",
        "location_1": "TEXT",
        "office_location_1": "TEXT",
        "manager_email_address_1": "TEXT",
        "title_1": "TEXT",
        "useractiveflag": "BOOLEAN",
        "userdeleteddate": "TIMESTAMPTZ",
        "userlocale": "TEXT",
        "usertimezone": "TEXT",
        "datalastupdated": "TIMESTAMPTZ",
        "row_hash": "TEXT",
    },
}

# report -> destination table mapping
TABLE_MAPPING: Dict[str, str] = {
    "phishing": "Mod_ThreatAwareness_Phishing",
    "users": "Mod_ThreatAwareness_Users",
}

# =========================
# Config / credentials
# =========================
def read_api_key() -> str:
    """Get API key from env(API_KEY) or api_key.txt."""
    env_key = os.getenv("API_KEY")
    if env_key:
        return env_key.strip()
    try:
        with open("api_key.txt", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.error("Provide API key via env var API_KEY or create api_key.txt.")
        raise

def read_db_config(filename: str = "proofpoint.ini", section: str = "postgresql") -> Dict[str, str]:
    """Read DB config; supports base64-encoded or plain INI."""
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Config file {filename} not found")

    with open(filename, "r") as fh:
        content = fh.read()

    parser = configparser.ConfigParser()
    try:
        decoded = base64.b64decode(content).decode("utf-8")
        parser.read_string(decoded)
    except base64.binascii.Error:
        parser.read(filename)

    if not parser.has_section(section):
        raise RuntimeError(f"Section [{section}] not found in {filename}")

    db = {k: v for k, v in parser.items(section)}
    for key in ["host", "port", "dbname", "user", "password"]:
        if key not in db:
            raise RuntimeError(f"DB config missing '{key}'")
    return db

def test_db_connection(db: Dict[str, str]) -> bool:
    try:
        with psycopg2.connect(
            host=db["host"], port=db["port"], dbname=db["dbname"], user=db["user"], password=db["password"]
        ):
            pass
        logger.info("Database connection test successful.")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

# =========================
# Watermark state table
# =========================
def ensure_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_sync_state (
              report_type TEXT PRIMARY KEY,
              last_success_ts TIMESTAMPTZ,
              last_seen_id TEXT
            );
        """)
    conn.commit()

def get_last_seen_id(conn, report_type: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT last_seen_id FROM etl_sync_state WHERE report_type=%s", (report_type,))
        row = cur.fetchone()
        return row[0] if row else None

def set_last_seen_id(conn, report_type: str, last_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_sync_state (report_type, last_success_ts, last_seen_id)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (report_type) DO UPDATE
              SET last_success_ts = EXCLUDED.last_success_ts,
                  last_seen_id = EXCLUDED.last_seen_id;
        """, (report_type, last_id))
    conn.commit()

# =========================
# Table creation helpers
# =========================
def ensure_table(conn, table_name: str, schema: Dict[str, str]) -> None:
    """
    Create table if not exists using fixed schema.
    'id' is PRIMARY KEY (needed for upsert).
    """
    cols_sql: List[str] = []
    for col, pg_type in schema.items():
        if col == "id":
            cols_sql.append(f'"{col}" {pg_type} PRIMARY KEY')
        else:
            cols_sql.append(f'"{col}" {pg_type}')
    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_sql)});'
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def ensure_unique_index_on_id(conn, table_name: str) -> None:
    idx = f'{table_name.lower()}_id_uidx'.replace('"', "").replace(".", "_")
    with conn.cursor() as cur:
        cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {idx} ON "{table_name}"("id");')
    conn.commit()

# =========================
# Type casting helpers (incl. numpy.bool_ → bool fix)
# =========================
def to_python_bool(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    if s in {"true", "t", "1", "yes"}:
        return True
    if s in {"false", "f", "0", "no"}:
        return False
    try:
        return bool(int(s))
    except Exception:
        return bool(v)

def cast_dataframe_types(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """
    Coerce columns to expected types per schema.
    Ensures BOOLEAN columns are native Python bool (not numpy.bool_).
    """
    if df.empty:
        return df.copy()

    out = pd.DataFrame()
    for col, pgtype in schema.items():
        if col not in df.columns:
            out[col] = None
            continue

        series = df[col]
        pg = pgtype.upper()
        if pg == "TIMESTAMPTZ":
            out[col] = pd.to_datetime(series, errors="coerce", utc=True)
        elif pg == "BOOLEAN":
            out[col] = series.apply(to_python_bool)  # <-- ensures native Python bool / None
        else:
            # TEXT (or others) → keep as string; preserve None
            out[col] = series.astype("string").where(~series.isna(), None)

    # psycopg2-friendly NULLs
    out = out.where(pd.notnull(out), None)
    return out

# =========================
# Hash for update-on-change
# =========================
def compute_row_hash(row: pd.Series, cols: List[str]) -> str:
    payload = {c: (None if pd.isna(row[c]) else row[c]) for c in cols if c in row.index}
    s = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    import hashlib
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# =========================
# API helpers
# =========================
def extract_user_tags(attributes: dict) -> dict:
    out = {}
    ut = attributes.get("usertags", {}) or {}
    for cat, vals in ut.items():
        cat = str(cat).lower().replace("_1", "")
        if vals is None:
            continue
        if not isinstance(vals, list):
            vals = [vals]
        for i, v in enumerate(vals):
            if v is not None:
                out[f"{cat}_{i+1}"] = v
    return out

def fetch_report(report_type: str, api_key: str, seen_id: Optional[str]) -> Tuple[pd.DataFrame, Optional[str]]:
