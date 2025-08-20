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
# DB schema (whitelists + types)
# Keys = destination table names; values = {column: pg_type}
# Adjust as needed to match exactly what you want to store.
# =========================
DB_SCHEMAS: Dict[str, Dict[str, str]] = {
    "Mod_ThreatAwareness_Phishing": {
        # core
        "id": "TEXT",
        "type": "TEXT",
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
        # row_hash for update-on-change
        "row_hash": "TEXT"
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
        "row_hash": "TEXT"
    }
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
# Type casting helpers
# =========================
def to_bool_series(s: pd.Series) -> pd.Series:
    return s.map(
        lambda v: None if pd.isna(v)
        else bool(int(v)) if str(v).isdigit()
        else str(v).strip().lower() in {"true", "t", "1", "yes"}
    ).astype("boolean")

def cast_dataframe_types(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Coerce columns to expected types per schema."""
    if df.empty:
        return df.copy()

    out = pd.DataFrame()
    for col, pgtype in schema.items():
        if col not in df.columns:
            out[col] = pd.NA
            continue

        series = df[col]
        if pgtype.upper() == "TIMESTAMPTZ":
            out[col] = pd.to_datetime(series, errors="coerce", utc=True)
        elif pgtype.upper() == "BOOLEAN":
            out[col] = to_bool_series(series)
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
# API fetch (pagination + early stop by last_seen_id)
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
    """
    Fetch a report incrementally:
      - follow links.next if present; else page[number]
      - stop when previously-seen id encountered
      - return DataFrame + newest_id (top-most id from first non-empty page)
    """
    session = requests.Session()
    headers = {"x-apikey-token": api_key}

    # first URL
    api_url = f"{BASE_URL}{report_type}?"
    if report_type == "users":
        api_url += "user_tag_enabled&"
    next_url = f"{api_url}page[size]=8000&filter[_includedeletedusers]=TRUE"

    page = 0
    newest_id = None
    stop = False
    rows: List[dict] = []

    while next_url and not stop:
        page += 1
        logger.info(f"[{report_type}] Fetching page {page} ...")
        try:
            resp = session.get(next_url, headers=headers, timeout=60)
        except requests.exceptions.RequestException as e:
            logger.error(f"[{report_type}] Request error: {e}")
            break

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"[{report_type}] Rate limit hit. Sleeping {retry_after}s...")
            time.sleep(retry_after)
            continue

        if resp.status_code != 200:
            logger.error(f"[{report_type}] API returned {resp.status_code}: {resp.text[:300]}")
            break

        payload = resp.json()
        items = payload.get("data", []) or []
        if not items:
            break

        if newest_id is None:
            newest_id = items[0].get("id")

        for it in items:
            if seen_id and it.get("id") == seen_id:
                logger.info(f"[{report_type}] Reached previously seen id={seen_id}; stopping.")
                stop = True
                break

            attrs = it.get("attributes", {}) or {}
            row = {"id": it.get("id"), "type": it.get("type")}
            # flatten attributes (keep usertags encoded as JSON text)
            for k, v in attrs.items():
                if isinstance(v, (dict, list)):
                    if k == "usertags":
                        row["usertags"] = json.dumps(v, ensure_ascii=False)
                    continue
                row[k] = v

            # optionally expand usertags to columns for 'users' (skipped here)
            rows.append(row)

        # follow links.next or fallback to page[number]
        next_url = (payload.get("links") or {}).get("next")
        if not next_url:
            next_url = f"{api_url}page[number]={page+1}&page[size]=8000&filter[_includedeletedusers]=TRUE"

    logger.info(f"[{report_type}] fetched {len(rows)} new rows.")
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    # one known normalization example:
    if "location_1" in df.columns and pd.api.types.is_string_dtype(df["location_1"]):
        df["location_1"] = df["location_1"].str.replace("São Paulo", "Sao Paulo", regex=False)

    return df, newest_id

# =========================
# DB write (UPSERT, no truncates)
# =========================
def upsert_dataframe(conn, df: pd.DataFrame, table: str, schema: Dict[str, str]) -> None:
    """
    1) Align/cast to schema
    2) Compute row_hash
    3) Ensure table & index
    4) UPSERT with ON CONFLICT(id) DO UPDATE ... WHERE row changed
    """
    if df is None or df.empty:
        return

    # align & types
    aligned = cast_dataframe_types(df, schema)

    # compute row_hash on all columns except row_hash itself
    cols_for_hash = [c for c in aligned.columns if c != "row_hash"]
    aligned["row_hash"] = aligned.apply(lambda r: compute_row_hash(r, cols_for_hash), axis=1)

    # create table if missing
    ensure_table(conn, table, schema)
    ensure_unique_index_on_id(conn, table)

    cols = list(aligned.columns)
    col_sql = ", ".join([f'"{c}"' for c in cols])
    set_sql = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in ("id", "row_hash")] + ['"row_hash" = EXCLUDED."row_hash"']
    )
    upsert_sql = f"""
        INSERT INTO "{table}" ({col_sql})
        VALUES %s
        ON CONFLICT ("id") DO UPDATE
        SET {set_sql}
        WHERE "{table}"."row_hash" IS DISTINCT FROM EXCLUDED."row_hash";
    """

    values = [tuple(row) for row in aligned.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        execute_values(cur, upsert_sql, values, page_size=1000)
    conn.commit()
    logger.info(f'Upserted {len(aligned)} rows into "{table}".')

# =========================
# Main
# =========================
def main():
    logger.info("Starting Proofpoint → Postgres incremental sync (DB-only).")
    api_key = read_api_key()
    db = read_db_config("proofpoint.ini", "postgresql")
    if not test_db_connection(db):
        logger.error("DB connection failed. Exiting.")
        return

    conn_params = dict(host=db["host"], port=db["port"], dbname=db["dbname"], user=db["user"], password=db["password"])

    # read watermarks
    with psycopg2.connect(**conn_params) as conn:
        ensure_state_table(conn)
        seen_ids = {rt: get_last_seen_id(conn, rt) for rt in REPORT_TYPES}

    # fetch in parallel
    results: Dict[str, pd.DataFrame] = {}
    newest_ids: Dict[str, Optional[str]] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(REPORT_TYPES))) as ex:
        future_map = {ex.submit(fetch_report, rt, api_key, seen_ids.get(rt)): rt for rt in REPORT_TYPES}
        for fut in as_completed(future_map):
            rt = future_map[fut]
            try:
                df, top_id = fut.result()
                results[rt] = df
                newest_ids[rt] = top_id
            except Exception as e:
                logger.error(f"Error fetching {rt}: {e}")
                results[rt] = pd.DataFrame()
                newest_ids[rt] = None

    # upsert to DB
    with psycopg2.connect(**conn_params) as conn:
        for rt, df in results.items():
            table = TABLE_MAPPING.get(rt)
            if not table or df is None or df.empty:
                continue
            schema = DB_SCHEMAS[table]
            upsert_dataframe(conn, df, table, schema)

    # persist new watermarks
    with psycopg2.connect(**conn_params) as conn:
        for rt, nid in newest_ids.items():
            if nid:
                set_last_seen_id(conn, rt, nid)

    logger.info("Incremental sync complete.")

# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
