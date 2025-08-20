#!/usr/bin/env python3
import os
import time
import json
import base64
import hashlib
import logging
import configparser
from datetime import datetime
from typing import Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pandas.api.types import (
    is_integer_dtype,
    is_float_dtype,
    is_bool_dtype,
    is_datetime64_any_dtype,
)

# ---------------------------------
# Logging
# ---------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("proofpoint_api.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ---------------------------------
# Constants
# ---------------------------------
BASE_URL = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"
REPORT_TYPES = ["users", "phishing"]     # add "training" later if needed
DEFAULT_SSO_DOMAIN = "Org.com"    # can be overridden in [app] sso_domain

# Map API report -> DB table
TABLE_MAPPING = {
    "users": "Mod_ThreatAwareness_Users",
    "phishing": "Mod_ThreatAwareness_Phishing",
    # "training": "Mod_ThreatAwareness_Training",
}

# Keep only the columns you showed; cast timestamps/bools appropriately
DB_SCHEMAS: Dict[str, Dict] = {
    "phishing": {
        "keep": [
            "id",
            "type",
            "eventtype",
            "eventtimestamp",
            "senttimestamp",
            "campaigntype",
            "campaignname",
            "campaignstatus",
            "campaignstartdate",
            "campaignenddate",
            "templatename",
            "templatesubject",
            "assessmentsarchived",
            "autoenrollment",
            "sso_id",
            "useremailaddress",
            "userfirstname",
            "userlastname",
            "useractiveflag",
            "userdeleteddate",
            "usertags",             # will be JSON text if present
        ],
        "rename": {},
        "dtypes": {
            "eventtimestamp": "timestamp",
            "senttimestamp": "timestamp",
            "campaignstartdate": "timestamp",
            "campaignenddate": "timestamp",
            "userdeleteddate": "timestamp",
            "assessmentsarchived": "bool",
            "autoenrollment": "bool",
            "useractiveflag": "bool",
        },
    },
    "users": {
        "keep": [
            "id",
            "type",
            "useremailaddress",
            "userfirstname",
            "userlastname",
            "sso_id",
            "sso_id_email",               # we now keep this
            "department_1",
            "location_1",
            "office_location_1",
            "manager_email_address_1",
            "title_1",
            "useractiveflag",
            "userdeleteddate",
            "userlocale",
            "usertimezone",
            "datalastupdated",
        ],
        "rename": {},
        "dtypes": {
            "userdeleteddate": "timestamp",
            "datalastupdated": "timestamp",
            "useractiveflag": "bool",
        },
    },
    # "training": {...}
}

# ---------------------------------
# Config & credentials
# ---------------------------------
def read_api_key() -> str:
    env_key = os.getenv("API_KEY")
    if env_key:
        return env_key.strip()
    try:
        with open("api_key.txt", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.error("api_key.txt not found. Provide API_KEY env var or create api_key.txt.")
        raise

def read_config(filename="proofpoint.ini", section="postgresql") -> dict:
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
        raise RuntimeError(f"Section [{section}] not found in config")

    db = {k: v for k, v in parser.items(section)}
    # Force database name to Highlands_everest
    db["dbname"] = "Highlands_everest"

    # Optional app section
    sso_domain = DEFAULT_SSO_DOMAIN
    if parser.has_section("app"):
        sso_domain = parser.get("app", "sso_domain", fallback=DEFAULT_SSO_DOMAIN)
    db["sso_domain"] = sso_domain

    for param in ["host", "port", "dbname", "user", "password"]:
        if param not in db:
            raise RuntimeError(f"DB config missing '{param}'")
    return db

def test_db_connection(db_config) -> bool:
    conn_params = (
        f"host={db_config['host']} port={db_config['port']} "
        f"dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"
    )
    try:
        with psycopg2.connect(conn_params):
            pass
        logger.info("Database connection test successful.")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

# ---------------------------------
# Watermark state table
# ---------------------------------
def ensure_state_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_sync_state (
                report_type TEXT PRIMARY KEY,
                last_success_ts TIMESTAMPTZ,
                last_seen_id TEXT
            );
            """
        )
    conn.commit()

def get_last_seen_id(conn, report_type: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT last_seen_id FROM etl_sync_state WHERE report_type=%s", (report_type,))
        row = cur.fetchone()
        return row[0] if row else None

def set_last_seen_id(conn, report_type: str, last_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_sync_state (report_type, last_success_ts, last_seen_id)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (report_type) DO UPDATE
            SET last_success_ts = EXCLUDED.last_success_ts,
                last_seen_id = EXCLUDED.last_seen_id
            """,
            (report_type, last_id),
        )
    conn.commit()

# ---------------------------------
# Table creation / indexes
# ---------------------------------
def ensure_table_exists(cur, table_name: str, df: pd.DataFrame) -> None:
    """
    Create table if missing, inferring column types from df.
    'id' becomes PRIMARY KEY.
    """
    col_defs = []
    for col in df.columns:
        if is_integer_dtype(df[col]):
            sql_type = "BIGINT"
        elif is_float_dtype(df[col]):
            sql_type = "DOUBLE PRECISION"
        elif is_bool_dtype(df[col]):
            sql_type = "BOOLEAN"
        elif is_datetime64_any_dtype(df[col]):
            sql_type = "TIMESTAMPTZ"
        else:
            sql_type = "TEXT"
        if col == "id":
            col_defs.append(f'"{col}" {sql_type} PRIMARY KEY')
        else:
            col_defs.append(f'"{col}" {sql_type}')
    cols_sql = ", ".join(col_defs)
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_sql});')

def ensure_unique_index_on_id(cur, table_name: str) -> None:
    idx_name = f'{table_name.lower()}_id_uidx'.replace('"', "").replace(".", "_")
    cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON "{table_name}"("id");')

# ---------------------------------
# Data shaping helpers
# ---------------------------------
def coerce_dtype(series: pd.Series, target: str) -> pd.Series:
    if target == "text":
        return series.astype("string")
    if target == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if target == "float":
        return pd.to_numeric(series, errors="coerce")
    if target == "bool":
        return series.map(
            lambda v: None
            if pd.isna(v)
            else bool(int(v)) if str(v).isdigit()
            else str(v).strip().lower() in {"true", "t", "1", "yes"}
        ).astype("boolean")
    if target == "timestamp":
        return pd.to_datetime(series, errors="coerce", utc=True)
    return series

def build_db_dataframe(report_type: str, df: pd.DataFrame) -> pd.DataFrame:
    schema = DB_SCHEMAS.get(report_type, {})
    keep = schema.get("keep", [])
    rename = schema.get("rename", {})
    dtypes = schema.get("dtypes", {})

    # ensure kept columns exist
    for col in keep:
        if col not in df.columns:
            df[col] = pd.NA

    db_df = df[keep].copy()

    # rename if needed
    if rename:
        db_df.rename(columns=rename, inplace=True)

    # cast types
    for col, target in dtypes.items():
        target_col = rename.get(col, col)
        if target_col in db_df.columns:
            db_df[target_col] = coerce_dtype(db_df[target_col], target)

    # psycopg2-friendly nulls
    db_df = db_df.replace({pd.NA: None})
    return db_df

def compute_row_hash(row: pd.Series, cols: list) -> str:
    payload = {c: (None if pd.isna(row[c]) else row[c]) for c in cols if c in row.index}
    s = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

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

# ---------------------------------
# API fetch (incremental via pagination + last_seen_id)
# ---------------------------------
def extract_attributes(
    api_url: str, report_type: str, api_key: str, seen_id: Optional[str]
) -> Tuple[list, Optional[str]]:
    """
    Follow links.next when available; fall back to page[number].
    Stop early when previously-seen id appears.
    """
    extracted = []
    session = requests.Session()
    headers = {"x-apikey-token": api_key}

    next_url = f"{api_url}page[size]=8000&filter[_includedeletedusers]=TRUE"
    page = 0
    newest_id = None
    stop = False

    while next_url and not stop:
        page += 1
        logger.info(f"Fetching page {page} for {report_type}...")
        try:
            resp = session.get(next_url, headers=headers, timeout=60)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            break

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"Rate limit hit. Waiting {retry_after}s...")
            time.sleep(retry_after)
            continue

        if resp.status_code != 200:
            logger.error(f"API returned {resp.status_code}: {resp.text[:300]}")
            break

        payload = resp.json()
        items = payload.get("data", []) or []
        if not items:
            break

        if newest_id is None:
            newest_id = items[0].get("id")

        for it in items:
            if seen_id and it.get("id") == seen_id:
                logger.info(f"Reached previously seen id={seen_id}; stopping.")
                stop = True
                break

            attrs = it.get("attributes", {}) or {}
            row = {"type": it.get("type"), "id": it.get("id")}
            for k, v in attrs.items():
                # keep complex 'usertags' by JSON-encoding; skip other dict/list
                if isinstance(v, (dict, list)):
                    if k == "usertags":
                        row["usertags"] = json.dumps(v, ensure_ascii=False)
                    continue
                row[k] = v

            if report_type == "users" and "usertags" in attrs:
                # also expand user tags into flat columns for users (if you want)
                row.update(extract_user_tags(attrs))

            extracted.append(row)

        links = payload.get("links") or {}
        next_url = links.get("next")
        if not next_url:
            next_url = f"{api_url}page[number]={page+1}&page[size]=8000&filter[_includedeletedusers]=TRUE"

    logger.info(f"Total new/changed records fetched for {report_type}: {len(extracted)}")
    return extracted, newest_id

def process_report(
    report_type: str, api_key: str, sso_domain: str, seen_id: Optional[str]
) -> Tuple[pd.DataFrame, Optional[str]]:
    api_url = f"{BASE_URL}{report_type}?user_tag_enabled&" if report_type == "users" else f"{BASE_URL}{report_type}?"
    raw, newest_id = extract_attributes(api_url, report_type, api_key, seen_id)
    if not raw:
        return pd.DataFrame(), newest_id

    df = pd.DataFrame(raw)
    if df.empty:
        return pd.DataFrame(), newest_id

    # Normalize a known location quirk
    if "location_1" in df.columns and pd.api.types.is_string_dtype(df["location_1"]):
        df["location_1"] = df["location_1"].str.replace("São Paulo", "Sao Paulo", regex=False)

    # Keep sso_id_email (used in DB), but still filter out rows where useremailaddress == sso_id_email
    if report_type == "users" and {"useremailaddress", "sso_id"}.issubset(df.columns):
        df["sso_id_email"] = df["sso_id"].astype(str) + f"@{sso_domain}"
        df = df[~df["useremailaddress"].isin(df["sso_id_email"])]

    return df, newest_id

# ---------------------------------
# UPSERT (no truncate)
# ---------------------------------
def upsert_df(cur, table_name: str, df: pd.DataFrame) -> None:
    """
    Upsert using ON CONFLICT(id) DO UPDATE, but only when row_hash differs.
    Assumes table exists and has a unique/PK on "id".
    """
    cols = df.columns.tolist()
    if "row_hash" not in cols:
        cols.append("row_hash")

    col_sql = ", ".join([f'"{c}"' for c in df.columns])
    set_sql = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in ("id", "row_hash")]
        + ['"row_hash" = EXCLUDED."row_hash"']
    )
    upsert_sql = f"""
        INSERT INTO "{table_name}" ({col_sql})
        VALUES %s
        ON CONFLICT ("id") DO UPDATE
        SET {set_sql}
        WHERE "{table_name}"."row_hash" IS DISTINCT FROM EXCLUDED."row_hash";
    """

    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    execute_values(cur, upsert_sql, values, page_size=1000)

def save_to_postgres_inc(dataframes: dict, db_config: dict) -> None:
    conn_params = (
        f"host={db_config['host']} port={db_config['port']} "
        f"dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"
    )
    with psycopg2.connect(conn_params) as conn, conn.cursor() as cur:
        for rt, df_full in dataframes.items():
            if rt not in TABLE_MAPPING:
                continue
            if df_full is None or df_full.empty:
                continue

            table_name = TABLE_MAPPING[rt]

            # keep only DB columns + cast
            db_df = build_db_dataframe(rt, df_full)

            # compute row_hash on stored columns
            hash_cols = [c for c in db_df.columns if c != "row_hash"]
            db_df["row_hash"] = db_df.apply(lambda r: compute_row_hash(r, hash_cols), axis=1)

            # psycopg2 NULLs
            db_df = db_df.where(pd.notnull(db_df), None)

            # ensure table exists & unique index
            ensure_table_exists(cur, table_name, db_df)
            ensure_unique_index_on_id(cur, table_name)

            # upsert
            upsert_df(cur, table_name, db_df)

        conn.commit()

# ---------------------------------
# Main
# ---------------------------------
def main() -> None:
    logger.info("Starting incremental Proofpoint → Postgres sync (DB-only).")
    api_key = read_api_key()
    db_config = read_config(filename="proofpoint.ini", section="postgresql")

    # Force Highlands_everest
    db_config["dbname"] = "Highlands_everest"
    sso_domain = db_config.get("sso_domain", DEFAULT_SSO_DOMAIN)

    if not test_db_connection(db_config):
        logger.error("Cannot proceed without DB connection.")
        return

    conn_params = (
        f"host={db_config['host']} port={db_config['port']} "
        f"dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"
    )

    # watermarks
    seen_ids = {}
    with psycopg2.connect(conn_params) as conn:
        ensure_state_table_exists(conn)
        for rt in REPORT_TYPES:
            seen_ids[rt] = get_last_seen_id(conn, rt)

    # fetch in parallel
    dataframes = {}
    newest_ids = {}
    with ThreadPoolExecutor(max_workers=min(4, len(REPORT_TYPES))) as ex:
        futs = {
            ex.submit(process_report, rt, api_key, sso_domain, seen_ids.get(rt)): rt
            for rt in REPORT_TYPES
        }
        for fut in as_completed(futs):
            rt = futs[fut]
            try:
                df, newest_id = fut.result()
                dataframes[rt] = df
                newest_ids[rt] = newest_id
            except Exception as e:
                logger.error(f"Error processing {rt}: {e}")
                dataframes[rt] = pd.DataFrame()

    # upsert to DB
    save_to_postgres_inc(dataframes, db_config)

    # persist new watermarks
    with psycopg2.connect(conn_params) as conn:
        for rt, nid in newest_ids.items():
            if nid:
                set_last_seen_id(conn, rt, nid)

    logger.info("Incremental DB sync complete.")

# ---------------------------------
# Entrypoint
# ---------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
