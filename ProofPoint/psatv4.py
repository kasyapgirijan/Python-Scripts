import os
import re
import json
import time
import base64
import logging
import configparser
from typing import Optional, Tuple, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ======================================
# Logging
# ======================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("proofpoint_api.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ======================================
# API constants
# ======================================
BASE_URL = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"
REPORT_TYPES = ["users", "phishing"]

# ======================================
# Fixed DB schemas
# ======================================
DB_SCHEMAS: Dict[str, Dict[str, str]] = {
    "Mod_ThreatAwareness_Phishing": {
        "id": "TEXT",
        "type": "TEXT",
        "eventtype": "TEXT",
        "eventtimestamp": "TIMESTAMPTZ",
        "senttimestamp": "TIMESTAMPTZ",
        "campaigntype": "TEXT",
        "campaignname": "TEXT",
        "campaignstatus": "TEXT",
        "campaignstartdate": "TIMESTAMPTZ",
        "campaignenddate": "TIMESTAMPTZ",
        "templatename": "TEXT",
        "templatesubject": "TEXT",
        "assessmentsarchived": "BOOLEAN",
        "autoenrollment": "BOOLEAN",
        "sso_id": "TEXT",
        "useremailaddress": "TEXT",
        "userfirstname": "TEXT",
        "userlastname": "TEXT",
        "useractiveflag": "BOOLEAN",
        "userdeleteddate": "TIMESTAMPTZ",
        "usertags": "TEXT",
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

TABLE_MAPPING: Dict[str, str] = {
    "phishing": "Mod_ThreatAwareness_Phishing",
    "users": "Mod_ThreatAwareness_Users",
}

# ======================================
# Config / credentials
# ======================================
def read_api_key() -> str:
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
            host=db["host"], port=db["port"], dbname=db["dbname"],
            user=db["user"], password=db["password"]
        ):
            pass
        logger.info("Database connection test successful.")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

# ======================================
# State table
# ======================================
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

# ======================================
# Helpers
# ======================================
def ensure_table(conn, table_name: str, schema: Dict[str, str]) -> None:
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

def to_python_bool(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    if s in {"true", "t", "1", "yes"}: return True
    if s in {"false", "f", "0", "no"}: return False
    try: return bool(int(s))
    except Exception: return bool(v)

def cast_dataframe_types(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = pd.DataFrame()
    for col, pgtype in schema.items():
        pg = pgtype.upper()
        if col not in df.columns:
            out[col] = None
            continue
        s = df[col]
        if pg == "TIMESTAMPTZ":
            s = pd.to_datetime(s, errors="coerce", utc=True, infer_datetime_format=True)
            s = s.astype("object")
            s = s.where(pd.notnull(s), None)
            out[col] = s
        elif pg == "BOOLEAN":
            out[col] = s.apply(to_python_bool)
        else:
            out[col] = s.astype("string").where(~s.isna(), None)
    return out

def compute_row_hash(row: pd.Series, cols: List[str]) -> str:
    payload = {c: (None if pd.isna(row[c]) else row[c]) for c in cols if c in row.index}
    s = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    import hashlib
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# ======================================
# User tag expansion
# ======================================
def _normalize_tag_key(cat: str) -> str:
    cat = str(cat).lower()
    cat = re.sub(r"_1$", "", cat)
    cat = re.sub(r"\W+", "_", cat).strip("_")
    return cat

def extract_user_tags(attributes: dict) -> dict:
    out = {}
    ut = attributes.get("usertags", {}) or {}
    for cat, vals in ut.items():
        base = _normalize_tag_key(cat)
        if vals is None:
            continue
        if not isinstance(vals, list):
            vals = [vals]
        for i, v in enumerate(vals):
            if v is not None:
                out[f"{base}_{i+1}"] = v
    return out

# ======================================
# API fetch
# ======================================
def fetch_report(report_type: str, api_key: str, seen_id: Optional[str]) -> Tuple[pd.DataFrame, Optional[str]]:
    session = requests.Session()
    headers = {"x-apikey-token": api_key}
    api_url = f"{BASE_URL}{report_type}?"
    if report_type == "users":
        api_url += "user_tag_enabled&"
    next_url = f"{api_url}page[size]=8000&filter[_includedeletedusers]=TRUE"

    page = 0
    newest_id: Optional[str] = None
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
            for k, v in attrs.items():
                if isinstance(v, (dict, list)):
                    if k == "usertags":
                        row["usertags"] = json.dumps(v, ensure_ascii=False)
                    continue
                row[k] = v
            if report_type == "users":
                tag_cols = extract_user_tags(attrs)
                row.update(tag_cols)
            rows.append(row)

        raw_next = (payload.get("links") or {}).get("next")
        if raw_next:
            next_url = urljoin(BASE_URL, raw_next)
        else:
            next_url = f"{api_url}page[number]={page+1}&page[size]=8000&filter[_includedeletedusers]=TRUE"

    logger.info(f"[{report_type}] fetched {len(rows)} new rows.")
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if "location_1" in df.columns and pd.api.types.is_string_dtype(df["location_1"]):
        df["location_1"] = df["location_1"].str.replace("São Paulo", "Sao Paulo", regex=False)
    return df, newest_id

# ======================================
# DB write
# ======================================
def upsert_dataframe(conn, df: pd.DataFrame, table: str, schema: Dict[str, str]) -> None:
    if df is None or df.empty:
        return
    aligned = cast_dataframe_types(df, schema)
    aligned = aligned.replace({pd.NaT: None})
    cols_for_hash = [c for c in aligned.columns if c != "row_hash"]
    aligned["row_hash"] = aligned.apply(lambda r: compute_row_hash(r, cols_for_hash), axis=1)
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

# ======================================
# Main
# ======================================
def main():
    logger.info("Starting Proofpoint → Postgres incremental sync.")
    api_key = read_api_key()
    db = read_db_config("proofpoint.ini", "postgresql")
    if not test_db_connection(db):
        logger.error("DB connection failed. Exiting.")
        return
    conn_params = dict(host=db["host"], port=db["port"], dbname=db["dbname"], user=db["user"], password=db["password"])

    with psycopg2.connect(**conn_params) as conn:
        ensure_state_table(conn)
        seen_ids = {rt: get_last_seen_id(conn, rt) for rt in REPORT_TYPES}

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

    with psycopg2.connect(**conn_params) as conn:
        for rt, df in results.items():
            table = TABLE_MAPPING.get(rt)
            if not table or df.empty:
                continue
            schema = DB_SCHEMAS[table]
            upsert_dataframe(conn, df, table, schema)

    with psycopg2.connect(**conn_params) as conn:
        for rt, nid in newest_ids.items():
            if nid:
                set_last_seen_id(conn, rt, nid)

    logger.info("Incremental sync complete.")

# ======================================
# Entrypoint
# ======================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
