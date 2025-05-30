import os
import base64
import configparser
import logging
import requests
import psycopg2
import pandas as pd
from io import StringIO
from datetime import datetime
from psycopg2.extras import execute_values
import urllib3
import time

# ========== SETUP ==========
LOGFILE = "splunk_sync.log"
logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(module)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== CONFIG ==========
INI_FILE = "dbconfig.ini"
INI_SECTION = "database"
SPLUNK_KEY_FILE = "splunk_api.key"
SPLUNK_HOST = "https://localhost:8089"
VERIFY_SSL = False
TABLE_NAME = 'mod_cdoc_metrics'
PRIMARY_KEY = 'id'
DATE_COLUMNS = {"created_date", "updated_date", "closed_date"}
PAGE_SIZE = 10000

# ========== FUNCTION ==========
def read_config(filename=INI_FILE, section=INI_SECTION):
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Config file {filename} not found")
    with open(filename, 'r') as file:
        content = file.read()
    try:
        decoded = base64.b64decode(content).decode('utf-8')
        parser = configparser.ConfigParser()
        parser.read_string(decoded)
    except base64.binascii.Error:
        parser = configparser.ConfigParser()
        parser.read(filename)
    if parser.has_section(section):
        return {k: v for k, v in parser.items(section)}
    raise Exception(f"Section {section} not found in config file")

def read_splunk_token(key_file=SPLUNK_KEY_FILE):
    if not os.path.exists(key_file):
        raise FileNotFoundError(f"Splunk token file {key_file} not found")
    with open(key_file, 'r') as f:
        return f"Splunk {f.read().strip()}"

def create_table_sql(df, table_name):
    col_defs = []
    for col in df.columns:
        if col == PRIMARY_KEY:
            col_defs.append(f'"{col}" TEXT PRIMARY KEY')
        elif col in DATE_COLUMNS:
            col_defs.append(f'"{col}" DATE')
        else:
            col_defs.append(f'"{col}" TEXT')
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'

def poll_until_done(sid, headers):
    status_url = f"{SPLUNK_HOST}/services/search/jobs/{sid}"
    while True:
        r = requests.get(status_url, headers=headers, verify=VERIFY_SSL)
        state = r.json()['entry'][0]['content']['dispatchState']
        if state == "DONE":
            return
        logger.info(f"Waiting for search job {sid} to finish... Current state: {state}")
        time.sleep(2)

def fetch_paginated_results(sid, headers):
    all_results = []
    offset = 0
    while True:
        results_url = f"{SPLUNK_HOST}/services/search/jobs/{sid}/results"
        params = {
            "output_mode": "csv",
            "count": PAGE_SIZE,
            "offset": offset
        }
        response = requests.get(results_url, headers=headers, params=params, verify=VERIFY_SSL)
        if response.status_code != 200 or not response.text.strip():
            break
        batch_df = pd.read_csv(StringIO(response.text))
        all_results.append(batch_df)
        logger.info(f"Fetched {len(batch_df)} rows at offset {offset}.")
        if len(batch_df) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

# ========== SPLUNK SEARCH ==========
# Copy-paste your actual search body here as one string
SPLUNK_SEARCH = """
Paste here
"""

# ========== MAIN ==========
try:
    logger.info("Starting Splunk SID-based sync...")

    db_config = read_config()
    SPLUNK_TOKEN = read_splunk_token()

    headers = {
        "Authorization": SPLUNK_TOKEN,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    submit_url = f"{SPLUNK_HOST}/services/search/jobs"
    data = {
        "search": SPLUNK_SEARCH,
        "output_mode": "json"
    }

    response = requests.post(submit_url, headers=headers, data=data, verify=VERIFY_SSL)
    response.raise_for_status()
    sid = response.json()['sid']
    logger.info(f"Search job submitted. SID: {sid}")

    poll_until_done(sid, headers)
    df = fetch_paginated_results(sid, headers)

    if df.empty:
        logger.warning("No results from Splunk.")
        exit(0)

    logger.info(f"Total rows retrieved: {len(df)}")
    df.columns = [col.lower().replace(" ", "_").replace(".", "_") for col in df.columns]

    if PRIMARY_KEY not in df.columns:
        raise ValueError(f"Expected primary key column '{PRIMARY_KEY}' not found.")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            df[col] = df[col].where(df[col].notnull(), None)

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql(df, TABLE_NAME))
            logger.info(f"Ensured table '{TABLE_NAME}' exists.")

            columns = df.columns.tolist()
            insert_query = f"""
                INSERT INTO "{TABLE_NAME}" ({', '.join(columns)})
                VALUES %s
                ON CONFLICT ({PRIMARY_KEY}) DO UPDATE SET
                    closed_date = EXCLUDED.closed_date
                    WHERE EXCLUDED.closed_date IS NOT NULL;
            """

            data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
            execute_values(cur, insert_query, data)
            logger.info(f"{len(data)} rows inserted or updated in '{TABLE_NAME}'.")

except Exception as e:
    logger.exception(f"Fatal error during sync: {e}")
