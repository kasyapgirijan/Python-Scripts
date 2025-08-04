import os
import base64
import configparser
import json
import requests
import logging
import psycopg2
import pandas as pd
from io import StringIO
from datetime import datetime
from psycopg2.extras import execute_values
import urllib3
from dateutil import parser


# ======================== SETUP ========================
LOGFILE = "splunk_sync.log"
logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(module)s) - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Suppress SSL warnings for test environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ======================== CONFIG ========================
INI_FILE = ""
INI_SECTION = "postgresql"
SPLUNK_KEY_FILE = "splunk_api_key"
SPLUNK_HOST = "https://splunkcloud.com:8089"
SPLUNK_RANGE_DAYS = 30
SPLUNK_SEARCH = (
    "search=savedsearch CDOC_Metrics&output_mode=csv"
    f"&earliest_time=-{SPLUNK_RANGE_DAYS}d@d&latest=now"
)
VERIFY_SSL = False
TABLE_NAME = "mod_cdoc_metrics"
PRIMARY_KEY = "id"
DATETIME_COLUMNS = ["created_date", "updated_date", "closed_date"]


# ======================== FUNCTION: Read DB Config ========================
def read_config(filename=INI_FILE, section=INI_SECTION):
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Config file {filename} not found")

    with open(filename, 'r') as file:
        content = file.read()

    try:
        decoded = base64.b64decode(content).decode("utf-8")
        parser_obj = configparser.ConfigParser()
        parser_obj.read_string(decoded)
    except base64.binascii.Error:
        parser_obj = configparser.ConfigParser()
        parser_obj.read(filename)

    if parser_obj.has_section(section):
        return {k: v for k, v in parser_obj.items(section)}

    raise Exception(f"Section {section} not found in config file")


# ======================== FUNCTION: Load Splunk Token ========================
def read_splunk_token(key_file=SPLUNK_KEY_FILE):
    if not os.path.exists(key_file):
        raise FileNotFoundError(f"Splunk token file {key_file} not found")

    with open(key_file, 'r') as f:
        return f"Bearer {f.read().strip()}"


# ======================== FUNCTION: Create Table SQL ========================
def create_table_sql(df, table_name):
    col_defs = []
    for col in df.columns:
        if col == PRIMARY_KEY:
            col_defs.append(f'"{col}" TEXT PRIMARY KEY')
        elif col in DATETIME_COLUMNS:
            col_defs.append(f'"{col}" TIMESTAMP')
        else:
            col_defs.append(f'"{col}" TEXT')
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'


# ======================== MAIN ========================
try:
    logger.info("Starting Splunk data fetch and database sync...")

    db_config = read_config()
    SPLUNK_TOKEN = read_splunk_token()

    headers = {
        "Authorization": SPLUNK_TOKEN,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    export_url = f"{SPLUNK_HOST}/servicesNS/-/-/search/jobs/export?{SPLUNK_SEARCH}"
    logger.info(f"Querying Splunk: {export_url}")

    response = requests.get(export_url, headers=headers, stream=True, verify=VERIFY_SSL)
    response.raise_for_status()

    csv_data = response.content.decode("utf-8")
    df = pd.read_csv(StringIO(csv_data))

    if df.empty:
        logger.warning("No data returned from Splunk.")
        exit(0)

    logger.info(f"Retrieved {len(df)} rows from Splunk.")

    df.columns = [col.lower().replace(" ", "_").replace(".", "_") for col in df.columns]

    if PRIMARY_KEY not in df.columns:
        raise ValueError(f"Expected column '{PRIMARY_KEY}' not found in data")

    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%b %d, %Y %I:%M:%S %p", errors="coerce")
            df[col] = df[col].astype("object").where(df[col].notnull(), None)
            logger.info(f"Sample values in '{col}': {df[col].dropna().head(3).tolist()}")

    logger.info("Establishing database connection...")
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql(df, TABLE_NAME))
            logger.info(f"Ensured table '{TABLE_NAME}' exists.")

            columns = df.columns.tolist()
            quoted_columns = [f'"{col}"' for col in columns]
            placeholders = ', '.join(['%s'] * len(columns))

            insert_query = f"""
            INSERT INTO "{TABLE_NAME}" ({', '.join(quoted_columns)})
            VALUES %s
            ON CONFLICT ("{PRIMARY_KEY}") DO UPDATE
            SET {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != PRIMARY_KEY])}
            WHERE EXCLUDED.updated_date > "{TABLE_NAME}".updated_date;
            """

            data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
            execute_values(cur, insert_query, data)
            logger.info(f"Sync completed: {len(data)} rows processed for '{TABLE_NAME}'.")

except Exception as e:
    logger.exception(f"Fatal error occurred during sync: {e}")
