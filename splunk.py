import os
import base64
import configparser
import logging
import requests
import psycopg2
import pandas as pd
from io import StringIO

# ========== SETUP ==========
LOGFILE = "splunk_sync.log"
logging.basicConfig(filename=LOGFILE, level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========== CONFIG ==========
INI_FILE = "dbconfig.ini"
INI_SECTION = "database"
SPLUNK_KEY_FILE = "splunk_api.key"
SPLUNK_HOST = "https://localhost:8089"
SPLUNK_RANGE_DAYS = 365  # Change to 1 when running as daily cron job
SPLUNK_SEARCH = f"search=savedsearch CDOC_Metrics&output_mode=csv&earliest_time=-{SPLUNK_RANGE_DAYS}d&latest=now"
VERIFY_SSL = False
TABLE_NAME = 'mod_cdoc_metrics'
PRIMARY_KEY = 'id'

# ========== FUNCTION: Read DB Config ==========
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

# ========== FUNCTION: Load Splunk Token ==========
def read_splunk_token(key_file=SPLUNK_KEY_FILE):
    if not os.path.exists(key_file):
        raise FileNotFoundError(f"Splunk token file {key_file} not found")
    with open(key_file, 'r') as f:
        return f"Splunk {f.read().strip()}"

# ========== FUNCTION: Create SQL ==========
def create_table_sql(df, table_name):
    col_defs = []
    for col in df.columns:
        if col == PRIMARY_KEY:
            col_defs.append(f'"{col}" TEXT PRIMARY KEY')
        else:
            col_defs.append(f'"{col}" TEXT')
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'

# ========== MAIN ==========
try:
    logger.info("Starting Splunk data fetch and database sync...")

    db_config = read_config()
    SPLUNK_TOKEN = read_splunk_token()

    # Fetch from Splunk
    headers = {
        "Authorization": SPLUNK_TOKEN,
        "Content-Type": "application/x-www-form-urlencoded",
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

    # Normalize columns
    df.columns = [col.lower().replace(" ", "_").replace(".", "_") for col in df.columns]

    if PRIMARY_KEY not in df.columns:
        raise ValueError(f"Expected column '{PRIMARY_KEY}' not found in data")

    # Create table and insert
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(create_table_sql(df, TABLE_NAME))

    columns = df.columns.tolist()
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"""
        INSERT INTO "{TABLE_NAME}" ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({PRIMARY_KEY}) DO NOTHING;
    """

    inserted = 0
    for _, row in df.iterrows():
        cur.execute(insert_query, tuple(row[col] for col in columns))
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Done. Attempted {len(df)} rows. Inserted {inserted} new rows into '{TABLE_NAME}'.")

except Exception as e:
    logger.error(f"Fatal error: {e}")
