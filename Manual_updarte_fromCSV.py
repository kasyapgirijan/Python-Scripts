import os
import base64
import configparser
import logging
import psycopg2
import pandas as pd
from io import StringIO
from psycopg2.extras import execute_values

# ======================== SETUP ========================
LOGFILE = "manual_import.log"
logging.basicConfig(
    filename=LOGFILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ======================== CONFIG ========================
INI_FILE = "db_config.ini"
INI_SECTION = "postgresql"
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

# ======================== FUNCTION: Create Table ========================
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
def main():
    csv_path = input("Enter CSV file path: ").strip()
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    logger.info(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)

    df.columns = [col.lower().replace(" ", "_").replace(".", "_").replace("-", "_") for col in df.columns]

    if PRIMARY_KEY not in df.columns:
        raise ValueError(f"Expected column '{PRIMARY_KEY}' not found in CSV")

    # Convert datetime columns if present
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].astype("object").where(df[col].notnull(), None)

    db_config = read_config()
    logger.info("Connecting to database...")

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql(df, TABLE_NAME))
            logger.info(f"Ensured table '{TABLE_NAME}' exists.")

            columns = df.columns.tolist()
            quoted_columns = [f'"{col}"' for col in columns]

            insert_query = f"""
            INSERT INTO "{TABLE_NAME}" ({', '.join(quoted_columns)})
            VALUES %s
            ON CONFLICT ("{PRIMARY_KEY}") DO UPDATE
            SET {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != PRIMARY_KEY])}
            WHERE EXCLUDED.updated_date IS NOT NULL
              AND (EXCLUDED.updated_date > "{TABLE_NAME}".updated_date OR "{TABLE_NAME}".updated_date IS NULL);
            """

            data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
            execute_values(cur, insert_query, data)
            logger.info(f"✅ Imported/updated {len(data)} rows into '{TABLE_NAME}'")

    print(f"✅ Manual CSV import complete for {len(df)} rows.")
    logger.info("Manual import finished successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Error during manual import: {e}")
        print(f"❌ Import failed: {e}")
