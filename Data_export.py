import psycopg2
import pandas as pd
from psycopg2 import sql
from configparser import ConfigParser

# === 1️⃣ Read database details from .ini file ===
def read_db_config(filename="db_config.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section '{section}' not found in {filename}")
    return db

# === 2️⃣ Create table dynamically based on CSV header ===
def create_table_from_csv(conn, csv_path, table_name):
    df = pd.read_csv(csv_path)
    columns = df.columns

    # Drop duplicates / clean headers
    columns = [col.strip().replace(" ", "_").replace("-", "_").lower() for col in columns]

    with conn.cursor() as cur:
        # Drop table if exists (optional safety)
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))

        # Create table with id SERIAL and CSV columns
        col_defs = ["id SERIAL PRIMARY KEY"]
        for col in columns:
            col_defs.append(f'"{col}" TEXT')  # You can later infer types

        create_query = sql.SQL("CREATE TABLE {} ({});").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.SQL(c) for c in col_defs)
        )
        cur.execute(create_query)
        conn.commit()
        print(f"✅ Table '{table_name}' created with columns: {columns}")
    return df, columns

# === 3️⃣ Insert CSV data into the table ===
def insert_csv_data(conn, df, table_name, columns):
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(columns))
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(placeholders)
            )
            cur.execute(query, tuple(row))
        conn.commit()
        print(f"📦 Inserted {len(df)} rows into '{table_name}'")

# === 4️⃣ Main function ===
def main():
    db_params = read_db_config("db_config.ini")
    csv_path = input("Enter CSV file path: ").strip()
    table_name = input("Enter target table name: ").strip().lower()

    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        print("🧩 Connected to PostgreSQL")

        df, columns = create_table_from_csv(conn, csv_path, table_name)
        insert_csv_data(conn, df, table_name, columns)

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if conn:
            conn.close()
            print("🔒 Connection closed.")

if __name__ == "__main__":
    main()

#[postgresql]
#host = localhost
#port = 5432
#dbname = my_database
#user = my_user
#password = my_password
