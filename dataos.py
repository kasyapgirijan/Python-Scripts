import psycopg2
import pandas as pd
from psycopg2 import sql
from configparser import ConfigParser

# === Read DB config from .ini ===
def read_db_config(filename="db_config.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    if not parser.has_section(section):
        raise Exception(f"Section '{section}' not found in {filename}")
    return dict(parser.items(section))

# === Create table dynamically (all TEXT columns) ===
def create_table_from_csv(conn, csv_path, table_name):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().replace(" ", "_").replace("-", "_").lower() for c in df.columns]

    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(table_name)))

        col_defs = []
        for col in df.columns:
            if col == "id":
                col_defs.append(f'"{col}" TEXT PRIMARY KEY')
            else:
                col_defs.append(f'"{col}" TEXT')

        create_query = sql.SQL("CREATE TABLE {} ({});").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.SQL(c) for c in col_defs)
        )
        cur.execute(create_query)
        conn.commit()
        print(f"✅ Created table '{table_name}' with {len(df.columns)} TEXT columns")

    return df

# === Insert or Upsert CSV data ===
def upsert_csv_data(conn, df, table_name):
    with conn.cursor() as cur:
        cols = list(df.columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() * len(cols))

        update_cols = [
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(c), sql.Identifier(c)
            )
            for c in df.columns if c != "id"
        ]

        query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT (id)
            DO UPDATE SET {};
        """).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, cols)),
            placeholders,
            sql.SQL(", ").join(update_cols)
        )

        for _, row in df.iterrows():
            cur.execute(query, tuple(str(v) if pd.notna(v) else None for v in row))
        conn.commit()
        print(f"📦 Upserted {len(df)} rows into '{table_name}'")

# === Main ===
def main():
    db = read_db_config("db_config.ini")
    csv_path = input("Enter CSV file path: ").strip()
    table_name = input("Enter target table name: ").strip().lower()

    conn = psycopg2.connect(**db)
    print("🧩 Connected to PostgreSQL")

    df = create_table_from_csv(conn, csv_path, table_name)
    upsert_csv_data(conn, df, table_name)

    conn.close()
    print("🔒 Connection closed.")

if __name__ == "__main__":
    main()
