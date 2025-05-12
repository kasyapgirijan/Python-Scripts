import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import psycopg2
import configparser
import base64

# Read API key
with open("api_key.txt", "r") as file:
    api_key = file.read().strip()

datetime_now = datetime.now()
date_string = datetime_now.strftime("Y-%m-%d_%H-%M-%S")

base_url = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"

# Read PostgreSQL config from base64-encoded .ini file
def config(filename='riskrecon.ini', section='postgresql'):
    with open(filename, 'r') as file:
        encoded_content = file.read()
    decoded_content = base64.b64decode(encoded_content).decode('utf-8')
    parser = configparser.ConfigParser()
    parser.read_string(decoded_content)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the decoded content')
    
    return db

# Extract user tags into flat columns
def extract_user_tags(attributes):
    user_tags_data = {}
    user_tags = attributes.get('usertags', {})
    if user_tags:
        for tag_category, tag_values in user_tags.items():
            tag_category = tag_category.lower().replace('_1', '')
            user_tags_data.update(
                {f'{tag_category}_{i+1}': value for i, value in enumerate(tag_values)})
    return user_tags_data

# Paginated API extraction
def extract_attributes(api_url, report_type):
    extracted_data = []
    try:
        page = 1
        while True:
            url = f"{api_url}&page[number]={page}&page[size]=8000&filter[_includedeletedusers]=TRUE"
            print(url)
            headers = {"x-apikey-token": f"{api_key}"}
            response = requests.get(url, headers=headers)
            data = response.json()

            if not data['data']:
                break

            for item in data['data']:
                attributes = item.get('attributes', {})
                item_data = {
                    'type': item.get('type'),
                    'id': item.get('id'),
                    **attributes
                }

                if report_type == 'users':
                    item_data.update(extract_user_tags(attributes))

                extracted_data.append(item_data)
            page += 1

    except Exception as e:
        print(f"Error extracting attributes: {e}")

    return extracted_data

# Insert DataFrame into PostgreSQL table
def insert_to_postgres(df, table_name):
    db_params = config()
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Truncate existing table data
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")

        # Build insert query
        cols = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        # Clean NaNs for psycopg2
        data = df.where(pd.notnull(df), None).values.tolist()

        cur.executemany(insert_query, data)
        conn.commit()
        print(f"Inserted {len(df)} rows into '{table_name}'")

        cur.close()
    except Exception as e:
        print(f"Database error for {table_name}: {e}")
    finally:
        if conn:
            conn.close()

# Process a single report type
def process_report(report_type, workbook):
    if report_type == 'users':
        api_url = f"{base_url}{report_type}?user_tag_enabled&"
    else:
        api_url = f"{base_url}{report_type}?"

    extracted_data = extract_attributes(api_url, report_type)

    print(f"Collecting {report_type.capitalize()}")

    if extracted_data:
        df = pd.DataFrame(extracted_data)

        if report_type == 'users':
            df['sso_id_email'] = df['sso_id'] + '@transunion.com'
            df = df[~df['useremailaddress'].isin(df['sso_id_email'])]

        df.to_excel(workbook, sheet_name=report_type.capitalize()[:31], index=False)

        # Insert into DB
        table_name = f"mod_{report_type.lower()}"
        insert_to_postgres(df, table_name)

        print(f"{report_type.capitalize()} data saved to Excel and database.")
    else:
        print(f"Failed to retrieve attributes for {report_type.capitalize()} data.")

# Main runner
def main():
    report_types = ['training', 'users', 'phishing']  # Add more if needed
    folder_path = os.path.join(os.getcwd(), date_string)
    os.makedirs(folder_path, exist_ok=True)
    workbook_path = os.path.join(folder_path, "Proofpoint.xlsx")

    with pd.ExcelWriter(workbook_path, engine='xlsxwriter') as writer:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(process_report, report_type, writer): report_type
                for report_type in report_types
            }
            for future in futures:
                future.result()

    print(f"All reports saved to {workbook_path}")

if __name__ == '__main__':
    main()
