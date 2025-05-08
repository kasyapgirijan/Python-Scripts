import os
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import configparser
import base64

# Read API key from file
with open("api_key.txt", "r") as file:
    api_key = file.read().strip()

# Get current datetime for folder naming
date_string = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

base_url = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"

def extract_user_tags(attributes):
    """Extract user tags from the attributes dictionary"""
    user_tags_data = {}
    user_tags = attributes.get('usertags', {})
    if user_tags:
        for tag_category, tag_values in user_tags.items():
            tag_category = tag_category.lower().replace('_1', '')
            user_tags_data.update(
                {f'{tag_category}_{i+1}': value for i, value in enumerate(tag_values)})
    return user_tags_data

def extract_attributes(api_url, report_type):
    """Extract attributes from the API using pagination"""
    extracted_data = []
    try:
        page = 1
        while True:
            url = f"{api_url}&page[number]={page}&page[size]=8000&filter[_includedeletedusers]=TRUE"
            print(f"Fetching page {page} for {report_type}...")
            headers = {"x-apikey-token": api_key}
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error: API returned status code {response.status_code}")
                print(f"Response: {response.text}")
                break
                
            data = response.json()

            if not data.get('data', []):
                break  # No more pages

            for item in data.get('data', []):
                attributes = item.get('attributes', {})
                item_data = {
                    'type': item.get('type'),
                    'id': item.get('id'),
                    **attributes
                }

                if report_type == 'users':
                    item_data.update(extract_user_tags(attributes))

                extracted_data.append(item_data)
            
            page += 1  # Move to the next page

    except Exception as e:
        print(f"Error extracting attributes for {report_type}: {e}")

    return extracted_data

def process_report(report_type):
    """Process a specific report type and return the data"""
    if report_type == 'users':
        api_url = f"{base_url}{report_type}?user_tag_enabled&"
    else:
        api_url = f"{base_url}{report_type}?"

    print(f"Collecting {report_type.capitalize()} data...")
    extracted_data = extract_attributes(api_url, report_type)

    if extracted_data:
        df = pd.DataFrame(extracted_data)

        # Remove Rows where useremailaddress matches sso_id@whateverdomain.com
        if report_type == 'users':
            df['sso_id_email'] = df['sso_id'] + '@transunion.com'
            df = df[~df['useremailaddress'].isin(df['sso_id_email'])]
            
        return df
    else:
        print(f"Failed to retrieve attributes for {report_type.capitalize()} data.")
        return None

def save_to_excel(dataframes, workbook_path):
    """Save all dataframes to a single Excel file with multiple sheets"""
    try:
        with pd.ExcelWriter(workbook_path, engine='xlsxwriter') as writer:
            for report_type, df in dataframes.items():
                if df is not None and not df.empty:
                    df.to_excel(writer, sheet_name=report_type.capitalize(), index=False)
                    print(f"{report_type.capitalize()} data saved to workbook.")
    except Exception as e:
        print(f"Error saving to Excel: {e}")

def config(filename='proofpoint.ini', section='postgresql'):
    """Read database configuration from base64 encoded ini file"""
    try:
        with open(filename, 'r') as file:
            encoded_content = file.read()
        
        # Decode base64 content
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
    except Exception as e:
        print(f"Error reading config file: {e}")
        raise

def truncate_tables(db_config, table_names):
    """Truncate specified tables to prevent duplications"""
    try:
        # Create connection string
        conn_params = f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"
        
        # Connect to PostgreSQL using psycopg2
        conn = psycopg2.connect(conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        for table_name in table_names:
            try:
                print(f"Truncating table: {table_name}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                print(f"Table {table_name} truncated successfully")
            except Exception as e:
                print(f"Error truncating table {table_name}: {e}")
        
        cursor.close()
        conn.close()
        print("All tables truncated successfully")
    except Exception as e:
        print(f"Error connecting to database for truncation: {e}")

def save_to_postgres(dataframes, db_config):
    """Save all dataframes to PostgreSQL tables using specific table names"""
    # Map report types to specific table names
    table_mapping = {
        'phishing': 'Mod_Phishing',
        'users': 'Mod_Users',
        'training': 'Mod_Training'
    }
    
    # Get list of tables to truncate
    tables_to_truncate = [table for table in table_mapping.values()]
    
    # Truncate tables before inserting new data
    truncate_tables(db_config, tables_to_truncate)
    
    try:
        # Create SQLAlchemy engine
        conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(conn_string)
        
        # Map report types to specific table names
        table_mapping = {
            'phishing': 'Mod_Phishing',
            'users': 'Mod_Users',
            'training': 'Mod_Training'
        }
        
        for report_type, df in dataframes.items():
            if df is not None and not df.empty:
                # Convert column names to lowercase for better PostgreSQL compatibility
                df.columns = [col.lower() for col in df.columns]
                
                # Replace empty strings with None for better PostgreSQL storage
                df = df.replace('', None)
                
                # Get the specific table name
                table_name = table_mapping.get(report_type)
                if table_name:
                    # Save to PostgreSQL - using append since we've already truncated the tables
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                    print(f"{report_type.capitalize()} data saved to PostgreSQL table '{table_name}'.")
                else:
                    print(f"Warning: No table mapping defined for report type '{report_type}'")
                
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")

def main():
    # Get database configuration from ini file
    try:
        db_config = config(filename='proofpoint.ini', section='postgresql')
        print("Database configuration loaded successfully.")
    except Exception as e:
        print(f"Error loading database configuration: {e}")
        return
    
    # Report types to process
    report_types = ['training', 'users', 'phishing']  # Add more report types as needed
    
    # Create folder for export
    date_folder = date_string
    f_path = os.getcwd()
    folder_path = os.path.join(f_path, date_folder)
    os.makedirs(folder_path, exist_ok=True)
    
    # Process reports in parallel and collect dataframes
    dataframes = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_report, report_type): report_type for report_type in report_types}
        
        for future in futures:
            report_type = futures[future]
            df = future.result()
            dataframes[report_type] = df
    
    # Save all data to Excel
    workbook_path = os.path.join(folder_path, "Proofpoint.xlsx")
    save_to_excel(dataframes, workbook_path)
    
    # Save all data to PostgreSQL
    save_to_postgres(dataframes, db_config)
    
    print(f"All reports saved to Excel at {workbook_path} and to PostgreSQL database.")

if __name__ == '__main__':
    main()
