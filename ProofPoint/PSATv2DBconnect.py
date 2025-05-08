import os
import requests
import pandas as pd
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import configparser
import base64
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("proofpoint_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def read_api_key():
    """Read API key from file with error handling"""
    try:
        with open("api_key.txt", "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        logger.error("api_key.txt file not found. Please create this file with your API key.")
        raise
    except Exception as e:
        logger.error(f"Error reading API key: {e}")
        raise

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
            if isinstance(tag_values, list):  # Ensure tag_values is a list
                user_tags_data.update(
                    {f'{tag_category}_{i+1}': value for i, value in enumerate(tag_values)})
    return user_tags_data

def extract_attributes(api_url, report_type, api_key):
    """Extract attributes from the API using pagination"""
    extracted_data = []
    try:
        page = 1
        retry_count = 0
        max_retries = 3
        
        while True:
            url = f"{api_url}&page[number]={page}&page[size]=8000&filter[_includedeletedusers]=TRUE"
            logger.info(f"Fetching page {page} for {report_type}...")
            headers = {"x-apikey-token": api_key}
            
            try:
                response = requests.get(url, headers=headers, timeout=60)
                
                if response.status_code == 429:  # Rate limit hit
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit hit. Waiting for {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                if response.status_code != 200:
                    logger.error(f"Error: API returned status code {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    
                    if retry_count < max_retries:
                        retry_count += 1
                        wait_time = retry_count * 5  # Exponential backoff
                        logger.info(f"Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Max retries reached for {report_type}. Moving on.")
                        break
                    
                data = response.json()

                if not data.get('data', []):
                    logger.info(f"No more data for {report_type} after page {page-1}")
                    break  # No more pages

                for item in data.get('data', []):
                    attributes = item.get('attributes', {})
                    item_data = {
                        'type': item.get('type'),
                        'id': item.get('id'),
                    }
                    
                    # Add all attributes as individual columns
                    for key, value in attributes.items():
                        if isinstance(value, (dict, list)):
                            continue  # Skip nested structures except for user tags
                        item_data[key] = value

                    if report_type == 'users':
                        item_data.update(extract_user_tags(attributes))

                    extracted_data.append(item_data)
                
                # Reset retry count on successful request
                retry_count = 0
                page += 1  # Move to the next page

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                if retry_count < max_retries:
                    retry_count += 1
                    wait_time = retry_count * 5
                    logger.info(f"Retrying in {wait_time} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for {report_type}. Moving on.")
                    break

    except Exception as e:
        logger.error(f"Error extracting attributes for {report_type}: {e}")

    logger.info(f"Total records extracted for {report_type}: {len(extracted_data)}")
    return extracted_data

def process_report(report_type, api_key):
    """Process a specific report type and return the data"""
    if report_type == 'users':
        api_url = f"{base_url}{report_type}?user_tag_enabled&"
    else:
        api_url = f"{base_url}{report_type}?"

    logger.info(f"Collecting {report_type.capitalize()} data...")
    extracted_data = extract_attributes(api_url, report_type, api_key)

    if extracted_data:
        df = pd.DataFrame(extracted_data)
        
        # Handle empty dataframe
        if df.empty:
            logger.warning(f"Empty dataframe for {report_type}")
            return pd.DataFrame()

        # Remove Rows where useremailaddress matches sso_id@whateverdomain.com
        if report_type == 'users' and 'useremailaddress' in df.columns and 'sso_id' in df.columns:
            df['sso_id_email'] = df['sso_id'].astype(str) + '@transunion.com'
            df = df[~df['useremailaddress'].isin(df['sso_id_email'])]
            df = df.drop('sso_id_email', axis=1)
            
        logger.info(f"Processed {len(df)} records for {report_type}")
        return df
    else:
        logger.warning(f"Failed to retrieve attributes for {report_type.capitalize()} data.")
        return pd.DataFrame()  # Return empty DataFrame instead of None

def save_to_excel(dataframes, workbook_path):
    """Save all dataframes to a single Excel file with multiple sheets"""
    try:
        with pd.ExcelWriter(workbook_path, engine='xlsxwriter') as writer:
            for report_type, df in dataframes.items():
                if df is not None and not df.empty:
                    df.to_excel(writer, sheet_name=report_type.capitalize(), index=False)
                    logger.info(f"{report_type.capitalize()} data saved to workbook.")
        logger.info(f"Excel file saved at: {workbook_path}")
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")

def read_config(filename='proofpoint.ini', section='postgresql'):
    """Read database configuration from base64 encoded ini file"""
    try:
        # Check if file exists
        if not os.path.exists(filename):
            logger.error(f"Config file {filename} not found")
            raise FileNotFoundError(f"Config file {filename} not found")
            
        with open(filename, 'r') as file:
            encoded_content = file.read()
        
        try:
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
            
            # Validate required parameters
            required_params = ['host', 'port', 'dbname', 'user', 'password']
            for param in required_params:
                if param not in db:
                    raise Exception(f"Required parameter '{param}' not found in config")
                    
            return db
        except base64.binascii.Error:
            # If base64 decoding fails, try reading as plain text
            logger.warning("Base64 decoding failed, trying to read as plain text")
            parser = configparser.ConfigParser()
            parser.read(filename)
            
            db = {}
            if parser.has_section(section):
                params = parser.items(section)
                for param in params:
                    db[param[0]] = param[1]
                return db
            else:
                raise Exception(f'Section {section} not found in the plain text file')
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise

def test_db_connection(db_config):
    """Test the database connection before proceeding"""
    try:
        conn_params = f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"
        conn = psycopg2.connect(conn_params)
        conn.close()
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

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
                logger.info(f"Truncating table: {table_name}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                logger.info(f"Table {table_name} truncated successfully")
            except Exception as e:
                logger.error(f"Error truncating table {table_name}: {e}")
        
        cursor.close()
        conn.close()
        logger.info("All tables truncated successfully")
    except Exception as e:
        logger.error(f"Error connecting to database for truncation: {e}")
        raise

def save_to_postgres(dataframes, db_config):
    """Save all dataframes to PostgreSQL tables using psycopg2 to maintain case sensitivity"""
    # Map report types to specific table names (preserve exact case)
    table_mapping = {
        'phishing': 'Mod_Phishing',
        'users': 'Mod_Users',
        'training': 'Mod_Training'
    }
    
    # Get list of tables to truncate
    tables_to_truncate = [table for table in table_mapping.values()]
    
    try:
        # Truncate tables before inserting new data
        truncate_tables(db_config, tables_to_truncate)
        
        # Create connection string
        conn_params = f"host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"
        
        # Connect to PostgreSQL using psycopg2
        conn = psycopg2.connect(conn_params)
        cursor = conn.cursor()
        
        for report_type, df in dataframes.items():
            if df is not None and not df.empty:
                # Replace empty strings with None for better PostgreSQL storage
                df = df.replace('', None)
                
                # Get the specific table name with preserved case
                table_name = table_mapping.get(report_type)
                if table_name:
                    try:
                        # Process dataframe in chunks for better performance with large datasets
                        chunk_size = 1000
                        for i in range(0, len(df), chunk_size):
                            df_chunk = df.iloc[i:i+chunk_size]
                            
                            # Get column names preserving their original case
                            columns = df_chunk.columns.tolist()
                            # Create quoted column list for SQL
                            column_str = ', '.join([f'"{col}"' for col in columns])
                            
                            # Prepare values placeholders for each row
                            placeholders = ', '.join(['%s'] * len(columns))
                            
                            # Build the INSERT statement with double quotes around table and column names
                            insert_query = f'INSERT INTO "{table_name}" ({column_str}) VALUES ({placeholders})'
                            
                            # Convert dataframe to list of tuples for execution
                            values = [tuple(row) for row in df_chunk.to_numpy()]
                            
                            # Execute insert in batches
                            cursor.executemany(insert_query, values)
                            conn.commit()
                            
                            logger.info(f"Inserted chunk of {len(df_chunk)} rows into {table_name}")
                            
                        logger.info(f"{report_type.capitalize()} data saved to PostgreSQL table '{table_name}'.")
                    except Exception as e:
                        logger.error(f"Error saving {report_type} data to PostgreSQL: {e}")
                        conn.rollback()  # Rollback transaction on error
                else:
                    logger.warning(f"Warning: No table mapping defined for report type '{report_type}'")
        
        cursor.close()
        conn.close()
                
    except Exception as e:
        logger.error(f"Error in save_to_postgres function: {e}")

def main():
    try:
        # Read API key
        api_key = read_api_key()
        logger.info("API key loaded successfully.")
        
        # Get database configuration from ini file
        try:
            db_config = read_config(filename='proofpoint.ini', section='postgresql')
            logger.info("Database configuration loaded successfully.")
            
            # Test DB connection
            if not test_db_connection(db_config):
                logger.error("Database connection failed. Continuing with Excel export only.")
                db_connection_ok = False
            else:
                db_connection_ok = True
        except Exception as e:
            logger.error(f"Error loading database configuration: {e}")
            logger.info("Continuing with Excel export only.")
            db_connection_ok = False
        
        # Report types to process
        report_types = ['training', 'users', 'phishing']  # Add more report types as needed
        
        # Create folder for export
        date_folder = date_string
        f_path = os.getcwd()
        folder_path = os.path.join(f_path, date_folder)
        os.makedirs(folder_path, exist_ok=True)
        logger.info(f"Created export folder at {folder_path}")
        
        # Process reports in parallel and collect dataframes
        dataframes = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_report, report_type, api_key): report_type for report_type in report_types}
            
            for future in futures:
                report_type = futures[future]
                try:
                    df = future.result()
                    dataframes[report_type] = df
                except Exception as e:
                    logger.error(f"Error processing {report_type}: {e}")
                    dataframes[report_type] = pd.DataFrame()  # Add empty dataframe on error
        
        # Save all data to Excel
        workbook_path = os.path.join(folder_path, "Proofpoint.xlsx")
        save_to_excel(dataframes, workbook_path)
        
        # Save all data to PostgreSQL if DB connection is OK
        if db_connection_ok:
            save_to_postgres(dataframes, db_config)
            logger.info(f"All reports saved to Excel at {workbook_path} and to PostgreSQL database.")
        else:
            logger.info(f"All reports saved to Excel at {workbook_path}. Database export skipped.")
            
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == '__main__':
    try:
        logger.info("Starting Proofpoint data extraction script")
        main()
        logger.info("Script completed successfully")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
