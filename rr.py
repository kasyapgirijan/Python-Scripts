import requests 
import pandas as pd 
import json 
import urllib3
import datetime 
import psycopg2
from psycopg2 import sql 
import configparser 
import base64

# Fix typo in urllib3 import warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Function to read database configuration from the ini file
def config(filename='riskrecon.ini', section='postgresql'):
    with open(filename, 'r') as file:
        encoded_content = file.read()

    # Fix typo in base64 decode and variable spacing
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

# To retrieve API key from a file
with open("api_key.txt", "r") as file:
    api_key = file.read().strip()

# API endpoints
toe_analysis_endpoint = 'https://api.riskrecon.com/v1/toes?internal_identifier'

# Headers
headers = {'Authorization': 'Bearer ' + api_key}

# Define the fields to include in filtered data
toe_includes = ['toe_id', 'toe_short_name', 'name', 'domain']
findings_includes = ['finding_id', 'toe_id', 'host_id', 'host_name', 'finding_type', 'severity', 'status']

# Date and time for file outputs to ensure we don't overwrite
datetime_now = datetime.datetime.now()
date_string = datetime_now.strftime("%Y-%m-%d_%H-%M-%S")
findings_file = f"{date_string}_RiskRecon_Findings.csv"
portfolio_file = f"{date_string}_RiskRecon_portfolio.csv"

# Read the Company Name from excel
df = pd.read_excel('TU_List.xlsx')
companynames = df['Company_Name'].dropna().tolist()

# Defining empty dataframe for storing data
consolidated_integration_df = pd.DataFrame()
consolidated_findings_df = pd.DataFrame()
consolidated_toe_df = pd.DataFrame()

# Function to make request to API
def make_api_requests(endpoint, params=None, timeout=120):
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=timeout, verify=False)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Function to filter data from response
def filter_and_add_request(response_data, fields_to_keep, request_info):
    filtered_bodies = [
        {**{field: data.get(field, '') for field in fields_to_keep},
         'request': request_info}
        for data in response_data
    ]
    return filtered_bodies

# Function to convert list of dictionaries to a single string
def list_of_dicts_to_string(data):
    if isinstance(data, list) and all(isinstance(i, dict) for i in data):
        return ', '.join([f"{k}:{v}" for d in data for k, v in d.items()])
    return data

db_params = config()
print(db_params)
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

def truncate_table(table_name):
    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name)))
    conn.commit()

# Function to insert data into PostgreSQL
def insert_data_to_postgresql(table_name, dataframe):
    # Convert site_id column to integer if it exists in the dataframe
    if 'site_id' in dataframe.columns:
        # Convert to integer, handling NaN/None values
        dataframe['site_id'] = dataframe['site_id'].fillna(0).astype(int)
    
    # Look for any other columns that might be floats but should be integers
    # Common ID columns that typically should be integers
    id_columns = ['toe_id', 'finding_id', 'host_id', 'RiskRecon_ID']
    for col in id_columns:
        if col in dataframe.columns and pd.api.types.is_float_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].fillna(0).astype(int)
    
    for i, row in dataframe.iterrows():
        columns = list(row.index)
        values = [list_of_dicts_to_string(row[col]) if isinstance(row[col], list) else row[col] for col in columns]
        insert_statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name), 
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        cur.execute(insert_statement, values)
    conn.commit()

# Truncate the tables to prevent duplication
truncate_table('RiskRecon_Remediation')
truncate_table('RiskRecon')

# Picking up company names from lists
for company in companynames:
    toe_company_endpoint = 'https://api.riskrecon.com/v1/toes'
    payload = {'toe_short_name': company}
    toe_data = make_api_requests(toe_company_endpoint, params=payload)

    if toe_data:
        filtered_toe_data = filter_and_add_request(toe_data, toe_includes, f'{company}_toe_request_info')
        toe_df = pd.DataFrame(filtered_toe_data)  # Changed to use filtered_toe_data
        consolidated_toe_df = pd.concat([consolidated_toe_df, toe_df], ignore_index=True)

        toe_id = toe_data[0].get('toe_id', None)
        if toe_id:
            integration_endpoint = f'https://api.riskrecon.com/v1/integration/routes/{toe_id}'
            findings_endpoint = f'https://api.riskrecon.com/v1/findings/{toe_id}'

            integration_data = make_api_requests(integration_endpoint)
            findings_data = make_api_requests(findings_endpoint)

            if findings_data and integration_data:
                filtered_findings_data = filter_and_add_request(findings_data, findings_includes, f'{company}_findings_request_info')
                
                # Handle the integration data properly
                integration_crop = integration_data['links']['data_loss']
                integration_optimized = integration_crop.replace('security-profile/data_loss', 'download-report')  # Fixed typo in 'security'
                integration_data['links'] = {'data_loss': integration_optimized}

                # Save all data into respective DataFrame
                integration_df = pd.DataFrame([integration_data])  # Wrap in list since it's a single dict
                consolidated_integration_df = pd.concat([consolidated_integration_df, integration_df], ignore_index=True)

                findings_df = pd.DataFrame(filtered_findings_data)
                consolidated_findings_df = pd.concat([consolidated_findings_df, findings_df], ignore_index=True)
            else:
                print(f"Failed to retrieve integration or findings data for {company}.")
        else:
            print(f"No toe_id found for {company}. Skipping further processing.")
    else:
        print(f"Failed to retrieve Toe data for {company}. Skipping further processing.")

    print(f"Processing for {company} completed.\n")

# Renaming headers as needed
# Assuming we want to rename some columns - add your actual renaming logic here
# consolidated_toe_df.rename(columns={'old_name': 'new_name'}, inplace=True)
# consolidated_findings_df.rename(columns={'old_name': 'new_name'}, inplace=True)
# consolidated_integration_df.rename(columns={'old_name': 'new_name'}, inplace=True)

# Create analysis dataframe with deduplicated data
consolidated_analysis_df = consolidated_toe_df.drop_duplicates(subset=['toe_id'], keep="first")

# Add RiskRecon_ID column for merging if it doesn't exist
if 'RiskRecon_ID' not in consolidated_analysis_df.columns:
    consolidated_analysis_df['RiskRecon_ID'] = consolidated_analysis_df['toe_id']

if 'RiskRecon_ID' not in consolidated_integration_df.columns:
    consolidated_integration_df['RiskRecon_ID'] = consolidated_integration_df['toe_id']

if 'RiskRecon_ID' not in consolidated_findings_df.columns:
    consolidated_findings_df['RiskRecon_ID'] = consolidated_findings_df['toe_id']

# Merging and inserting data into PostgreSQL
analysis_df = consolidated_analysis_df.merge(consolidated_integration_df, on='RiskRecon_ID')

# Make sure 'RiskRecon_ID' exists in df before merging
if 'RiskRecon_ID' not in df.columns:
    print("Warning: 'RiskRecon_ID' column not found in the Excel data. Check your TU_List.xlsx file.")
    # You might need to create this mapping or use a different join key

# Only perform merges if dataframes have common columns
if set(['RiskRecon_ID']).issubset(df.columns) and set(['RiskRecon_ID']).issubset(analysis_df.columns):
    final_analysis_df = analysis_df.merge(df, on='RiskRecon_ID')
    
    if set(['RiskRecon_ID']).issubset(consolidated_findings_df.columns):
        final_findings_df = consolidated_findings_df.merge(df, on='RiskRecon_ID')
        
        # Fix the renaming issue - assuming you want to rename 'Company_Name_y' to something else
        if 'Company_Name_y' in final_analysis_df.columns:
            final_analysis_df.rename(columns={'Company_Name_y': 'Company_Name'}, inplace=True)
        
        # Insert data into PostgreSQL
        insert_data_to_postgresql('RiskRecon_Remediation', final_findings_df)
        insert_data_to_postgresql('RiskRecon', final_analysis_df)
    else:
        print("Cannot create final_findings_df: 'RiskRecon_ID' missing in consolidated_findings_df")
else:
    print("Cannot create final_analysis_df: 'RiskRecon_ID' missing in df or analysis_df")

# Close database connection
cur.close()
conn.close()

# Option to save to CSV files for review
consolidated_findings_df.to_csv(findings_file, index=False)
consolidated_toe_df.to_csv(portfolio_file, index=False)
