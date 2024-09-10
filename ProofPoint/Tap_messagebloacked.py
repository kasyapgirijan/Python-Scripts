import base64
import requests
import datetime
import time
import json
import pandas as pd
import logging
import urllib3
import os
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    filename="proofpoint_etl.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Function to read credentials from a file
def read_credentials(filename):
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Error reading credentials: {e}")
        raise

# Function to clean data and handle special characters
def clean_data(data):
    if isinstance(data, list):
        return [clean_data(item) for item in data]
    if isinstance(data, dict):
        return {key: clean_data(value) for key, value in data.items()}
    if isinstance(data, str):
        # Replace square brackets and commas with semicolons
        return data.replace('[', '').replace(']', '').replace(',', ';')
    return data

# Function to expand messagesBlocked and create a row for each threat
def expand_messages_blocked(messages_blocked):
    expanded_data = []
    for message in messages_blocked:
        base_data = clean_data(message)
        base_data.pop('threatsInfoMap', None)  # Avoid duplication of threat details
        if 'threatsInfoMap' in message:
            for threat in message['threatsInfoMap']:
                threat_data = clean_data(threat)
                # Merge base message data with threat data
                row_data = {**base_data, **threat_data}
                expanded_data.append(row_data)
        else:
            # No threatsInfoMap, add the base message data
            expanded_data.append(base_data)
    return expanded_data

# Function to split threatTime into separate date and time columns
def split_threat_time(data):
    for item in data:
        if 'threatTime' in item and item['threatTime']:
            try:
                threat_datetime = datetime.datetime.strptime(item['threatTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
                item['threatDate'] = threat_datetime.strftime("%Y-%m-%d")
                item['threatTimeSplit'] = threat_datetime.strftime("%H:%M:%S")
            except ValueError:
                item['threatDate'] = ''
                item['threatTimeSplit'] = ''
    return data

# Function to filter data based on desired headers
def filter_data(data, headers):
    return [{key: item.get(key, '') for key in headers} for item in data]

# Function to save data to CSV
def save_to_csv(data, folder_name, filename):
    os.makedirs(folder_name, exist_ok=True)
    df = pd.DataFrame(data)
    
    # Clean column names and ensure all columns are strings
    df.columns = [col.replace('[', '').replace(']', '').replace(',', ';') for col in df.columns]
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    file_path = os.path.join(folder_name, filename)
    df.to_csv(file_path, index=False)
    logging.info(f"Data saved to {file_path}")

# Function to perform API requests with retry and backoff for rate limiting
def api_request(url, headers, retries=5, backoff_factor=1):
    for retry in range(retries):
        try:
            response = requests.get(url, headers=headers, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logging.error(f"Other error occurred: {err}")
        
        # Exponential backoff for retries
        time.sleep(backoff_factor * (2 ** retry))
    raise Exception(f"Failed to fetch data after {retries} attempts")

# Main function to run the data extraction process
def extract_proofpoint_data():
    credentials = read_credentials('credentials.json')

    # Define request parameters
    req = {
        'principal': credentials['principal'], 
        'secret': credentials['secret'], 
        'uri': 'https://tap-api-v2.proofpoint.com', 
        'command': '/v2/siem/all',
        'parameters': '?format=json'
    }

    # Create the basic auth header
    userpass = f"{req['principal']}:{req['secret']}"
    encoded_u = base64.b64encode(userpass.encode()).decode()
    headers = {'Authorization': f'Basic {encoded_u}'}

    # Set time range (last 7 days)
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(days=7)
    
    total_hours = int((end_time - start_time).total_seconds() / 3600)
    completed_requests = 0
    all_messages_blocked = []

    # Loop through each hour
    current_time = start_time
    while current_time < end_time:
        next_time = current_time + datetime.timedelta(hours=1)
        interval = f'&interval=PT1H/{current_time.strftime("%Y-%m-%dT%H:%M:%SZ")}'
        url = f"{req['uri']}{req['command']}{req['parameters']}{interval}"
        
        try:
            response_data = api_request(url, headers)
            if 'messagesBlocked' in response_data:
                all_messages_blocked.extend(response_data['messagesBlocked'])
            logging.info(f"Successfully retrieved data for {current_time}")
        except Exception as e:
            logging.error(f"Failed to retrieve data for {current_time}: {e}")
        
        # Update progress
        completed_requests += 1
        logging.info(f"Completed {completed_requests}/{total_hours} requests")
        current_time = next_time
    
    # Process the data
    expanded_data = expand_messages_blocked(all_messages_blocked)
    expanded_data = split_threat_time(expanded_data)

    # Filter to include only specified headers
    headers_to_keep = [
        "threatsInfoMap", "spamScore", "phishScore", "messageTime", "impostorScore",
        "malwareScore", "subject", "quarantineFolder", "quarantineRule", "messageID",
        "threatID", "threatStatus", "classification", "detectionType", "threatURL",
        "threatTime", "threatDate", "threatTimeSplit", "threat", "campaignId", "threatType"
    ]
    filtered_data = filter_data(expanded_data, headers_to_keep)

    # Save to CSV
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"proofpoint_Email_Security_{timestamp}"
    filename = "Email_security.csv"
    save_to_csv(filtered_data, folder_name, filename)

if __name__ == "__main__":
    extract_proofpoint_data()
