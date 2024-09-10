import base64
import requests
import datetime
import time
import json
import pandas as pd
import urllib3
import os
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(InsecureRequestWarning)

# Function to read credentials from a file
def read_credentials(filename):
    with open(filename, 'r') as file:
        return json.load(file)

# Function to clean data and handle special characters
def clean_data(data):
    if isinstance(data, list):
        return [clean_data(item) for item in data]
    if isinstance(data, dict):
        return {key: clean_data(value) for key, value in data.items()}
    if isinstance(data, str):
        # Replace square brackets and commas
        return data.replace('[', '').replace(']', '').replace(',', ';')
    return data

# Function to expand messagesBlocked and create a row for each threat
def expand_messages_blocked(messages_blocked):
    expanded_data = []
    for message in messages_blocked:
        base_data = clean_data(message)
        base_data.pop('threatsInfoMap', None)  # Remove threatsInfoMap to avoid duplication
        if 'threatsInfoMap' in message:
            for threat in message['threatsInfoMap']:
                threat_data = clean_data(threat)
                # Merge base message data with threat data
                row_data = {**base_data, **threat_data}
                expanded_data.append(row_data)
        else:
            # If no threatsInfoMap, add the base message data as is
            expanded_data.append(base_data)
    return expanded_data

# Function to split threatTime into separate date and time columns
def split_threat_time(data):
    for item in data:
        if 'threatTime' in item and item['threatTime']:
            try:
                # Parse the threatTime to a datetime object
                threat_datetime = datetime.datetime.strptime(item['threatTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
                # Add new keys for threatDate and threatTimeSplit
                item['threatDate'] = threat_datetime.strftime("%Y-%m-%d")
                item['threatTimeSplit'] = threat_datetime.strftime("%H:%M:%S")
            except ValueError:
                # Handle cases where threatTime is not in the expected format
                item['threatDate'] = ''
                item['threatTimeSplit'] = ''
    return data

# Function to filter data based on desired headers
def filter_data(data, headers):
    filtered_data = []
    for item in data:
        filtered_data.append({key: item.get(key, '') for key in headers})
    return filtered_data

# Function to save messagesBlocked data to CSV
def save_to_csv(data, folder_name, filename):
    os.makedirs(folder_name, exist_ok=True)
    df = pd.DataFrame(data)
    df.columns = [col.replace('[', '').replace(']', '').replace(',', ';') for col in df.columns]
    
    # Ensure all columns are converted to string type to avoid issues
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    file_path = os.path.join(folder_name, filename)
    df.to_csv(file_path, index=False)
    print(f"Data has been saved to {file_path}")

# Read credentials from the credentials file
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
userpass = req['principal'] + ':' + req['secret']
encoded_u = base64.b64encode(userpass.encode()).decode()
headers = {'Authorization': 'Basic %s' % encoded_u}

# Calculate the start time (7 days ago) and end time (current time)
end_time = datetime.datetime.utcnow()
start_time = end_time - datetime.timedelta(days=7)

# Calculate the total number of requests (1 per hour)
total_hours = int((end_time - start_time).total_seconds() / 3600)
completed_requests = 0

# Initialize an empty list to store the messagesBlocked data
all_messages_blocked = []

# Loop through each hour in the past 7 days
current_time = start_time
while current_time < end_time:
    next_time = current_time + datetime.timedelta(hours=1)
    interval = f'&interval=PT1H/{current_time.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    try:
        response = requests.get(req['uri'] + req['command'] + req['parameters'] + interval, headers=headers, verify=False)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        res = response.json()
        
        if 'messagesBlocked' in res:
            all_messages_blocked.extend(res['messagesBlocked'])
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve data for interval starting at {current_time}. Error: {e}")
    
    # Update progress
    completed_requests += 1
    elapsed_time = completed_requests  # Assuming each request takes about 1 second
    remaining_requests = total_hours - completed_requests
    estimated_time_remaining = remaining_requests  # in seconds
    eta = datetime.datetime.now() + datetime.timedelta(seconds=estimated_time_remaining)

    print(f"Completed {completed_requests}/{total_hours} requests. ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')}")

    # Sleep for a second to avoid hitting the API rate limit
    time.sleep(1)
    
    current_time = next_time

# Expand the messagesBlocked data
expanded_data = expand_messages_blocked(all_messages_blocked)
expanded_data = split_threat_time(expanded_data)

# Specify the headers to keep (including new columns for threatDate and threatTimeSplit)
headers_to_keep = [
    "threatsInfoMap", "spamScore", "phishScore", "messageTime", "impostorScore",
    "malwareScore", "subject", "quarantineFolder", "quarantineRule", "messageID",
    "threatID", "threatStatus", "classification", "detectionType", "threatURL",
    "threatTime", "threatDate", "threatTimeSplit", "threat", "campaignId", "threatType"
]

# Filter the data to include only specified headers
filtered_data = filter_data(expanded_data, headers_to_keep)

# Generate a timestamp for folder naming
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# Specify the folder name with timestamp and the fixed filename
folder_name = f"proofpoint_Email_Security_{timestamp}"
filename = "Email_security.csv"

# Save the filtered data to a CSV file in the specified folder
save_to_csv(filtered_data, folder_name, filename)
