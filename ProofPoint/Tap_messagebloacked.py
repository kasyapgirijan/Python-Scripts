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

# Function to extract classifications from threatsInfoMap
def extract_classifications(data):
    if 'threatsInfoMap' in data:
        classifications = [threat['classification'] for threat in data['threatsInfoMap']]
        return ', '.join(classifications)
    return ''

# Function to save messagesBlocked data to Excel
def save_to_excel(data, folder_name, filename):
    os.makedirs(folder_name, exist_ok=True)
    cleaned_data = clean_data(data)
    
    # Extract classifications and add to cleaned data
    for item in cleaned_data:
        item['Classifications'] = extract_classifications(item)
    
    df = pd.DataFrame(cleaned_data)
    df.columns = [col.replace('[', '').replace(']', '').replace(',', ';') for col in df.columns]
    file_path = os.path.join(folder_name, filename)
    df.to_excel(file_path, index=False)
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

# Save the messagesBlocked data to an Excel file
folder_name = "proofpoint_data"
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"messages_blocked_{timestamp}.xlsx"
save_to_excel(all_messages_blocked, folder_name, filename)
