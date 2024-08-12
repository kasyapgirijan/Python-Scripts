import base64
import requests
import datetime
import time
import json
import pandas as pd
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(InsecureRequestWarning)

# Function to read credentials from a file
def read_credentials(filename):
    with open(filename, 'r') as file:
        return json.load(file)

# Function to save messagesBlocked data to Excel
def save_to_excel(data, filename):
    df = pd.DataFrame(data)
    df.to_excel(filename, index=False)
    print(f"Data has been saved to {filename}")

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
save_to_excel(all_messages_blocked, 'messages_blocked.xlsx')
