import base64
import requests
import datetime
import time
import json

# Function to read credentials from a file
def read_credentials(filename):
    with open(filename, 'r') as file:
        return json.load(file)

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

# Initialize an empty list to store all the results
all_results = []

# Loop through each hour in the past 7 days
current_time = start_time
while current_time < end_time:
    next_time = current_time + datetime.timedelta(hours=1)
    interval = f'&interval=PT1H/{current_time.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    response = requests.get(req['uri'] + req['command'] + req['parameters'] + interval, headers=headers, verify=False)
    
    if response.status_code == 200:
        res = response.json()
        all_results.append(res)
    else:
        print(f"Failed to retrieve data for interval starting at {current_time}. Status code: {response.status_code}")
    
    # Sleep for a second to avoid hitting the API rate limit
    time.sleep(1)
    
    current_time = next_time

# Save all results to a JSON file
with open('proofpoint_data.json', 'w') as json_file:
    json.dump(all_results, json_file, indent=2)

print("Data has been saved to proofpoint_data.json")
