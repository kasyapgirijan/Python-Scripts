import requests
import json
import pandas as pd
from datetime import datetime, timedelta

# Replace with your actual API endpoint and authentication details
api_url = "https://api.proofpoint.com/v2/siem/all"
api_token = "your_api_token_here"

headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

# Function to fetch data from Proofpoint SIEM API
def fetch_proofpoint_data(api_url, headers, start_time, end_time):
    all_data = []
    while True:
        params = {
            "format": "json",  # Specify the format if required
            "since": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "until": end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()  # Ensure the request was successful
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        # Assuming the API provides a way to get the next set of data
        if 'next' in response.links:
            api_url = response.links['next']['url']
        else:
            break
        # Update start_time for the next iteration to fetch the next batch of data
        start_time = end_time
    return all_data

# Function to retrieve historical data
def get_historical_data(api_url, headers, start_date, end_date):
    all_data = []
    current_start = start_date
    current_end = start_date + timedelta(days=1)
    
    while current_start < end_date:
        print(f"Fetching data from {current_start} to {current_end}")
        data = fetch_proofpoint_data(api_url, headers, current_start, current_end)
        all_data.extend(data)
        current_start = current_end
        current_end += timedelta(days=1)
        # To handle API limits, add a delay if necessary
        # time.sleep(1)
        
    return all_data

# Define the date range for historical data
start_date = datetime(2023, 1, 1)  # Start date (YYYY, M, D)
end_date = datetime(2023, 12, 31)  # End date (YYYY, M, D)

# Fetch historical data
historical_data = get_historical_data(api_url, headers, start_date, end_date)

# Convert the data to a pandas DataFrame
df = pd.DataFrame(historical_data)

# Save the data to an Excel file
excel_file = "proofpoint_historical_data.xlsx"
df.to_excel(excel_file, index=False)

print(f"Data extraction complete. Data saved to {excel_file}.")
