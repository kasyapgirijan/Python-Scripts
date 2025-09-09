import base64
import requests
import csv
import json

# Function to read credentials from a file
def read_credentials(filename):
    with open(filename, 'r') as file:
        return json.load(file)

# Function to fetch data from Proofpoint SEIM API
def fetch_proofpoint_data(api_url, headers):
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None

# Function to process 'messagesBlocked' data
def process_messages_blocked(data):
    messages_blocked = data.get("messagesBlocked", [])
    processed_data = []
    
    for message in messages_blocked:
        base_data = {
            "GUID": message.get("GUID"),
            "QID": message.get("QID"),
            "fromAddress": message.get("fromAddress"),
            "subject": message.get("subject"),
            "senderIP": message.get("senderIP"),
            "messageTime": message.get("messageTime"),
            "quarantineFolder": message.get("quarantineFolder"),
            "spamScore": message.get("spamScore"),
            "phishScore": message.get("phishScore"),
            "malwareScore": message.get("malwareScore")
        }
        
        # Iterate through threatsInfoMap and add separate columns
        threats_info_map = message.get("threatsInfoMap", [])
        for idx, threat in enumerate(threats_info_map):
            threat_data = {
                f"threat_{idx+1}_campaignId": threat.get("campaignId"),
                f"threat_{idx+1}_classification": threat.get("classification"),
                f"threat_{idx+1}_threat": threat.get("threat"),
                f"threat_{idx+1}_threatType": threat.get("threatType"),
                f"threat_{idx+1}_threatStatus": threat.get("threatStatus"),
                f"threat_{idx+1}_threatUrl": threat.get("threatUrl"),
                f"threat_{idx+1}_threatTime": threat.get("threatTime")
            }
            base_data.update(threat_data)
        
        processed_data.append(base_data)
    
    return processed_data

# Function to save processed data to CSV
def save_to_csv(data, filename):
    if not data:
        print("No data to write.")
        return

    # Extract headers from the first item
    headers = data[0].keys()
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved to {filename}")

# Main function
def main():
    # Read credentials from a JSON file
    credentials = read_credentials('credentials.json')
    
    # Define request parameters
    api_url = "https://tap-api-v2.proofpoint.com/v2/siem/messagesBlocked"
    
    # Create the basic auth header
    userpass = f"{credentials['principal']}:{credentials['secret']}"
    encoded_u = base64.b64encode(userpass.encode()).decode()
    headers = {
        "Authorization": f"Basic {encoded_u}"
    }
    
    # Fetch and process data
    data = fetch_proofpoint_data(api_url, headers)
    
    if data:
        processed_data = process_messages_blocked(data)
        save_to_csv(processed_data, "proofpoint_messages_blocked.csv")

if __name__ == "__main__":
    main()


location_mapping =
VAR src =
    DISTINCT ( 'Mod_ThreatAwareness_Users'[office_location_1] )
RETURN
    ADDCOLUMNS (
        src,
        "Location",
        VAR ol = [office_location_1]
        /* remove leading "Remote -"/"Remote-" once, then trim */
        VAR noRemote =
            TRIM (
                IF (
                    LEFT ( ol, 8 ) = "Remote -",
                    SUBSTITUTE ( ol, "Remote -", "", 1 ),
                    IF ( LEFT ( ol, 7 ) = "Remote-", SUBSTITUTE ( ol, "Remote-", "", 1 ), ol )
                )
            )
        /* if there is " - " keep text before it, else keep as-is */
        VAR dashPos = SEARCH ( " - ", noRemote, 1, 0 )
        RETURN IF ( dashPos > 0, LEFT ( noRemote, dashPos - 1 ), noRemote )
    )
)
