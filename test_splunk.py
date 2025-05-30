import requests
import urllib3

# ========== CONFIG ==========
SPLUNK_HOST = "https://localhost:8089"  # Change to IP or FQDN if needed
SPLUNK_TOKEN = "Splunk <your_token_here>"  # Paste token with "Splunk " prefix
VERIFY_SSL = False

# ========== TEST SEARCH ==========
test_search = {
    "search": "search index=_internal earliest=-15m | head 5",
    "output_mode": "json"
}

headers = {
    "Authorization": SPLUNK_TOKEN,
    "Content-Type": "application/x-www-form-urlencoded"
}

# ========== DISABLE SSL WARNINGS ==========
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== SEND TEST ==========
try:
    response = requests.post(
        f"{SPLUNK_HOST}/services/search/jobs",
        headers=headers,
        data=test_search,
        verify=VERIFY_SSL
    )

    print("\n=== REQUEST ===")
    print("POST", f"{SPLUNK_HOST}/services/search/jobs")
    print("Headers:", headers)
    print("Data:", test_search)

    print("\n=== RESPONSE ===")
    print("Status Code:", response.status_code)
    print(response.text)

    response.raise_for_status()

    sid = response.json().get("sid")
    print("\n✅ Success! Got SID:", sid)

except Exception as e:
    print("\n❌ Request failed:", e)
