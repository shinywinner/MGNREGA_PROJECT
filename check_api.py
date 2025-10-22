import requests
import json

API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571bE"
RESOURCE_ID = "ee03643a-ee4c-48c2-ac30-9f2ff26ab722"
API_URL = f"https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722?api-key=579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b&format=json"

# Fetch datafset=0&limit=
response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()
    records = data.get("records", [])
    
    # Print the first record
    if records:
        print(json.dumps(records[0], indent=2))
    else:
        print("No records returned from API")
else:
    print("API request failed with status:", response.status_code)
