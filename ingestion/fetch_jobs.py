import requests
import json

# Replace these with your Adzuna credentials
APP_ID = "d2c08e2a"
APP_KEY = "d5c4c6cff3e58d013813fa775152c49b"

url = "https://api.adzuna.com/v1/api/jobs/us/search/1"
params = {
    "app_id": APP_ID,
    "app_key": APP_KEY,
    "results_per_page": 10,  # keep small for testing
    "what": "data engineer",
}

response = requests.get(url, params=params)
data = response.json()

# Save locally in your project
with open("data/jobs_raw.json", "w") as f:
    json.dump(data, f, indent=2)

print("✅ Raw job data saved to data/jobs_raw.json")
