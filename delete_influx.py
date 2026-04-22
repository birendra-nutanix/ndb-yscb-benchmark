import requests
from dotenv import load_dotenv
load_dotenv()
import config

url = f"{config.INFLUXDB_URL}/api/v2/delete?org={config.INFLUXDB_ORG}&bucket={config.INFLUXDB_BUCKET}"
headers = {
    "Authorization": f"Token {config.INFLUXDB_TOKEN}",
    "Content-Type": "application/json"
}

measurements = ["ndb_operations", "ndb_databases"]

for measurement in measurements:
    data = {
        "start": "1970-01-01T00:00:00Z",
        "stop": "2030-01-01T00:00:00Z",
        "predicate": f"_measurement=\"ndb_operations\" AND operation_id=\"ef9a24a3-1fca-4de4-b2d4-a59580d4e784\""
    }

    try:
        resp = requests.post(url, headers=headers, json=data)
        print(f"Status Code for {measurement}: {resp.status_code}")
        if resp.status_code == 204:
            print(f"Successfully deleted all {measurement} from InfluxDB.")
        else:
            print(f"Failed to delete {measurement}. Response: {resp.text}")
    except Exception as e:
        print(f"Error deleting {measurement}: {e}")
