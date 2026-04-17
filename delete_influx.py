import requests
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
        "predicate": f"_measurement=\"ndb_operations\" AND operation_id=\"b9b98f1c-bc62-4544-bd76-a0933f810966\""
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
