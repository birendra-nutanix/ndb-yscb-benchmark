import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://10.111.48.231:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "admin123")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "Nutanix")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "tomcat_jmx_metrics")

# Grafana Configuration
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://10.111.48.231:3000")
