import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kobo credentials
KOBO_USERNAME = floreniyigena
KOBO_PASSWORD = Rukundo@julia12
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/ahVC6wcUev3fWdrhCgZmtE/export-settings/esja7vLghZGKfymazreQM37/data.csv"

# PostgreSQL credentials
PG_HOST = localhost
PG_DATABASE = postgres
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

PG_HOST=localhost
PG_DATABASE=postgres
PG_USER=postgres
PG_PASSWORD=Rukundo@julia12
PG_PORT=5432