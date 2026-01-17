import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kobo credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/a97B3UMS6QB9rrR4mtGqem/export-settings/esxy3BNW6X3JEAxxTRTP8fn/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

# Schema and table
schema_name = "mental_health"
table_name = "mental_health_wellbeing"

# -------------------------------
# Step 1: Fetch CSV from Kobo
# -------------------------------
print("Fetching data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))
if response.status_code != 200:
    raise Exception(f"Failed to fetch data: {response.status_code}")
print("✅ Data fetched successfully")

# -------------------------------
# Step 2: Load CSV into pandas
# -------------------------------
df = pd.read_csv(io.StringIO(response.text), sep=";", on_bad_lines="skip")

# Clean column names
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("&", "and")
    .str.replace("-", "_")
)

# Step 2a: Show columns and first rows
print("\nColumns in CSV after cleaning:")
print(df.columns.tolist())

print("\nFirst 5 rows in CSV:")
print(df.head())

# -------------------------------
# Step 3: Convert timestamps
# -------------------------------
if "start" in df.columns:
    df["start"] = pd.to_datetime(df["start"], errors="coerce")
if "end" in df.columns:
    df["end"] = pd.to_datetime(df["end"], errors="coerce")

# -------------------------------
# Step 4: PostgreSQL connection
# -------------------------------
print("\nConnecting to PostgreSQL...")
conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT
)
cur = conn.cursor()

# Create schema and table if not exists
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
cur.execute(f"""
    CREATE TABLE {schema_name}.{table_name} (
        id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        age_range TEXT,
        gender TEXT,
        location TEXT,
        stressed_or_overwhelmed TEXT,
        source_of_stress TEXT,
        mental_health_issue_experienced TEXT,
        rate_your_current_mental_health TEXT,
        cope_with_stress_or_emotional_challenges TEXT,
        Met_mental_health_professional TEXT,
        seeking_support TEXT,
        interested_in_attending_therapy_sessions TEXT,
        joining_mental_health_support_group TEXT,
        mental_health_support_you_need TEXT
    );
""")
print("✅ Table created successfully")

# -------------------------------
# Step 5: Map CSV columns to DB columns
# -------------------------------
COLUMN_MAP = {
    "start": "start_time",
    "end": "end_time",
    "age_range": "age_range",
    "gender": "gender",
    "location": "location",
    "stressed_or_overwhelmed": "stressed_or_overwhelmed",
    "source_of_stress": "source_of_stress",
    "mental_health_issue_experienced": "mental_health_issue_experienced",
    "rate_your_current_mental_health": "rate_your_current_mental_health",
    "cope_with_stress_or_emotional_challenges": "cope_with_stress_or_emotional_challenges",
    "met_mental_health_professional": "Met_mental_health_professional",
    "seeking_support": "seeking_support",
    "interested_in_attending_therapy_sessions": "interested_in_attending_therapy_sessions",
    "joining_mental_health_support_group": "joining_mental_health_support_group",
    "mental_health_support_you_need": "mental_health_support_you_need"
}

# Only keep columns that exist in CSV
existing_columns = [col for col in COLUMN_MAP.keys() if col in df.columns]
if len(existing_columns) == 0:
    raise Exception("⚠️ None of the expected columns exist in the CSV! Check column names.")

print("\nColumns that will be inserted into PostgreSQL:")
print(existing_columns)

# -------------------------------
# Step 6: Prepare insert
# -------------------------------
insert_query = f"""
    INSERT INTO {schema_name}.{table_name} ({', '.join([COLUMN_MAP[col] for col in existing_columns])})
    VALUES ({', '.join(['%s'] * len(existing_columns))})
"""

# Preview first row to check values
print("\nFirst row values that will be inserted:")
print([df.iloc[0].get(col) for col in existing_columns])

# -------------------------------
# Step 7: Insert data
# -------------------------------
values_list = [tuple(row.get(col) for col in existing_columns) for _, row in df.iterrows()]
if len(values_list) == 0:
    print("⚠️ CSV has no data rows. Nothing to insert.")
else:
    execute_batch(cur, insert_query, values_list)
    conn.commit()
    print(f"✅ {len(values_list)} rows inserted successfully!")

cur.close()
conn.close()

