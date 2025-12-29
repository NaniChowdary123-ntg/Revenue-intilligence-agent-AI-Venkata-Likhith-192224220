
import mysql.connector
import os
from dotenv import load_dotenv

# Load env from Backend/.env
# calculated path relative to this script or consistent absolute path
# Load env from .env in the same directory or current executing directory
# Assuming we run this from Backend root
from pathlib import Path
env_path = Path(".") / ".env"
load_dotenv(env_path)

host = os.getenv("DB_HOST", "localhost")
user = os.getenv("DB_USER", "root")
password = os.getenv("DB_PASSWORD", "")
database = os.getenv("DB_NAME", "dental_clinic")
port = int(os.getenv("DB_PORT", 3306))

print(f"Connecting to {host}:{port} as {user} for db {database}...")

try:
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    if conn.is_connected():
        print("SUCCESS: Connected to MySQL database!")
        conn.close()
    else:
        print("FAILED: Connection object returned but not connected.")
except Exception as e:
    print(f"ERROR: Could not connect to MySQL: {e}")
