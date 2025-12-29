# dental_agents/config.py
import os
from dotenv import load_dotenv
from pathlib import Path


load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "dental_clinic")

APP_TZ = os.getenv("APP_TZ", "Asia/Kolkata")

WORKER_ID = os.getenv("WORKER_ID", "worker-1")
POLL_MS = int(os.getenv("POLL_MS", "1200"))

LOCK_TTL_SECONDS = int(os.getenv("LOCK_TTL_SECONDS", "60"))
MAX_EVENT_ATTEMPTS = int(os.getenv("MAX_EVENT_ATTEMPTS", "8"))

# operational knobs
NO_SHOW_GRACE_MIN = int(os.getenv("NO_SHOW_GRACE_MIN", "15"))
EXPIRY_HORIZON_DAYS = int(os.getenv("EXPIRY_HORIZON_DAYS", "30"))
AR_REMINDER_DAYS = int(os.getenv("AR_REMINDER_DAYS", "3"))
