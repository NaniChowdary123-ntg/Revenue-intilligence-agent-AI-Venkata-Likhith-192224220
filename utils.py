import json
import re
from datetime import datetime, date

def json_dumps(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"

def json_loads(s: str):
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}

def now_dt() -> datetime:
    return datetime.now()

def to_date_str(x) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s[:10] if len(s) >= 10 else s

def normalize_proc_code(type_str: str) -> str:
    """
    "Root Canal" -> "ROOT_CANAL"
    "General" -> "GENERAL"
    """
    s = (type_str or "GENERAL").strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "GENERAL"
