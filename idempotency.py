# dental_agents/idempotency.py
from datetime import datetime, timedelta

from .config import WORKER_ID


def claim(conn, lock_key: str, ttl_seconds: int) -> bool:
    """
    Simple DB-based idempotency lock.
    Returns True if acquired/renewed, False if someone else holds it and not expired.
    """
    lock_key = (lock_key or "").strip()
    if not lock_key:
        return False

    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+05:30'")
        # remove expired lock if any
        cur.execute(
            "DELETE FROM idempotency_locks WHERE lock_key=%s AND expires_at <= NOW()",
            (lock_key,),
        )

        # try insert
        try:
            cur.execute(
                "INSERT INTO idempotency_locks (lock_key, locked_by, expires_at) VALUES (%s, %s, DATE_ADD(NOW(), INTERVAL %s SECOND))",
                (lock_key, WORKER_ID, int(ttl_seconds)),
            )
            return True
        except Exception:
            # if exists, try renew only if expired (already cleaned) -> otherwise fail
            return False
