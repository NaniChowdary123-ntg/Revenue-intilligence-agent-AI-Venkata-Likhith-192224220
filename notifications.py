# dental_agents/notifications.py
from __future__ import annotations
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from .db import get_conn

IST = ZoneInfo("Asia/Kolkata")

def _now_ist() -> datetime:
    return datetime.now(tz=IST)

def create_notification(
    *,
    user_id: int,
    title: str,
    message: str,
    notif_type: str = "INFO",
    related_table: Optional[str] = None,
    related_id: Optional[int] = None,
    scheduled_at: Optional[datetime] = None,
    channel: str = "IN_APP",
) -> int:
    """
    Inserts into notifications table.
    Assumes schema supports:
      notifications(id, user_id, title, message, type, channel, status, scheduled_at, sent_at, related_table, related_id, created_at)
    """
    if not user_id:
        return 0

    title = (title or "").strip()[:200] or "Notification"
    message = (message or "").strip()[:2000] or ""
    notif_type = (notif_type or "INFO").strip()[:50]
    channel = (channel or "IN_APP").strip()[:50]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            conn.begin()
            cur.execute("SET time_zone = '+05:30'")
            cur.execute(
                """
                INSERT INTO notifications
                  (user_id, title, message, type, channel, status, scheduled_at, related_table, related_id, created_at)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    int(user_id),
                    title,
                    message,
                    notif_type,
                    channel,
                    "PENDING" if scheduled_at else "NEW",
                    scheduled_at.strftime("%Y-%m-%d %H:%M:%S") if scheduled_at else None,
                    related_table,
                    related_id,
                ),
            )
            nid = int(cur.lastrowid)
            conn.commit()
            return nid
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
