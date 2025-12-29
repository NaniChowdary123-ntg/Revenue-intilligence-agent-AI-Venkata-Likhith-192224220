# dental_agents/event_queue.py
from __future__ import annotations

from typing import Any, Dict, Optional

from .db import get_conn, enqueue_event as _enqueue_db


def enqueue_event(
    conn=None,
    event_type: str = "",
    payload: Dict[str, Any] = None,
    *,
    status: str = "NEW",
    priority: int = 50,
    run_at: Optional[str] = None,         # "YYYY-MM-DD HH:MM:SS"
    dedupe_key: Optional[str] = None,
    created_by_user_id: Optional[int] = None,
    correlation_id: Optional[str] = None,
    max_attempts: Optional[int] = None,
) -> int:
    """
    Convenience wrapper:
      - if conn not provided, opens/closes a new connection
      - otherwise uses existing conn (recommended inside worker)
    """
    payload = payload or {}

    if conn is not None:
        return _enqueue_db(
            conn,
            event_type,
            payload,
            status=status,
            priority=priority,
            run_at=run_at,
            dedupe_key=dedupe_key,
            created_by_user_id=created_by_user_id,
            correlation_id=correlation_id,
            max_attempts=max_attempts,
        )

    c = get_conn()
    try:
        c.begin() if hasattr(c, "begin") else c.start_transaction()
        eid = _enqueue_db(
            c,
            event_type,
            payload,
            status=status,
            priority=priority,
            run_at=run_at,
            dedupe_key=dedupe_key,
            created_by_user_id=created_by_user_id,
            correlation_id=correlation_id,
            max_attempts=max_attempts,
        )
        c.commit()
        return eid
    finally:
        try:
            c.close()
        except Exception:
            pass
