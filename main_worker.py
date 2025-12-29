# dental_agents/worker.py
from __future__ import annotations

import json
import time
import traceback
from typing import Any, Dict

from dental_agents.config import WORKER_ID, POLL_MS
from dental_agents.db import (
    ensure_schema,
    get_conn,
    lock_next_event,
    mark_done,
    mark_failed,
    log_run,
)

from dental_agents.agents.appointment_agent import (
    on_appointment_created,
    on_appointment_completed,
    appointment_monitor_sweep,
)

from dental_agents.agents.inventory_agent import (
    on_appointment_completed as inv_on_completed,
    daily_inventory_checks,
)

from dental_agents.agents.revenue_agent import (
    on_appointment_created as rev_on_created,
    on_appointment_completed as rev_on_completed,
    daily_revenue_insights,
    ar_reminders_sweep,
)

from dental_agents.agents.case_tracking_agent import (
    on_case_updated,
    on_appointment_completed as case_on_completed,
)


def _parse_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return json.loads(row.get("payload_json") or "{}")
    except Exception:
        return {}


def dispatch(event_type: str, payload: Dict[str, Any]) -> None:
    # Events coming from Node server.js:
    # AppointmentCreated, AppointmentCompleted, CaseUpdated
    if event_type == "AppointmentCreated":
        on_appointment_created(payload)
        rev_on_created(payload)
        return

    if event_type == "AppointmentCompleted":
        on_appointment_completed(payload)
        inv_on_completed(payload)
        rev_on_completed(payload)
        case_on_completed(payload)
        return

    if event_type == "CaseUpdated":
        on_case_updated(payload)
        return

    # Optional cron/tick events (if you enqueue them from Node or OS scheduler)
    if event_type == "DailyInventoryChecks":
        daily_inventory_checks()
        return

    if event_type == "DailyRevenueInsights":
        daily_revenue_insights()
        ar_reminders_sweep()
        return

    if event_type == "AppointmentMonitorSweep":
        appointment_monitor_sweep()
        return


def main() -> None:
    print(f"[worker] starting id={WORKER_ID} poll={POLL_MS}ms")
    ensure_schema()

    while True:
        conn = None
        row = None
        try:
            conn = get_conn()
            row = lock_next_event(conn, worker_id=WORKER_ID)
            if not row:
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(POLL_MS / 1000.0)
                continue

            event_id = int(row["id"])
            event_type = str(row["event_type"])
            payload = _parse_payload(row)

            started_at = time.time()
            status = "DONE"
            err = None

            try:
                dispatch(event_type, payload)
            except Exception as e:
                status = "FAILED"
                err = "".join(traceback.format_exception(type(e), e, e.__traceback__))

            duration_ms = int((time.time() - started_at) * 1000)

            # log run
            try:
                log_run(
                    conn,
                    event_id=event_id,
                    event_type=event_type,
                    status=status,
                    duration_ms=duration_ms,
                    error_text=err,
                )
            except Exception:
                pass

            if status == "DONE":
                mark_done(conn, event_id)
            else:
                mark_failed(conn, event_id, err or "unknown_error")

        except Exception as outer:
            print("[worker] loop_error:", outer)
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
