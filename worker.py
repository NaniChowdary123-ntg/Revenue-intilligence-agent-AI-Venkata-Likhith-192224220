# dental_agents/worker.py
import json
import time
import traceback

from dental_agents.config import WORKER_ID, POLL_MS
from dental_agents.db import (
    get_conn, ensure_schema, lock_next_event, mark_done, mark_failed, log_run, enqueue_event
)
from dental_agents.idempotency import claim

from dental_agents.agents.appointment_agent import AppointmentAgent
from dental_agents.agents.inventory_agent import InventoryAgent
from dental_agents.agents.revenue_agent import RevenueAgent
from dental_agents.agents.case_tracking_agent import CaseTrackingAgent

APPT = AppointmentAgent()
INV = InventoryAgent()
REV = RevenueAgent()
CASE = CaseTrackingAgent()


def _parse_payload(row):
    try:
        return json.loads(row.get("payload_json") or "{}")
    except Exception:
        return {}


def _dispatch(conn, event_id: int, event_type: str, payload: dict):
    # events from Node server.js:
    # AppointmentCreated, AppointmentCompleted, CaseUpdated (+ internal periodic ticks)
    if event_type == "AppointmentCreated":
        APPT.handle(conn, event_type, event_id, payload)
        REV.handle(conn, event_type, event_id, payload)
        return

    if event_type == "AppointmentCompleted":
        APPT.handle(conn, event_type, event_id, payload)
        INV.handle(conn, event_type, event_id, payload)
        REV.handle(conn, event_type, event_id, payload)
        CASE.handle(conn, event_type, event_id, payload)
        return

    if event_type in ("CaseUpdated", "CaseGenerateSummary"):
        CASE.handle(conn, event_type, event_id, payload)
        return

    if event_type in ("AppointmentAutoScheduleRequested", "AppointmentMonitorTick"):
        APPT.handle(conn, event_type, event_id, payload)
        return

    if event_type.startswith("Inventory") or event_type == "InventoryDailyTick":
        INV.handle(conn, event_type, event_id, payload)
        return

    if event_type.startswith("Revenue") or event_type in ("ARRankAndNotify", "RevenueDailyTick"):
        REV.handle(conn, event_type, event_id, payload)
        return

    # unknown event: ignore
    return


def _enqueue_periodics(conn):
    # every minute: monitor late/no-show
    if claim(conn, "cron:minute:appt_monitor", ttl_seconds=55):
        enqueue_event(conn, "AppointmentMonitorTick", {}, priority=10, status="NEW")

    # hourly: inventory checks + revenue reminders/overdue + insights
    if claim(conn, "cron:hour:inventory", ttl_seconds=3600):
        enqueue_event(conn, "InventoryDailyTick", {"horizon_days": 30}, priority=10, status="NEW")

    if claim(conn, "cron:hour:revenue", ttl_seconds=3600):
        enqueue_event(conn, "RevenueDailyTick", {}, priority=10, status="NEW")


def run_loop():
    boot = get_conn()
    try:
        ensure_schema(boot)
    finally:
        boot.close()

    print(f"[python-worker] started id={WORKER_ID} poll={POLL_MS}ms")

    while True:
        conn = None
        try:
            conn = get_conn()

            # 1) enqueue periodics (single txn)
            try:
                _enqueue_periodics(conn)
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass

            # 2) lock next event (single txn)
            row = None
            try:
                row = lock_next_event(conn)
                conn.commit()  # release FOR UPDATE locks quickly
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                row = None

            if not row:
                time.sleep(POLL_MS / 1000.0)
                continue

            event_id = int(row["id"])
            event_type = str(row.get("event_type") or "")
            payload = _parse_payload(row)

            # 3) process event (single txn)
            try:
                log_run(conn, "worker", event_id, "STARTED")
                _dispatch(conn, event_id, event_type, payload)
                mark_done(conn, event_id)
                log_run(conn, "worker", event_id, "DONE")
                conn.commit()
            except Exception as e:
                err = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                try:
                    mark_failed(conn, event_id, err)
                    log_run(conn, "worker", event_id, "FAILED", err)
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

        except Exception as e:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            print(f"[python-worker] loop error: {e}")
            time.sleep(POLL_MS / 1000.0)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    run_loop()
