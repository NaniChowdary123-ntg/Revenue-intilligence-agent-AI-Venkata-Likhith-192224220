# dental_agents/agents/appointment_agent.py
from __future__ import annotations

from datetime import datetime, timedelta, date, time, timezone
from typing import Any, Dict, Optional, List, Tuple
import json

from ..db import get_conn
from ..notifications import create_notification


def _get_ist_tz():
    """
    Windows + Python 3.12 often doesn't ship tz database, so ZoneInfo("Asia/Kolkata")
    may crash unless tzdata is installed. This avoids breaking the worker.
    """
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        return ZoneInfo("Asia/Kolkata")
    except Exception:
        return timezone(timedelta(hours=5, minutes=30))


IST = _get_ist_tz()

DEFAULT_DURATIONS_MIN = {
    "CONSULTATION": 20,
    "CHECKUP": 20,
    "SCALING": 45,
    "FILLING": 60,
    "EXTRACTION": 45,
    "ROOT_CANAL": 90,
    "IMPLANT": 120,
}

# Business rules
GRACE_MIN_DELAY = 10        # after scheduled start → delayed
GRACE_MIN_NO_SHOW = 45      # after scheduled start → mark no-show (if still not started)
WORKDAY_START = time(9, 0)
WORKDAY_END = time(18, 0)
SLOT_STEP_MIN = 15


class AppointmentAgent:
    """
    Worker expects this class. It wraps the existing function-style handlers.
    """
    def handle(self, conn, event_type: str, event_id: int, payload: Dict[str, Any]) -> None:
        if event_type == "AppointmentCreated":
            on_appointment_created(payload, conn=conn)
            return
        if event_type == "AppointmentCompleted":
            on_appointment_completed(payload, conn=conn)
            return
        if event_type == "AppointmentMonitorTick":
            appointment_monitor_sweep(conn=conn)
            return
        if event_type == "AppointmentAutoScheduleRequested":
            # optional: implement later; ignore safely for now
            return


def _norm_proc_type(s: Any) -> str:
    t = (str(s or "").strip().upper().replace("-", "_").replace(" ", "_")) or "CONSULTATION"
    return t[:50]


def _parse_dt(val: Any) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        return val.astimezone(IST) if val.tzinfo else val.replace(tzinfo=IST)
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=IST)
        except Exception:
            pass
    return None


def _combine_date_time(d: Any, t: Any) -> Optional[datetime]:
    if not d or not t:
        return None
    ds = str(d).strip()
    ts = str(t).strip()
    if not ds or not ts:
        return None
    for tfmt in ("%H:%M:%S", "%H:%M"):
        try:
            dt = datetime.strptime(f"{ds} {ts}", f"%Y-%m-%d {tfmt}")
            return dt.replace(tzinfo=IST)
        except Exception:
            pass
    return None


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s
        LIMIT 1
        """,
        (name,),
    )
    return cur.fetchone() is not None


def _column_exists(cur, table: str, col: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s
        LIMIT 1
        """,
        (table, col),
    )
    return cur.fetchone() is not None


def _predict_duration_minutes(conn, procedure_type: str) -> int:
    """
    Estimate the expected duration for a procedure.  Uses the median
    ``actual_duration_min`` from ``visit_procedures`` if at least five
    historical values exist.  Falls back to ``DEFAULT_DURATIONS_MIN``.
    This helper transparently handles the column name change from
    ``procedure_type`` to ``procedure_code`` by querying whichever
    exists.  See schema_query.sql for details.
    """
    proc = _norm_proc_type(procedure_type)
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "visit_procedures"):
                return int(DEFAULT_DURATIONS_MIN.get(proc, 30))

            # Detect which column to query.  New schema uses procedure_code.
            col = "procedure_type"
            try:
                cur.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='visit_procedures'
                          AND COLUMN_NAME IN ('procedure_code','procedure_type')
                    LIMIT 1
                    """
                )
                c = cur.fetchone()
                if c:
                    # c may be tuple or dict
                    cname = c["COLUMN_NAME"] if isinstance(c, dict) else c[0]
                    if cname:
                        col = cname
            except Exception:
                pass

            # Query durations ordered ascending for median selection.
            cur.execute(
                f"""
                SELECT actual_duration_min
                FROM visit_procedures
                WHERE {col} = %s
                  AND actual_duration_min IS NOT NULL
                  AND actual_duration_min > 0
                ORDER BY actual_duration_min
                """,
                (proc,),
            )
            rows = cur.fetchall() or []
            vals: List[int] = []
            for r in rows:
                v = r["actual_duration_min"] if isinstance(r, dict) else r[0]
                if v:
                    vals.append(int(v))
            if len(vals) >= 5:
                vals.sort()
                mid = len(vals) // 2
                if len(vals) % 2:
                    med = vals[mid]
                else:
                    med = (vals[mid - 1] + vals[mid]) // 2
                # Bound the result within [10, 240]
                return max(10, min(med, 240))
    except Exception:
        pass

    return int(DEFAULT_DURATIONS_MIN.get(proc, 30))


def _overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


def _fetch_appt_datetime(appt_row: dict) -> Optional[datetime]:
    adt = _parse_dt(appt_row.get("appointment_datetime"))
    if adt:
        return adt
    return _combine_date_time(appt_row.get("scheduled_date"), appt_row.get("scheduled_time"))


def _fetch_appt_end_datetime(appt_row: dict, start_dt: Optional[datetime], duration_min: int) -> Optional[datetime]:
    if not start_dt:
        return None
    end_dt = _combine_date_time(appt_row.get("scheduled_date"), appt_row.get("scheduled_end_time"))
    if end_dt:
        return end_dt
    return start_dt + timedelta(minutes=int(duration_min))


def _write_audit(conn, appt_id: int, action: str, meta: dict) -> None:
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "appointment_audit_logs"):
                return
            cur.execute(
                """
                INSERT INTO appointment_audit_logs (appointment_id, action, meta_json, created_at)
                VALUES (%s, %s, %s, NOW())
                """,
                (appt_id, (action or "")[:50], json.dumps(meta or {}, ensure_ascii=False)),
            )
    except Exception:
        pass


def _update_predicted_fields(conn, appt_id: int, duration_min: int, start_dt: Optional[datetime]) -> None:
    if not start_dt:
        return
    try:
        with conn.cursor() as cur:
            has_pred = _column_exists(cur, "appointments", "predicted_duration_min")
            has_end = _column_exists(cur, "appointments", "scheduled_end_time")
            sets = []
            params: List[Any] = []
            if has_pred:
                sets.append("predicted_duration_min=%s")
                params.append(int(duration_min))
            if has_end:
                end_dt = start_dt + timedelta(minutes=int(duration_min))
                sets.append("scheduled_end_time=%s")
                params.append(end_dt.strftime("%H:%M:%S"))
            if not sets:
                return
            params.append(appt_id)
            cur.execute(f"UPDATE appointments SET {', '.join(sets)}, updated_at=NOW() WHERE id=%s", tuple(params))
    except Exception:
        pass


def _detect_conflicts(conn, appt_id: int, doctor_id: int, start_dt: datetime, end_dt: datetime, operatory_room_id: Optional[int]) -> List[dict]:
    conflicts: List[dict] = []
    with conn.cursor() as cur:
        has_operatory = _column_exists(cur, "appointments", "operatory_room_id")

        cur.execute(
            """
            SELECT id, scheduled_date, scheduled_time, scheduled_end_time, appointment_datetime, status, predicted_duration_min
            FROM appointments
            WHERE doctor_id=%s
              AND id<>%s
              AND status NOT IN ('CANCELLED','COMPLETED','NO_SHOW')
            """,
            (doctor_id, appt_id),
        )
        rows = cur.fetchall() or []
        for r in rows:
            row = r if isinstance(r, dict) else {}
            s = _fetch_appt_datetime(row)
            if not s:
                continue
            dur = int(row.get("predicted_duration_min") or 0)
            e = _fetch_appt_end_datetime(row, s, dur or 30)
            if e and _overlaps(start_dt, end_dt, s, e):
                conflicts.append({"type": "DOCTOR", "with_appointment_id": int(row["id"]), "at": str(s), "status": row.get("status")})

        if has_operatory and operatory_room_id:
            cur.execute(
                """
                SELECT id, scheduled_date, scheduled_time, scheduled_end_time, appointment_datetime, status, predicted_duration_min
                FROM appointments
                WHERE operatory_room_id=%s
                  AND id<>%s
                  AND status NOT IN ('CANCELLED','COMPLETED','NO_SHOW')
                """,
                (operatory_room_id, appt_id),
            )
            rows2 = cur.fetchall() or []
            for r in rows2:
                row = r if isinstance(r, dict) else {}
                s = _fetch_appt_datetime(row)
                if not s:
                    continue
                dur = int(row.get("predicted_duration_min") or 0)
                e = _fetch_appt_end_datetime(row, s, dur or 30)
                if e and _overlaps(start_dt, end_dt, s, e):
                    conflicts.append({"type": "OPERATORY", "with_appointment_id": int(row["id"]), "at": str(s), "status": row.get("status")})
    return conflicts


def on_appointment_created(payload: Dict[str, Any], conn=None) -> None:
    appt_id = int(payload.get("appointmentId") or 0)
    if not appt_id:
        return

    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()

    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SET time_zone = '+05:30'")
            except Exception:
                pass

            cur.execute("SELECT * FROM appointments WHERE id=%s", (appt_id,))
            appt_row = cur.fetchone()
            if not appt_row:
                return

            patient_id = int(appt_row.get("patient_id") or payload.get("patientId") or 0)
            doctor_id = int(appt_row.get("doctor_id") or payload.get("doctorId") or 0)
            appt_type = appt_row.get("type") or payload.get("type") or "CONSULTATION"
            # Operatory assignment – use operatory_id from the DB record or
            # payload.  The schema defines an ``operatory_id`` column rather
            # than ``operatory_room_id``.  Accept either field on the payload for
            # backward compatibility.
            operatory_room_id = (
                appt_row.get("operatory_id")
                or payload.get("operatoryId")
                or payload.get("operatoryRoomId")
            )
            operatory_room_id = (
                int(operatory_room_id)
                if operatory_room_id not in (None, "", 0)
                else None
            )

            start_dt = _fetch_appt_datetime(appt_row) or _parse_dt(payload.get("appointmentDateTime"))
            if not start_dt:
                start_dt = _combine_date_time(payload.get("scheduledDate"), payload.get("scheduledTime"))

            dur_min = _predict_duration_minutes(conn, appt_type)
            end_dt = (start_dt + timedelta(minutes=dur_min)) if start_dt else None

            _update_predicted_fields(conn, appt_id, dur_min, start_dt)

            conflicts = []
            if start_dt and end_dt and doctor_id:
                conflicts = _detect_conflicts(conn, appt_id, doctor_id, start_dt, end_dt, operatory_room_id)

            _write_audit(conn, appt_id, "CREATED", {"source": "python_agent", "predicted_duration_min": dur_min, "conflicts": conflicts})

        # send notifications outside cursor
        if conflicts and doctor_id:
            create_notification(
                user_id=doctor_id,
                title="Appointment Conflict Detected",
                message=f"Appointment #{appt_id} overlaps with existing booking(s). Please review.",
                notif_type="APPOINTMENT_CONFLICT",
                related_table="appointments",
                related_id=appt_id,
                meta={"conflicts": conflicts},
            )

        if start_dt and patient_id:
            now = datetime.now(tz=IST)
            pretty = start_dt.strftime("%d %b %Y, %I:%M %p")
            for hrs, label in [(24, "24h"), (2, "2h")]:
                when = start_dt - timedelta(hours=hrs)
                if when > now:
                    create_notification(
                        user_id=patient_id,
                        title=f"Appointment Reminder ({label})",
                        message=f"Your dental appointment is scheduled at {pretty}.",
                        notif_type="APPOINTMENT_REMINDER",
                        related_table="appointments",
                        related_id=appt_id,
                        scheduled_at=when,
                    )
                    if doctor_id:
                        create_notification(
                            user_id=doctor_id,
                            title=f"Upcoming Appointment ({label})",
                            message=f"Patient appointment at {pretty} (Type: {appt_type}).",
                            notif_type="APPOINTMENT_REMINDER",
                            related_table="appointments",
                            related_id=appt_id,
                            scheduled_at=when,
                        )

    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass


def appointment_monitor_sweep(conn=None) -> None:
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()

    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SET time_zone = '+05:30'")
            except Exception:
                pass

            today = datetime.now(tz=IST).date().strftime("%Y-%m-%d")
            cur.execute("SELECT * FROM appointments WHERE scheduled_date=%s", (today,))
            rows = cur.fetchall() or []

        now = datetime.now(tz=IST)

        for appt in rows:
            status = str(appt.get("status") or "").upper()
            if status in ("CANCELLED", "COMPLETED", "NO_SHOW"):
                continue

            appt_id = int(appt.get("id") or 0)
            patient_id = int(appt.get("patient_id") or 0)
            doctor_id = int(appt.get("doctor_id") or 0)

            start_dt = _fetch_appt_datetime(appt)
            if not start_dt:
                continue

            if now > start_dt + timedelta(minutes=GRACE_MIN_NO_SHOW):
                try:
                    with conn.cursor() as cur2:
                        cur2.execute("UPDATE appointments SET status='NO_SHOW', updated_at=NOW() WHERE id=%s", (appt_id,))
                        _write_audit(conn, appt_id, "NO_SHOW", {"source": "python_agent"})
                except Exception:
                    pass

                if patient_id:
                    create_notification(
                        user_id=patient_id,
                        title="Missed Appointment",
                        message="You missed your appointment. Please reschedule if needed.",
                        notif_type="APPOINTMENT_NO_SHOW",
                        related_table="appointments",
                        related_id=appt_id,
                    )
                if doctor_id:
                    create_notification(
                        user_id=doctor_id,
                        title="No-show Alert",
                        message=f"Patient did not arrive for Appointment #{appt_id}.",
                        notif_type="APPOINTMENT_NO_SHOW",
                        related_table="appointments",
                        related_id=appt_id,
                    )
                continue

            if now > start_dt + timedelta(minutes=GRACE_MIN_DELAY):
                if doctor_id:
                    create_notification(
                        user_id=doctor_id,
                        title="Appointment Running Late",
                        message=f"Appointment #{appt_id} appears delayed (scheduled {start_dt.strftime('%H:%M')}).",
                        notif_type="APPOINTMENT_DELAY",
                        related_table="appointments",
                        related_id=appt_id,
                    )

    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass


def on_appointment_completed(payload: Dict[str, Any], conn=None) -> None:
    appt_id = int(payload.get("appointmentId") or 0)
    if not appt_id:
        return

    linked_case_id = payload.get("linkedCaseId")
    linked_case_id = int(linked_case_id) if linked_case_id not in (None, "", 0) else None

    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()

    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SET time_zone = '+05:30'")
            except Exception:
                pass

            cur.execute("SELECT * FROM appointments WHERE id=%s", (appt_id,))
            appt = cur.fetchone()
            if not appt:
                return

            patient_id = int(appt.get("patient_id") or 0)
            doctor_id = int(appt.get("doctor_id") or 0)
            appt_type = appt.get("type") or "CONSULTATION"

            if not linked_case_id and appt.get("linked_case_id"):
                try:
                    linked_case_id = int(appt["linked_case_id"])
                except Exception:
                    linked_case_id = None

            visit_id = None
            if _table_exists(cur, "visits"):
                cur.execute("SELECT id FROM visits WHERE appointment_id=%s LIMIT 1", (appt_id,))
                r = cur.fetchone()
                if r:
                    visit_id = int(r["id"] if isinstance(r, dict) else r[0])
                else:
                    # Create a visit record when none exists.  The visits table
                    # uses a ``status`` column rather than ``visit_status``;
                    # default new visits to OPEN so that clinical staff can
                    # continue editing and then close them when complete.
                    cur.execute(
                        """
                        INSERT INTO visits (appointment_id, patient_id, doctor_id, linked_case_id, status, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, 'OPEN', NOW(), NOW())
                        """,
                        (appt_id, patient_id, doctor_id, linked_case_id),
                    )
                    visit_id = int(cur.lastrowid)

            if visit_id and _table_exists(cur, "visit_procedures"):
                # If no procedure rows exist for this visit, insert a basic
                # procedure record.  The schema uses ``procedure_code`` rather
                # than ``procedure_type`` and does not require an updated_at
                # column; unit_price can remain NULL until billing occurs.
                cur.execute("SELECT id FROM visit_procedures WHERE visit_id=%s LIMIT 1", (visit_id,))
                if not cur.fetchone():
                    predicted = _predict_duration_minutes(conn, appt_type)
                    cur.execute(
                        """
                        INSERT INTO visit_procedures (visit_id, procedure_code, qty, predicted_duration_min, created_at)
                        VALUES (%s, %s, 1, %s, NOW())
                        """,
                        (visit_id, _norm_proc_type(appt_type), predicted),
                    )

            _write_audit(conn, appt_id, "COMPLETED", {"source": "python_agent"})

        if patient_id:
            create_notification(
                user_id=patient_id,
                title="Appointment Completed",
                message="Your appointment is marked as completed. Billing and follow-ups (if any) will be updated shortly.",
                notif_type="APPOINTMENT_COMPLETED",
                related_table="appointments",
                related_id=appt_id,
            )

    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass
