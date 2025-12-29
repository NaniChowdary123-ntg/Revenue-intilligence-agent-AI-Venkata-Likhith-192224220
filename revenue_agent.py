# dental_agents/agents/revenue_agent.py
from __future__ import annotations

from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, List, Optional
import json

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

from ..notifications import create_notification

def _ist_tz():
    try:
        if ZoneInfo is not None:
            return ZoneInfo("Asia/Kolkata")
    except Exception:
        pass
    return timezone(timedelta(hours=5, minutes=30))

IST = _ist_tz()

AR_OVERDUE_DAYS = 14


def _today() -> date:
    return datetime.now(tz=IST).date()


def _norm(s: Any) -> str:
    return (str(s or "").strip().upper().replace("-", "_").replace(" ", "_"))[:50] or "CONSULTATION"


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


def _get_catalog_price(conn, procedure_type: str) -> Optional[float]:
    pt = _norm(procedure_type)
    with conn.cursor() as cur:
        if not _table_exists(cur, "procedure_catalog"):
            return None

        key_col = "procedure_type" if _column_exists(cur, "procedure_catalog", "procedure_type") else (
            "code" if _column_exists(cur, "procedure_catalog", "code") else None
        )
        price_col = "default_price" if _column_exists(cur, "procedure_catalog", "default_price") else None
        if not key_col or not price_col:
            return None

        cur.execute(f"SELECT {price_col} AS p FROM procedure_catalog WHERE {key_col}=%s LIMIT 1", (pt,))
        r = cur.fetchone()
        if r and (r.get("p") if isinstance(r, dict) else r[0]) is not None:
            return float(r.get("p") if isinstance(r, dict) else r[0])
    return None


def _sum_visit_items(conn, *, visit_id: int) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        if not _table_exists(cur, "visit_procedures"):
            return []

        proc_col = "procedure_type" if _column_exists(cur, "visit_procedures", "procedure_type") else (
            "procedure_code" if _column_exists(cur, "visit_procedures", "procedure_code") else None
        )
        qty_col = "qty" if _column_exists(cur, "visit_procedures", "qty") else (
            "quantity" if _column_exists(cur, "visit_procedures", "quantity") else None
        )
        unit_col = "unit_price" if _column_exists(cur, "visit_procedures", "unit_price") else None

        if not proc_col or not qty_col:
            return []

        sel_unit = f"{unit_col} AS unit_price" if unit_col else "NULL AS unit_price"
        cur.execute(
            f"""
            SELECT {proc_col} AS proc, {qty_col} AS qty, {sel_unit}
            FROM visit_procedures
            WHERE visit_id=%s
            """,
            (visit_id,),
        )
        rows = list(cur.fetchall() or [])

    items: List[Dict[str, Any]] = []
    for r in rows:
        pt = r.get("proc") if isinstance(r, dict) else r[0]
        qty = float(r.get("qty") if isinstance(r, dict) else r[1] or 1)
        unit = r.get("unit_price") if isinstance(r, dict) else r[2]
        if unit is None:
            unit = _get_catalog_price(conn, pt) or 0.0
        unit = float(unit or 0)
        items.append(
            {"procedure_type": _norm(pt), "qty": qty, "unit_price": unit, "amount": unit * qty}
        )
    return items


def _ensure_provisional_invoice(conn, *, appointment_id: int, patient_id: int, procedure_type: str) -> int:
    pt = _norm(procedure_type)
    with conn.cursor() as cur:
        if not _table_exists(cur, "invoices"):
            return 0

        # idempotent: if appointment_id + invoice_type exists
        if _column_exists(cur, "invoices", "appointment_id") and _column_exists(cur, "invoices", "invoice_type"):
            cur.execute(
                """
                SELECT id FROM invoices
                WHERE appointment_id=%s AND invoice_type='PROVISIONAL'
                ORDER BY id DESC LIMIT 1
                """,
                (appointment_id,),
            )
            r = cur.fetchone()
            if r:
                return int(r["id"] if isinstance(r, dict) else r[0])

        est = float(_get_catalog_price(conn, pt) or 0.0)

        cols = ["appointment_id", "patient_id", "invoice_type", "status", "amount"]
        vals = [appointment_id, patient_id, "PROVISIONAL", "PENDING", est]

        if _column_exists(cur, "invoices", "issue_date"):
            cols.append("issue_date")
            vals.append(_today().strftime("%Y-%m-%d"))

        if _column_exists(cur, "invoices", "created_at"):
            cols.append("created_at")
            vals.append(datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"))
        if _column_exists(cur, "invoices", "updated_at"):
            cols.append("updated_at")
            vals.append(datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"))

        placeholders = ",".join(["%s"] * len(vals))
        cur.execute(f"INSERT INTO invoices ({','.join(cols)}) VALUES ({placeholders})", tuple(vals))
        inv_id = int(cur.lastrowid)

        # invoice_items optional
        if _table_exists(cur, "invoice_items") and _column_exists(cur, "invoice_items", "invoice_id"):
            try:
                cur.execute(
                    """
                    INSERT INTO invoice_items (invoice_id, item_type, description, qty, unit_price, amount, created_at, updated_at)
                    VALUES (%s, 'PROCEDURE', %s, 1, %s, %s, NOW(), NOW())
                    """,
                    (inv_id, f"Estimated: {pt}", est, est),
                )
            except Exception:
                pass

        return inv_id


def _detect_leakage(conn, *, visit_id: int, invoice_id: int) -> Dict[str, Any]:
    flags = {"unbilled_procedures": False, "missing_charges": False, "details": {}}
    with conn.cursor() as cur:
        if not (_table_exists(cur, "visit_procedures") and _table_exists(cur, "invoice_items") and _table_exists(cur, "invoices")):
            return flags

        cur.execute("SELECT COUNT(*) AS c FROM visit_procedures WHERE visit_id=%s", (visit_id,))
        vp = int((cur.fetchone() or {}).get("c") or 0)

        cur.execute("SELECT COUNT(*) AS c FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
        ii = int((cur.fetchone() or {}).get("c") or 0)

        if vp > 0 and ii == 0:
            flags["unbilled_procedures"] = True

        cur.execute("SELECT amount FROM invoices WHERE id=%s", (invoice_id,))
        amt = float((cur.fetchone() or {}).get("amount") or 0)
        if vp > 0 and amt <= 0:
            flags["missing_charges"] = True

        flags["details"] = {"visit_procedure_count": vp, "invoice_item_count": ii, "invoice_amount": amt}
    return flags


def on_appointment_created(conn, payload: Dict[str, Any]) -> None:
    """
    Provisional billing on booking.
    """
    appt_id = int(payload.get("appointmentId") or 0)
    if not appt_id:
        return

    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        if not _table_exists(cur, "appointments"):
            return

        cur.execute("SELECT id, patient_id, type FROM appointments WHERE id=%s", (appt_id,))
        appt = cur.fetchone()
        if not appt:
            return

        patient_id = int(appt.get("patient_id") if isinstance(appt, dict) else appt[1] or 0)
        appt_type = (appt.get("type") if isinstance(appt, dict) else appt[2]) or payload.get("type") or "CONSULTATION"

    inv_id = _ensure_provisional_invoice(conn, appointment_id=appt_id, patient_id=patient_id, procedure_type=appt_type)

    if inv_id and patient_id:
        create_notification(
            user_id=patient_id,
            title="Provisional Bill Created",
            message="A provisional estimate has been created for your appointment. Final bill will be generated after completion.",
            notif_type="BILLING_PROVISIONAL",
            related_table="invoices",
            related_id=inv_id,
        )


def on_appointment_completed(conn, payload: Dict[str, Any]) -> None:
    """
    Finalize bill after completion using visit_procedures.
    Also generates leakage alerts.
    """
    appt_id = int(payload.get("appointmentId") or 0)
    if not appt_id:
        return

    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        if not _table_exists(cur, "appointments"):
            return

        cur.execute("SELECT id, patient_id, type FROM appointments WHERE id=%s", (appt_id,))
        appt = cur.fetchone()
        if not appt:
            return

        patient_id = int(appt.get("patient_id") if isinstance(appt, dict) else appt[1] or 0)
        appt_type = (appt.get("type") if isinstance(appt, dict) else appt[2]) or "CONSULTATION"

        # visit_id
        visit_id = 0
        if _table_exists(cur, "visits"):
            cur.execute("SELECT id FROM visits WHERE appointment_id=%s ORDER BY id DESC LIMIT 1", (appt_id,))
            vr = cur.fetchone()
            if vr:
                visit_id = int(vr["id"] if isinstance(vr, dict) else vr[0])

        # invoice id (prefer provisional)
        inv_id = 0
        if _table_exists(cur, "invoices") and _column_exists(cur, "invoices", "appointment_id"):
            cur.execute(
                """
                SELECT id, invoice_type
                FROM invoices
                WHERE appointment_id=%s
                ORDER BY (invoice_type='PROVISIONAL') DESC, id DESC
                LIMIT 1
                """,
                (appt_id,),
            )
            ir = cur.fetchone()
            if ir:
                inv_id = int(ir["id"] if isinstance(ir, dict) else ir[0])
            else:
                # create final invoice if needed
                cols = ["appointment_id", "patient_id", "invoice_type", "status", "amount"]
                vals = [appt_id, patient_id, "FINAL", "PENDING", 0.0]
                if _column_exists(cur, "invoices", "issue_date"):
                    cols.append("issue_date")
                    vals.append(_today().strftime("%Y-%m-%d"))
                if _column_exists(cur, "invoices", "created_at"):
                    cols.append("created_at"); vals.append(datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"))
                if _column_exists(cur, "invoices", "updated_at"):
                    cols.append("updated_at"); vals.append(datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"))

                cur.execute(
                    f"INSERT INTO invoices ({','.join(cols)}) VALUES ({','.join(['%s']*len(vals))})",
                    tuple(vals),
                )
                inv_id = int(cur.lastrowid)

    # items from visit
    items: List[Dict[str, Any]] = []
    if visit_id:
        items = _sum_visit_items(conn, visit_id=visit_id)

    if not items:
        est = float(_get_catalog_price(conn, appt_type) or 0.0)
        items = [{"procedure_type": _norm(appt_type), "qty": 1.0, "unit_price": est, "amount": est}]

    total = float(sum(float(x["amount"]) for x in items))

    with conn.cursor() as cur:
        if inv_id and _table_exists(cur, "invoice_items"):
            try:
                cur.execute("DELETE FROM invoice_items WHERE invoice_id=%s", (inv_id,))
            except Exception:
                pass

            for it in items:
                try:
                    cur.execute(
                        """
                        INSERT INTO invoice_items (invoice_id, item_type, description, qty, unit_price, amount, created_at, updated_at)
                        VALUES (%s, 'PROCEDURE', %s, %s, %s, %s, NOW(), NOW())
                        """,
                        (inv_id, _norm(it["procedure_type"]), float(it["qty"]), float(it["unit_price"]), float(it["amount"])),
                    )
                except Exception:
                    pass

        # finalize invoice
        if inv_id and _table_exists(cur, "invoices"):
            try:
                cur.execute(
                    """
                    UPDATE invoices
                    SET invoice_type='FINAL', amount=%s, status='PENDING'
                    """ + (", updated_at=NOW()" if _column_exists(cur, "invoices", "updated_at") else "") + """
                    WHERE id=%s
                    """,
                    (total, inv_id),
                )
            except Exception:
                pass

    # notify patient
    if patient_id and inv_id:
        create_notification(
            user_id=patient_id,
            title="Final Bill Generated",
            message="Your final bill has been generated. Please check billing section for details.",
            notif_type="BILLING_FINAL",
            related_table="invoices",
            related_id=inv_id,
        )

    # leakage alert
    if inv_id and visit_id:
        flags = _detect_leakage(conn, visit_id=visit_id, invoice_id=inv_id)
        if flags["unbilled_procedures"] or flags["missing_charges"]:
            create_notification(
                user_id=1,
                title="Revenue Leakage Alert",
                message=f"Potential leakage detected for Appointment #{appt_id} / Invoice #{inv_id}.",
                notif_type="REVENUE_LEAKAGE",
                related_table="invoices",
                related_id=inv_id,
                meta=flags,
            )


def daily_revenue_insights(conn) -> None:
    """
    Daily KPI report + simple forecast placeholder.
    Writes to revenue_insights if exists (best-effort).
    """
    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        if not _table_exists(cur, "invoices"):
            return

        today = _today()
        start = today.strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")

        cur.execute(
            """
            SELECT
              COUNT(*) AS invoices_count,
              SUM(CASE WHEN invoice_type='FINAL' THEN amount ELSE 0 END) AS final_revenue,
              SUM(CASE WHEN invoice_type='PROVISIONAL' THEN amount ELSE 0 END) AS provisional_value
            FROM invoices
            WHERE created_at >= %s AND created_at < %s
            """,
            (start, end),
        )
        stats = cur.fetchone() or {}
        final_rev = float(stats.get("final_revenue") or 0)
        inv_cnt = int(stats.get("invoices_count") or 0)

        cur.execute(
            """
            SELECT COALESCE(SUM(amount),0) AS s
            FROM invoices
            WHERE invoice_type='FINAL'
              AND issue_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            """
        )
        trailing_7 = float((cur.fetchone() or {}).get("s") or 0.0)

        insight = {
            "as_of_date": str(today),
            "final_revenue_today": final_rev,
            "invoices_created_today": inv_cnt,
            "forecast_next_7_days": trailing_7,
            "notes": [
                "Forecast uses trailing averages as deterministic baseline.",
                "AI narrative can be layered but core numbers are deterministic.",
            ],
        }

        if _table_exists(cur, "revenue_insights") and _column_exists(cur, "revenue_insights", "as_of_date") and _column_exists(cur, "revenue_insights", "raw_json"):
            try:
                cur.execute(
                    """
                    INSERT INTO revenue_insights (as_of_date, raw_json, created_at, updated_at)
                    VALUES (%s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE raw_json=VALUES(raw_json), updated_at=NOW()
                    """,
                    (today.strftime("%Y-%m-%d"), json.dumps(insight, ensure_ascii=False)),
                )
            except Exception:
                pass


def ar_reminders_sweep(conn) -> None:
    """
    AR reminders: PENDING invoices older than AR_OVERDUE_DAYS.
    """
    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        if not _table_exists(cur, "invoices"):
            return

        cutoff = (_today() - timedelta(days=AR_OVERDUE_DAYS)).strftime("%Y-%m-%d")
        issue_col = "issue_date" if _column_exists(cur, "invoices", "issue_date") else None
        if not issue_col:
            return

        cur.execute(
            f"""
            SELECT id, patient_id, amount, {issue_col} AS issue_date
            FROM invoices
            WHERE status IN ('PENDING','Pending','OVERDUE','Overdue')
              AND {issue_col} IS NOT NULL
              AND {issue_col} <= %s
            LIMIT 300
            """,
            (cutoff,),
        )
        rows = list(cur.fetchall() or [])

    for r in rows:
        pid = int(r["patient_id"] if isinstance(r, dict) else r[1] or 0)
        inv_id = int(r["id"] if isinstance(r, dict) else r[0])
        amt = float(r.get("amount") if isinstance(r, dict) else r[2] or 0)
        issue_date = (r.get("issue_date") if isinstance(r, dict) else r[3])

        if pid:
            create_notification(
                user_id=pid,
                title="Payment Reminder",
                message=f"Your invoice #{inv_id} (₹{amt:.2f}) is pending since {issue_date}.",
                notif_type="AR_REMINDER",
                related_table="invoices",
                related_id=inv_id,
            )


# -------------------------
# ✅ CLASS REQUIRED BY WORKER
# -------------------------
class RevenueAgent:
    """
    Worker calls: REV.handle(conn, event_type, event_id, payload)
    """

    def handle(self, conn, event_type: str, event_id: int, payload: Dict[str, Any]) -> None:
        if event_type == "AppointmentCreated":
            on_appointment_created(conn, payload)
            return

        if event_type == "AppointmentCompleted":
            on_appointment_completed(conn, payload)
            return

        if event_type in ("RevenueDailyTick", "ARRankAndNotify"):
            daily_revenue_insights(conn)
            ar_reminders_sweep(conn)
            return

        return
