# dental_agents/agents/inventory_agent.py
from __future__ import annotations

from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, Optional, List, Tuple
import json

from ..db import get_conn

# --- timezone safe on Windows/Python 3.12 (no tzdata required) ---
try:
    from zoneinfo import ZoneInfo  # py3.9+
    try:
        IST = ZoneInfo("Asia/Kolkata")
    except Exception:
        IST = timezone(timedelta(hours=5, minutes=30), name="IST")
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30), name="IST")


LOW_STOCK_DEFAULT_THRESHOLD = 5
EXPIRY_ALERT_DAYS = 30


# ----------------------------
# cursor / row helpers (dict-safe)
# ----------------------------
def _cursor(conn):
    """
    mysql-connector: conn.cursor(dictionary=True)
    pymysql (DictCursor set in db.get_conn): conn.cursor()
    """
    try:
        return conn.cursor(dictionary=True)  # mysql-connector
    except TypeError:
        return conn.cursor()  # pymysql
    except Exception:
        return conn.cursor()


def _row_to_dict(cur, row):
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    # tuple -> dict (mysql-connector non-dict cursor)
    cols = []
    if hasattr(cur, "column_names") and cur.column_names:
        cols = list(cur.column_names)
    elif getattr(cur, "description", None):
        cols = [d[0] for d in cur.description]
    if not cols:
        return {}
    return {cols[i]: row[i] for i in range(min(len(cols), len(row)))}


def _rows_to_dicts(cur, rows):
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return rows
    return [_row_to_dict(cur, r) for r in rows]


def _today() -> date:
    return datetime.now(tz=IST).date()


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


# ----------------------------
# notifications (write directly to table; no tzdata dependency)
# ----------------------------
def _create_notification(
    conn,
    *,
    user_id: Optional[int],
    title: str,
    message: str,
    notif_type: str,
    related_table: Optional[str] = None,
    related_id: Optional[int] = None,
    scheduled_at: Optional[datetime] = None,
    meta: Optional[dict] = None,
    channel: str = "IN_APP",
    user_role: Optional[str] = None,
) -> None:
    """
    Inserts into notifications table created by db.ensure_schema().
    Safe if table doesn't exist.
    """
    cur = _cursor(conn)
    try:
        if not _table_exists(cur, "notifications"):
            return

        meta_payload = dict(meta or {})
        if related_table:
            meta_payload["related_table"] = related_table
        if related_id is not None:
            meta_payload["related_id"] = related_id

        sched = None
        if scheduled_at:
            # store as naive string for DB, DB time_zone is already set in worker/db
            sched = scheduled_at.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

        cur.execute(
            """
            INSERT INTO notifications
              (user_id, user_role, channel, type, title, message, status, scheduled_at, meta_json, created_at, updated_at)
            VALUES
              (%s, %s, %s, %s, %s, %s, 'PENDING', %s, %s, NOW(), NOW())
            """,
            (
                int(user_id) if user_id else None,
                user_role,
                channel,
                notif_type,
                (title or "")[:200],
                message or "",
                sched,
                json.dumps(meta_payload, ensure_ascii=False) if meta_payload else None,
            ),
        )
    finally:
        try:
            cur.close()
        except Exception:
            pass


# ----------------------------
# inventory logic
# ----------------------------
def _consume_items_from_visit(conn, visit_id: int) -> List[Dict[str, Any]]:
    """
    Try to find consumables usage:
      1) visit_consumables(visit_id, item_id, qty_used)
      2) visit_items(visit_id, inventory_item_id, qty) fallback
    Returns list of {item_id, qty}
    """
    out: List[Dict[str, Any]] = []
    cur = _cursor(conn)
    try:
        if _table_exists(cur, "visit_consumables"):
            cur.execute(
                "SELECT item_id, qty_used FROM visit_consumables WHERE visit_id=%s",
                (visit_id,),
            )
            rows = _rows_to_dicts(cur, cur.fetchall() or [])
            for r in rows:
                out.append(
                    {
                        "item_id": int(r.get("item_id") or 0),
                        "qty": float(r.get("qty_used") or 0),
                    }
                )
            return [x for x in out if x["item_id"] and x["qty"] > 0]

        if _table_exists(cur, "visit_items"):
            # discover columns
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='visit_items'
                """
            )
            cols_rows = _rows_to_dicts(cur, cur.fetchall() or [])
            cols = {r.get("COLUMN_NAME") for r in cols_rows if r.get("COLUMN_NAME")}

            item_col = "inventory_item_id" if "inventory_item_id" in cols else ("item_id" if "item_id" in cols else None)
            qty_col = "qty" if "qty" in cols else ("quantity" if "quantity" in cols else None)

            if item_col and qty_col:
                cur.execute(f"SELECT {item_col} AS item_id, {qty_col} AS qty FROM visit_items WHERE visit_id=%s", (visit_id,))
                rows = _rows_to_dicts(cur, cur.fetchall() or [])
                for r in rows:
                    out.append({"item_id": int(r.get("item_id") or 0), "qty": float(r.get("qty") or 0)})
            return [x for x in out if x["item_id"] and x["qty"] > 0]

        return []
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _get_inventory_stock_cols(cur) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (stock_col, threshold_col, expiry_col)
    """
    stock_col = None
    for c in ("stock", "current_stock", "qty_on_hand"):
        if _column_exists(cur, "inventory_items", c):
            stock_col = c
            break
    th_col = "reorder_threshold" if _column_exists(cur, "inventory_items", "reorder_threshold") else None
    exp_col = "expiry_date" if _column_exists(cur, "inventory_items", "expiry_date") else None
    return stock_col, th_col, exp_col


def _apply_consumption(conn, *, item_id: int, qty: float) -> Tuple[float, float]:
    """
    Decrement inventory_items stock.
    Returns (before, after). If item not found/unsupported returns (0,0).
    """
    if qty <= 0:
        return (0.0, 0.0)

    cur = _cursor(conn)
    try:
        if not _table_exists(cur, "inventory_items"):
            return (0.0, 0.0)

        stock_col, _, _ = _get_inventory_stock_cols(cur)
        if not stock_col:
            return (0.0, 0.0)

        # lock row
        cur.execute(f"SELECT id, {stock_col} AS stock FROM inventory_items WHERE id=%s FOR UPDATE", (item_id,))
        row = _row_to_dict(cur, cur.fetchone())
        if not row:
            return (0.0, 0.0)

        before = float(row.get("stock") or 0)
        after = before - float(qty)

        # update (updated_at might not exist in your schema; check)
        has_updated_at = _column_exists(cur, "inventory_items", "updated_at")
        if has_updated_at:
            cur.execute(
                f"UPDATE inventory_items SET {stock_col}=%s, updated_at=NOW() WHERE id=%s",
                (after, item_id),
            )
        else:
            cur.execute(
                f"UPDATE inventory_items SET {stock_col}=%s WHERE id=%s",
                (after, item_id),
            )

        # transaction log (optional)
        if _table_exists(cur, "inventory_transactions"):
            try:
                cur.execute(
                    """
                    INSERT INTO inventory_transactions (item_id, txn_type, qty, meta_json, created_at)
                    VALUES (%s, 'CONSUME', %s, %s, NOW())
                    """,
                    (item_id, float(qty), json.dumps({"before": before, "after": after}, ensure_ascii=False)),
                )
            except Exception:
                pass

        return (before, after)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def on_appointment_completed(conn, payload: Dict[str, Any]) -> None:
    """
    Workflow:
      - find visit for appointment
      - decrement stock for recorded consumables
      - notify low stock immediately if threshold crossed
    Payload: { appointmentId }
    """
    appt_id = int(payload.get("appointmentId") or 0)
    if not appt_id:
        return

    # 1) find visit + actors
    cur = _cursor(conn)
    try:
        if not _table_exists(cur, "visits"):
            return

        cur.execute(
            "SELECT id, patient_id, doctor_id FROM visits WHERE appointment_id=%s ORDER BY id DESC LIMIT 1",
            (appt_id,),
        )
        vr = _row_to_dict(cur, cur.fetchone())
        if not vr:
            return

        visit_id = int(vr.get("id") or 0)
        patient_id = int(vr.get("patient_id") or 0)
        doctor_id = int(vr.get("doctor_id") or 0)
        if not visit_id:
            return
    finally:
        try:
            cur.close()
        except Exception:
            pass

    # 2) consume items (read)
    items = _consume_items_from_visit(conn, visit_id)
    if not items:
        return

    touched: List[Tuple[int, float, float]] = []  # (item_id, before, after)

    # 3) apply consumption (within current worker transaction)
    for it in items:
        item_id = int(it["item_id"])
        qty = float(it["qty"])
        before, after = _apply_consumption(conn, item_id=item_id, qty=qty)
        if item_id:
            touched.append((item_id, before, after))

    # 4) low-stock check + anomaly notifications
    cur2 = _cursor(conn)
    try:
        if not _table_exists(cur2, "inventory_items"):
            return

        stock_col, th_col, _ = _get_inventory_stock_cols(cur2)
        if not stock_col:
            return

        for (item_id, before, after) in touched:
            cur2.execute(
                f"SELECT id, name, {stock_col} AS stock" + (f", {th_col} AS th" if th_col else "") + " FROM inventory_items WHERE id=%s",
                (item_id,),
            )
            r = _row_to_dict(cur2, cur2.fetchone())
            if not r:
                continue

            stock = float(r.get("stock") or 0)
            th = float(r.get("th") or LOW_STOCK_DEFAULT_THRESHOLD) if th_col else float(LOW_STOCK_DEFAULT_THRESHOLD)
            name = r.get("name") or f"Item #{item_id}"

            if stock <= th:
                _create_notification(
                    conn,
                    user_id=doctor_id or 1,
                    title="Low Stock Alert",
                    message=f"Inventory low: {name} (stock {stock:.2f}).",
                    notif_type="INVENTORY_LOW_STOCK",
                    related_table="inventory_items",
                    related_id=item_id,
                    meta={"stock": stock, "threshold": th, "appointment_id": appt_id, "visit_id": visit_id},
                )

            if stock < 0:
                _create_notification(
                    conn,
                    user_id=doctor_id or 1,
                    title="Inventory Anomaly",
                    message=f"{name} stock became negative ({stock:.2f}). Please reconcile.",
                    notif_type="INVENTORY_ANOMALY",
                    related_table="inventory_items",
                    related_id=item_id,
                    meta={"before": before, "after": stock, "appointment_id": appt_id, "visit_id": visit_id},
                )
    finally:
        try:
            cur2.close()
        except Exception:
            pass


def daily_inventory_checks(conn, *, horizon_days: int = EXPIRY_ALERT_DAYS) -> None:
    """
    Workflow (doc-aligned):
      - low stock alerts
      - expiry alerts within horizon_days
      - anomaly flags (negative stock)
      - purchase order draft suggestion (notification meta)
    """
    horizon_days = int(horizon_days or EXPIRY_ALERT_DAYS)

    cur = _cursor(conn)
    try:
        if not _table_exists(cur, "inventory_items"):
            return

        stock_col, th_col, exp_col = _get_inventory_stock_cols(cur)
        if not stock_col:
            return

        # low stock
        if th_col:
            cur.execute(
                f"""
                SELECT id, name, {stock_col} AS stock, {th_col} AS th
                FROM inventory_items
                WHERE {stock_col} <= {th_col}
                ORDER BY {stock_col} ASC
                LIMIT 200
                """
            )
        else:
            cur.execute(
                f"""
                SELECT id, name, {stock_col} AS stock
                FROM inventory_items
                WHERE {stock_col} <= %s
                ORDER BY {stock_col} ASC
                LIMIT 200
                """,
                (LOW_STOCK_DEFAULT_THRESHOLD,),
            )
        low_rows = _rows_to_dicts(cur, cur.fetchall() or [])

        # expiry
        exp_rows: List[dict] = []
        if exp_col:
            cutoff = _today() + timedelta(days=horizon_days)
            cur.execute(
                f"""
                SELECT id, name, {exp_col} AS expiry
                FROM inventory_items
                WHERE {exp_col} IS NOT NULL AND {exp_col} <= %s
                ORDER BY {exp_col} ASC
                LIMIT 200
                """,
                (cutoff,),
            )
            exp_rows = _rows_to_dicts(cur, cur.fetchall() or [])

        # anomalies
        cur.execute(
            f"""
            SELECT id, name, {stock_col} AS stock
            FROM inventory_items
            WHERE {stock_col} < 0
            ORDER BY {stock_col} ASC
            LIMIT 50
            """
        )
        neg_rows = _rows_to_dicts(cur, cur.fetchall() or [])

    finally:
        try:
            cur.close()
        except Exception:
            pass

    # notifications (admin default user_id=1)
    for r in low_rows:
        stock = float(r.get("stock") or 0)
        th = float(r.get("th") or LOW_STOCK_DEFAULT_THRESHOLD)
        _create_notification(
            conn,
            user_id=1,
            title="Low Stock Alert",
            message=f"{r.get('name') or ('Item #' + str(r.get('id')))} is low (stock {stock:.2f}, threshold {th:.2f}).",
            notif_type="INVENTORY_LOW_STOCK",
            related_table="inventory_items",
            related_id=int(r.get("id") or 0),
            meta={"stock": stock, "threshold": th},
        )

    for r in exp_rows:
        _create_notification(
            conn,
            user_id=1,
            title="Expiry Alert",
            message=f"{r.get('name') or ('Item #' + str(r.get('id')))} expires on {r.get('expiry')}.",
            notif_type="INVENTORY_EXPIRY",
            related_table="inventory_items",
            related_id=int(r.get("id") or 0),
            meta={"expiry": str(r.get("expiry"))},
        )

    for r in neg_rows:
        _create_notification(
            conn,
            user_id=1,
            title="Inventory Anomaly",
            message=f"{r.get('name') or ('Item #' + str(r.get('id')))} has negative stock ({float(r.get('stock') or 0):.2f}).",
            notif_type="INVENTORY_ANOMALY",
            related_table="inventory_items",
            related_id=int(r.get("id") or 0),
            meta={"stock": float(r.get("stock") or 0)},
        )

    # purchase draft suggestion
    if low_rows:
        draft = []
        for r in low_rows[:20]:
            item_id = int(r.get("id") or 0)
            th = float(r.get("th") or LOW_STOCK_DEFAULT_THRESHOLD)
            draft.append(
                {
                    "item_id": item_id,
                    "name": r.get("name"),
                    "suggest_qty": max(0, int(th * 2)),
                }
            )

        _create_notification(
            conn,
            user_id=1,
            title="Purchase Draft Suggested",
            message="Low stock items detected. A purchase draft is suggested.",
            notif_type="PURCHASE_DRAFT",
            related_table="inventory_items",
            related_id=int(low_rows[0].get("id") or 0),
            meta={"draft_items": draft},
        )


# ----------------------------
# Agent wrapper (so worker import works)
# ----------------------------
class InventoryAgent:
    """
    Worker-facing agent wrapper.
    """

    def handle(self, conn, event_type: str, event_id: int, payload: Dict[str, Any]) -> None:
        if event_type == "AppointmentCompleted":
            on_appointment_completed(conn, payload)
            return

        if event_type in ("InventoryDailyTick", "InventoryDailyCheck"):
            horizon = int((payload or {}).get("horizon_days") or EXPIRY_ALERT_DAYS)
            daily_inventory_checks(conn, horizon_days=horizon)
            return

        # ignore other events
        return
