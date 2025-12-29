# dental_agents/agents/case_tracking_agent.py
from __future__ import annotations

from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Optional, List
import json

from ..db import get_conn
from ..notifications import create_notification


def _get_ist():
    try:
        from zoneinfo import ZoneInfo  # type: ignore
        try:
            return ZoneInfo("Asia/Kolkata")
        except Exception:
            return timezone(timedelta(hours=5, minutes=30), name="IST")
    except Exception:
        return timezone(timedelta(hours=5, minutes=30), name="IST")


IST = _get_ist()


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


def _risk_score(stage: str, next_review_date: Optional[date]) -> int:
    st = (stage or "").strip().upper()
    score = 30
    if st in ("BLOCKED", "URGENT"):
        score = 85
    elif st in ("IN_TREATMENT", "ACTIVE"):
        score = 55
    elif st in ("CLOSED", "RESOLVED"):
        score = 10

    if next_review_date and next_review_date <= _today() and st not in ("CLOSED", "RESOLVED"):
        score = min(100, score + 20)
    return score


def _insert_timeline(conn, *, case_id: int, event_type: str, message: str, meta: dict) -> None:
    """
    Append an entry to the ``case_timeline`` table.  The schema defines
    ``title`` and ``body`` columns rather than a single ``message``.  This
    helper stores the event_type as the title and the human message as the
    body, along with a JSON payload in ``meta_json``.  If the table does
    not exist, the operation is a no‑op.
    """
    with conn.cursor() as cur:
        if not _table_exists(cur, "case_timeline"):
            return
        cur.execute(
            """
            INSERT INTO case_timeline (case_id, event_type, title, body, meta_json, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                case_id,
                (event_type or "UPDATE")[:80],
                (event_type or "UPDATE")[:200],
                (message or "")[:5000],
                json.dumps(meta or {}, ensure_ascii=False),
            ),
        )


def _draft_summary(case_row: dict) -> dict:
    diagnosis = case_row.get("diagnosis") or "Not specified"
    stage = case_row.get("stage") or "ACTIVE"
    notes = case_row.get("notes") or ""
    plan = [
        "Review diagnosis and confirm treatment plan.",
        "Verify required procedures and estimated duration.",
        "Ensure follow-up date is set and reminders are enabled.",
    ]
    return {
        "summary": f"Draft case summary (pending doctor approval). Diagnosis: {diagnosis}. Stage: {stage}.",
        "recommendation": " | ".join(plan),
        "signals": {
            "has_notes": bool(notes),
            "has_diagnosis": bool(case_row.get("diagnosis")),
        },
        "confidence": 70 if case_row.get("diagnosis") else 55,
    }


def _on_case_updated_conn(conn, payload: Dict[str, Any]) -> None:
    case_id = int(payload.get("caseDbId") or payload.get("caseId") or 0)
    if not case_id:
        return

    case_row = None
    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        if not _table_exists(cur, "cases"):
            return

        cur.execute(
            """
            SELECT id, patient_id, doctor_id, stage, diagnosis, next_review_date, notes
            FROM cases
            WHERE id=%s
            """,
            (case_id,),
        )
        case_row = cur.fetchone()
        if not case_row:
            return

        _insert_timeline(
            conn,
            case_id=case_id,
            event_type="CASE_UPDATED",
            message="Case updated",
            meta={"payload": payload},
        )

        # Approval-gated draft summary
        if _table_exists(cur, "case_summaries"):
            # Generate a draft summary and insert it using the current schema.
            draft = _draft_summary(case_row)
            try:
                cur.execute(
                    """
                    INSERT INTO case_summaries
                      (case_id, summary, recommendation, confidence, status, created_by_agent, created_at)
                    VALUES
                      (%s, %s, %s, %s, 'PENDING_REVIEW', 1, NOW())
                    """,
                    (
                        case_id,
                        draft.get("summary") or "",
                        draft.get("recommendation") or "",
                        int(draft.get("confidence") or 0),
                    ),
                )
            except Exception:
                pass

            # Mirror summary and recommendation into the cases table if those columns exist.
            try:
                cur.execute(
                    """
                    UPDATE cases
                    SET agent_summary=%s,
                        agent_recommendation=%s,
                        approval_required=1,
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (
                        (draft.get("summary") or "")[:65535],
                        (draft.get("recommendation") or "")[:65535],
                        case_id,
                    ),
                )
            except Exception:
                pass

    # notifications after DB writes (still same workflow intent)
    doctor_id = int(case_row.get("doctor_id") or 0) if case_row else 0
    if doctor_id:
        create_notification(
            user_id=doctor_id,
            title="Case Review Needed",
            message=f"A draft summary was generated for Case #{case_id}. Please review and approve.",
            notif_type="CASE_REVIEW",
            related_table="cases",
            related_id=case_id,
        )

    nrd = case_row.get("next_review_date") if case_row else None
    if nrd and nrd <= _today() and doctor_id:
        create_notification(
            user_id=doctor_id,
            title="Follow-up Due",
            message=f"Follow-up is due for Case #{case_id} (next review date: {nrd}).",
            notif_type="FOLLOWUP_DUE",
            related_table="cases",
            related_id=case_id,
        )


def on_case_updated(payload: Dict[str, Any]) -> None:
    conn = get_conn()
    try:
        conn.begin() if hasattr(conn, "begin") else conn.start_transaction()
        _on_case_updated_conn(conn, payload)
        conn.commit()
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


def _on_appointment_completed_conn(conn, payload: Dict[str, Any]) -> None:
    appt_id = int(payload.get("appointmentId") or 0)
    if not appt_id:
        return

    linked_case_id = payload.get("linkedCaseId")
    linked_case_id = int(linked_case_id) if linked_case_id not in (None, "", 0) else None

    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        if not _table_exists(cur, "appointments"):
            return

        cur.execute("SELECT id, linked_case_id FROM appointments WHERE id=%s", (appt_id,))
        appt = cur.fetchone()
        if not appt:
            return

        if not linked_case_id and appt.get("linked_case_id"):
            try:
                linked_case_id = int(appt["linked_case_id"])
            except Exception:
                linked_case_id = None

        if not linked_case_id:
            return

        _insert_timeline(
            conn,
            case_id=linked_case_id,
            event_type="VISIT_COMPLETED",
            message="Visit completed",
            meta={"appointment_id": appt_id},
        )


def on_appointment_completed(payload: Dict[str, Any]) -> None:
    conn = get_conn()
    try:
        conn.begin() if hasattr(conn, "begin") else conn.start_transaction()
        _on_appointment_completed_conn(conn, payload)
        conn.commit()
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


class CaseTrackingAgent:
    """
    Worker-facing class (so worker.py can import CaseTrackingAgent).
    """

    def handle(self, conn, event_type: str, event_id: int, payload: Dict[str, Any]) -> None:
        """
        Dispatches events to the appropriate handler.  Supported events include:

        * CaseUpdated – create a draft summary and update risk on case change.
        * CaseGenerateSummary – run a more comprehensive summary across visits.
        * AppointmentCompleted – append a timeline entry when a visit is finished.
        """
        # Normal case updates always generate a draft summary and risk score.
        if event_type == "CaseUpdated":
            _on_case_updated_conn(conn, payload)
            return

        # When a doctor explicitly requests a summary via the web UI, we handle
        # CaseGenerateSummary.  This will look at the selected visit IDs (or all
        # visits in the case) and produce a more detailed summary and recommendation.
        if event_type == "CaseGenerateSummary":
            _on_case_generate_summary_conn(conn, payload)
            return

        # Upon appointment completion we record the fact in the case timeline.
        if event_type == "AppointmentCompleted":
            _on_appointment_completed_conn(conn, payload)
            return

        # Ignore unknown event types gracefully.
        return


def _on_case_generate_summary_conn(conn, payload: Dict[str, Any]) -> None:
    """
    Generate a consolidated AI summary for a case.  This function reads the
    clinical notes and procedure data from all associated visits (or a
    specified subset) and produces a summarised narrative along with
    recommendations.  The result is stored in the case_summaries table and
    surfaced on the case record.  A doctor must still review and approve the
    summary before it becomes part of the medico‑legal record.

    Expected payload structure:
      {
        "caseId": <int>,        # database ID of the case
        "visitIds": [<int>, ...],# optional list of visit database IDs to include
        "requestedBy": <int>     # optional user ID of the requester
      }
    """
    case_id = int(payload.get("caseId") or payload.get("caseDbId") or 0)
    if not case_id:
        return

    # Normalise visit IDs if provided
    visit_ids: Optional[List[int]] = None
    vlist = payload.get("visitIds") or payload.get("visitDbIds")
    if isinstance(vlist, list):
        ids = []
        for vid in vlist:
            try:
                n = int(vid)
                if n > 0:
                    ids.append(n)
            except Exception:
                continue
        if ids:
            visit_ids = ids

    case_row: Optional[Dict[str, Any]] = None
    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        # Fetch case meta (doctor, patient, type, etc.)
        cur.execute(
            """
            SELECT id, patient_id, doctor_id, case_type, stage
            FROM cases WHERE id=%s
            """,
            (case_id,),
        )
        case_row = cur.fetchone()
        if not case_row:
            return

        # Build a list of visits associated with this case
        visit_filter_sql = ""
        params: List[Any] = []
        if visit_ids:
            visit_filter_sql = " AND v.id IN (" + ",".join(["%s"] * len(visit_ids)) + ")"
            params.extend(visit_ids)

        if _table_exists(cur, "visits"):
            cur.execute(
                """
                SELECT v.id, v.started_at, v.ended_at, v.chief_complaint, v.clinical_notes,
                       v.diagnosis_text, v.procedures_json
                FROM visits v
                WHERE v.linked_case_id = %s
                """
                + visit_filter_sql +
                " ORDER BY v.started_at ASC",
                (case_id, *params),
            )
            rows = cur.fetchall() or []
        else:
            rows = []

    # Aggregate notes and procedures
    notes_sections: List[str] = []
    procedures: List[str] = []
    for r in rows:
        # prefer clinical_notes; fallback to chief_complaint or diagnosis_text
        text_parts: List[str] = []
        if r.get("clinical_notes"):
            text_parts.append(str(r.get("clinical_notes")))
        if r.get("chief_complaint"):
            text_parts.append(f"Complaint: {r.get('chief_complaint')}")
        if r.get("diagnosis_text"):
            text_parts.append(f"Diagnosis: {r.get('diagnosis_text')}")
        if text_parts:
            notes_sections.append("; ".join(text_parts))
        # extract procedure codes from JSON
        pj = r.get("procedures_json")
        if pj:
            try:
                arr = json.loads(pj)
                if isinstance(arr, list):
                    for it in arr:
                        code = (it.get("code") or it.get("procedure_code") or it.get("procedure_type")).strip() if isinstance(it, dict) else None
                        if code:
                            procedures.append(str(code))
            except Exception:
                pass

    # Fallback if no visits
    if not notes_sections and case_row:
        notes_sections.append(f"No visit notes available for case {case_id}.")

    # Compose summary text.  This simplistic implementation just concatenates
    # the notes.  In a real system this would call an LLM or summarisation
    # model.  We also include a short list of unique procedures involved.
    summary_text = " \n".join(notes_sections)
    uniq_procs = sorted(set([p.upper().replace("_", " ") for p in procedures]))
    if uniq_procs:
        summary_text += "\n\nProcedures involved: " + ", ".join(uniq_procs)

    recommendation = "Review the treatment timeline, confirm next appointment, and schedule follow‑ups as needed."

    # Confidence is a simple heuristic based on the number of visits considered
    conf = 50
    n_vis = len(rows)
    if n_vis >= 3:
        conf = 85
    elif n_vis == 2:
        conf = 70
    elif n_vis == 1:
        conf = 60

    # Persist to case_summaries and update cases.agent_summary/recommendation
    with conn.cursor() as cur:
        if _table_exists(cur, "case_summaries"):
            try:
                cur.execute(
                    """
                    INSERT INTO case_summaries
                      (case_id, summary, recommendation, confidence, status, created_by_agent, created_at)
                    VALUES
                      (%s, %s, %s, %s, 'PENDING_REVIEW', 1, NOW())
                    """,
                    (case_id, summary_text, recommendation, int(conf)),
                )
            except Exception:
                pass

        # Mirror into cases table for quick dashboard display
        try:
            cur.execute(
                """
                UPDATE cases
                SET agent_summary=%s,
                    agent_recommendation=%s,
                    approval_required=1,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (summary_text[:65535], recommendation[:65535], case_id),
            )
        except Exception:
            pass

        # Insert a case_timeline entry using the new schema
        if _table_exists(cur, "case_timeline"):
            try:
                cur.execute(
                    """
                    INSERT INTO case_timeline (case_id, event_type, title, body, meta_json, created_at)
                    VALUES (%s, 'SUMMARY_GENERATED', 'SUMMARY_GENERATED', %s, %s, NOW())
                    """,
                    (
                        case_id,
                        "AI summary generated",
                        json.dumps({"visit_ids": visit_ids or []}, ensure_ascii=False),
                    ),
                )
            except Exception:
                pass

    # Notify the assigned doctor that a new summary is ready
    doctor_id = int(case_row.get("doctor_id") or 0) if case_row else 0
    if doctor_id:
        create_notification(
            user_id=doctor_id,
            title="AI Summary Ready",
            message=f"A new AI summary is ready for Case #{case_id}. Please review and approve.",
            notif_type="CASE_SUMMARY_READY",
            related_table="cases",
            related_id=case_id,
        )
