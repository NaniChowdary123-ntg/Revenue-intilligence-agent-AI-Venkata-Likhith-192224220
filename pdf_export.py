from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from dental_agents.db import get_conn
from dental_agents.utils import json_loads

def export_case_pdf(case_id: int, out_path: str):
    """
    Generates a clean PDF using your existing tables:
    cases, users, case_timeline, case_summaries, case_attachments
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT c.*, p.full_name AS patient_name, d.full_name AS doctor_name
              FROM cases c
              JOIN users p ON p.id=c.patient_id
              LEFT JOIN users d ON d.id=c.doctor_id
              WHERE c.id=%s
              LIMIT 1
            """, (case_id,))
            case = cur.fetchone()
            if not case:
                raise RuntimeError(f"Case {case_id} not found")

            cur.execute("""
              SELECT event_type, title, body, created_at
              FROM case_timeline
              WHERE case_id=%s
              ORDER BY created_at ASC
              LIMIT 200
            """, (case_id,))
            timeline = cur.fetchall() or []

            cur.execute("""
              SELECT summary, recommendation, confidence, status, created_at
              FROM case_summaries
              WHERE case_id=%s
              ORDER BY created_at DESC
              LIMIT 5
            """, (case_id,))
            summaries = cur.fetchall() or []

        c = canvas.Canvas(out_path, pagesize=A4)
        w, h = A4
        y = h - 40

        def line(txt, dy=14, font="Helvetica", size=11):
            nonlocal y
            c.setFont(font, size)
            c.drawString(40, y, txt[:140])
            y -= dy
            if y < 60:
                c.showPage()
                y = h - 40

        c.setTitle(f"Case Report - {case.get('case_uid')}")
        line(f"Case Report: {case.get('case_uid')}", dy=18, font="Helvetica-Bold", size=16)
        line(f"Patient: {case.get('patient_name')}")
        line(f"Doctor: {case.get('doctor_name') or '—'}")
        line(f"Type: {case.get('case_type') or '—'}")
        line(f"Stage: {case.get('stage')}")
        line(f"Priority: {case.get('priority')}")
        line(f"Risk Score: {case.get('risk_score')}")
        line(f"Next Action: {case.get('next_action') or '—'}")
        line(f"Next Review Date: {case.get('next_review_date') or '—'}")

        y -= 10
        line("Latest Draft Summary", dy=16, font="Helvetica-Bold", size=13)
        if summaries:
            s0 = summaries[0]
            line(f"Status: {s0.get('status')} | Confidence: {s0.get('confidence')}")
            line("Summary:", font="Helvetica-Bold")
            for part in (s0.get("summary") or "").splitlines():
                line(part)
            if s0.get("recommendation"):
                line("Recommendation:", font="Helvetica-Bold")
                for part in (s0.get("recommendation") or "").splitlines():
                    line(part)
        else:
            line("No summaries yet.")

        y -= 10
        line("Timeline", dy=16, font="Helvetica-Bold", size=13)
        for t in timeline:
            line(f"- [{t.get('created_at')}] {t.get('event_type')}: {t.get('title') or ''}", font="Helvetica-Bold")
            if t.get("body"):
                for part in str(t["body"]).splitlines():
                    line(f"  {part}")

        c.save()
    finally:
        conn.close()
