import time
import pymysql
from config import MYSQL_CONFIG, logger

class BaseAgent:
    def __init__(self, agent_name):
        self.name = agent_name

    def get_db(self):
        return pymysql.connect(**MYSQL_CONFIG)

    def fetch_next_event(self, conn):
        sql = """
        SELECT * FROM agent_events
        WHERE status='NEW' AND available_at <= NOW()
        ORDER BY priority DESC, id ASC
        LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            evt = cur.fetchone()
            if evt:
                cur.execute(
                    "UPDATE agent_events SET status='PROCESSING', locked_by=%s, locked_until=DATE_ADD(NOW(), INTERVAL 5 MINUTE) WHERE id=%s",
                    (self.name, evt["id"]),
                )
                conn.commit()
            return evt

    def mark_done(self, conn, event_id):
        with conn.cursor() as cur:
            cur.execute("UPDATE agent_events SET status='DONE', updated_at=NOW() WHERE id=%s", (event_id,))
        conn.commit()

    def mark_failed(self, conn, event_id, err):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE agent_events SET status='FAILED', last_error=%s, updated_at=NOW() WHERE id=%s",
                (str(err)[:500], event_id),
            )
        conn.commit()

    def run(self):
        logger.info(f"🧠 Starting {self.name} agent loop...")
        while True:
            conn = self.get_db()
            try:
                evt = self.fetch_next_event(conn)
                if not evt:
                    time.sleep(1.2)
                    continue
                self.handle_event(conn, evt)
            except Exception as e:
                logger.error(f"[{self.name}] Fatal error: {e}")
                time.sleep(5)
            finally:
                conn.close()

    def handle_event(self, conn, evt):
        raise NotImplementedError
