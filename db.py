# dental_agents/db.py
import json
from typing import Any, Dict, Optional

from .config import (
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
    WORKER_ID, LOCK_TTL_SECONDS, MAX_EVENT_ATTEMPTS
)

# Try mysql-connector first, fallback to pymysql if user installed it
_CONNECTOR = None
try:
    import mysql.connector  # type: ignore
    _CONNECTOR = "mysql-connector"
except Exception:
    _CONNECTOR = None

if _CONNECTOR is None:
    try:
        import pymysql  # type: ignore
        import pymysql.cursors  # type: ignore
        _CONNECTOR = "pymysql"
    except Exception:
        raise RuntimeError(
            "No MySQL driver found. Install one:\n"
            "  pip install mysql-connector-python\n"
            "or\n"
            "  pip install pymysql"
        )

_SCHEMA_CACHE: Dict[str, Dict[str, bool]] = {}


def get_conn():
    if _CONNECTOR == "mysql-connector":
        import mysql.connector  # type: ignore
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=False,
        )
        return conn

    import pymysql  # type: ignore
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,  # type: ignore
    )
    return conn


def _dict_cursor(conn):
    # mysql-connector uses dictionary=True
    if _CONNECTOR == "mysql-connector":
        return conn.cursor(dictionary=True)
    return conn.cursor()


def _has_column(conn, table: str, col: str) -> bool:
    key = f"{table}"
    if key not in _SCHEMA_CACHE:
        _SCHEMA_CACHE[key] = {}
    if col in _SCHEMA_CACHE[key]:
        return _SCHEMA_CACHE[key][col]

    with _dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (DB_NAME, table, col),
        )
        ok = cur.fetchone() is not None
        _SCHEMA_CACHE[key][col] = ok
        return ok


def ensure_schema(conn):
    """
    Creates only ADDITIVE tables/columns needed for:
    - agent_events queue
    - agent_runs logs
    - notifications
    - idempotency_locks
    - case_timeline (for case tracking agent)
    """
    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_events (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              event_type VARCHAR(80) NOT NULL,
              payload_json LONGTEXT NULL,
              status ENUM('NEW','PENDING','PROCESSING','DONE','FAILED') NOT NULL DEFAULT 'NEW',
              priority INT NOT NULL DEFAULT 50,
              attempts INT NOT NULL DEFAULT 0,
              max_attempts INT NOT NULL DEFAULT 8,
              available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              locked_by VARCHAR(64) NULL,
              locked_at DATETIME NULL,
              last_error LONGTEXT NULL,
              done_at DATETIME NULL,
              correlation_id VARCHAR(64) NULL,
              created_by_user_id BIGINT NULL,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              KEY idx_status_available (status, available_at),
              KEY idx_locked_at (locked_at),
              KEY idx_event_type (event_type),
              KEY idx_priority (priority, id)
            ) ENGINE=InnoDB;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_runs (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              actor VARCHAR(64) NOT NULL,
              event_id BIGINT UNSIGNED NULL,
              status VARCHAR(24) NOT NULL,
              error_text LONGTEXT NULL,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              KEY idx_event (event_id),
              KEY idx_created_at (created_at)
            ) ENGINE=InnoDB;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              user_id BIGINT NULL,
              user_role VARCHAR(16) NULL,
              channel ENUM('IN_APP','EMAIL','SMS','WHATSAPP','CALL') NOT NULL DEFAULT 'IN_APP',
              type VARCHAR(64) NULL,
              title VARCHAR(200) NULL,
              message TEXT NOT NULL,
              status ENUM('PENDING','SENT','FAILED','READ') NOT NULL DEFAULT 'PENDING',
              scheduled_at DATETIME NULL,
              read_at DATETIME NULL,
              meta_json LONGTEXT NULL,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              sent_at DATETIME NULL,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              KEY idx_user (user_id),
              KEY idx_role (user_role),
              KEY idx_status (status),
              KEY idx_created_at (created_at),
              KEY idx_scheduled_at (scheduled_at)
            ) ENGINE=InnoDB;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS idempotency_locks (
              lock_key VARCHAR(120) NOT NULL,
              locked_by VARCHAR(64) NOT NULL,
              expires_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (lock_key),
              KEY idx_expires (expires_at)
            ) ENGINE=InnoDB;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS case_timeline (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              case_id BIGINT NOT NULL,
              event_type VARCHAR(80) NOT NULL,
              message TEXT NOT NULL,
              meta_json LONGTEXT NULL,
              created_by_user_id BIGINT NULL,
              created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              KEY idx_case (case_id),
              KEY idx_created_at (created_at)
            ) ENGINE=InnoDB;
            """
        )

    conn.commit()


def log_run(conn, actor: str, event_id: Optional[int], status: str, error_text: Optional[str] = None):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO agent_runs (actor, event_id, status, error_text) VALUES (%s, %s, %s, %s)",
            (actor, event_id, status, error_text),
        )


def enqueue_event(
    conn,
    event_type: str,
    payload: Dict[str, Any],
    *,
    status: str = "NEW",              # accept NEW or PENDING
    priority: int = 50,               # ✅ now supported
    run_at: Optional[str] = None,     # "YYYY-MM-DD HH:MM:SS"
    max_attempts: Optional[int] = None,
    created_by_user_id: Optional[int] = None,
    correlation_id: Optional[str] = None,
) -> int:
    """
    Insert an event into agent_events (outbox).
    IMPORTANT: does NOT begin/commit its own transaction.
    Caller controls commit/rollback.
    """
    payload_json = json.dumps(payload or {}, ensure_ascii=False)

    with conn.cursor() as cur:
        try:
            cur.execute("SET time_zone = '+05:30'")
        except Exception:
            pass

        # schema-safe optional cols
        has_available_at = _has_column(conn, "agent_events", "available_at")
        has_priority = _has_column(conn, "agent_events", "priority")
        has_max_attempts = _has_column(conn, "agent_events", "max_attempts")
        has_created_by = _has_column(conn, "agent_events", "created_by_user_id")
        has_corr = _has_column(conn, "agent_events", "correlation_id")

        cols = ["event_type", "payload_json", "status", "attempts", "created_at", "updated_at"]
        vals = ["%s", "%s", "%s", "0", "NOW()", "NOW()"]
        params = [event_type, payload_json, status]

        if has_priority:
            cols.insert(3, "priority")
            vals.insert(3, "%s")
            params.insert(3, int(priority))

        if has_max_attempts:
            cols.insert(4, "max_attempts")
            vals.insert(4, "%s")
            params.insert(4, int(max_attempts or MAX_EVENT_ATTEMPTS))

        if has_available_at:
            cols.insert(4, "available_at")
            vals.insert(4, "%s")
            params.insert(4, run_at)  # if None, DB default may apply depending on schema

        if has_created_by and created_by_user_id is not None:
            cols.insert(-2, "created_by_user_id")
            vals.insert(-2, "%s")
            params.append(int(created_by_user_id))

        if has_corr and correlation_id:
            cols.insert(-2, "correlation_id")
            vals.insert(-2, "%s")
            params.append(str(correlation_id)[:64])

        sql = f"INSERT INTO agent_events ({', '.join(cols)}) VALUES ({', '.join(vals)})"
        cur.execute(sql, tuple(params))
        return int(cur.lastrowid)


def lock_next_event(conn) -> Optional[Dict[str, Any]]:
    """
    Atomically claim the next available event.
    Picks both NEW and PENDING (Node may insert PENDING).
    """
    try:
        conn.cursor().execute("SET time_zone = '+05:30'")
    except Exception:
        pass

    has_locked_by = _has_column(conn, "agent_events", "locked_by")
    has_locked_at = _has_column(conn, "agent_events", "locked_at")
    has_available_at = _has_column(conn, "agent_events", "available_at")

    where = ["status IN ('NEW','PENDING')"]
    params = []

    if has_available_at:
        where.append("available_at <= NOW()")

    if has_locked_at:
        where.append("(locked_at IS NULL OR locked_at < DATE_SUB(NOW(), INTERVAL %s SECOND))")
        params.append(int(LOCK_TTL_SECONDS))

    where_sql = " AND ".join(where)

    cur = _dict_cursor(conn)
    try:
        cur.execute(
            f"""
            SELECT *
            FROM agent_events
            WHERE {where_sql}
            ORDER BY priority ASC, id ASC
            LIMIT 1
            FOR UPDATE
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            return None

        event_id = int(row["id"])

        # claim it
        if has_locked_by and has_locked_at:
            cur.execute(
                """
                UPDATE agent_events
                SET status='PROCESSING',
                    attempts = attempts + 1,
                    locked_by = %s,
                    locked_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (WORKER_ID, event_id),
            )
        elif has_locked_at:
            cur.execute(
                """
                UPDATE agent_events
                SET status='PROCESSING',
                    attempts = attempts + 1,
                    locked_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (event_id,),
            )
        else:
            cur.execute(
                """
                UPDATE agent_events
                SET status='PROCESSING',
                    attempts = attempts + 1,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (event_id,),
            )

        # re-read (dict)
        cur2 = _dict_cursor(conn)
        try:
            cur2.execute("SELECT * FROM agent_events WHERE id = %s", (event_id,))
            return cur2.fetchone()
        finally:
            try:
                cur2.close()
            except Exception:
                pass
    finally:
        try:
            cur.close()
        except Exception:
            pass


def mark_done(conn, event_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE agent_events
            SET status='DONE',
                done_at=NOW(),
                locked_by=NULL,
                locked_at=NULL,
                updated_at=NOW()
            WHERE id=%s
            """,
            (event_id,),
        )


def mark_failed(conn, event_id: int, error_text: str):
    """
    Backoff + retry until max_attempts, else FAILED.
    """
    with _dict_cursor(conn) as cur:
        cur.execute("SELECT attempts, max_attempts FROM agent_events WHERE id=%s", (event_id,))
        r = cur.fetchone() or {}
        attempts = int(r.get("attempts") or 0)
        max_attempts = int(r.get("max_attempts") or MAX_EVENT_ATTEMPTS)

        # exponential-ish backoff capped at 10 minutes
        backoff = min(600, max(10, (2 ** min(attempts, 8)) * 5))

        if attempts >= max_attempts:
            cur.execute(
                """
                UPDATE agent_events
                SET status='FAILED',
                    last_error=%s,
                    locked_by=NULL,
                    locked_at=NULL,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (error_text, event_id),
            )
        else:
            cur.execute(
                """
                UPDATE agent_events
                SET status='NEW',
                    last_error=%s,
                    locked_by=NULL,
                    locked_at=NULL,
                    available_at = DATE_ADD(NOW(), INTERVAL %s SECOND),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (error_text, int(backoff), event_id),
            )
