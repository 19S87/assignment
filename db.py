"""SQLite-backed round-robin cursor (one row per Zammad group)."""
import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

log = logging.getLogger("rr.db")

DB_PATH = Path(os.getenv("RR_DB_PATH", "/data/round_robin.db"))


@contextmanager
def _conn():
    """Open a short-lived autocommit connection and always close it."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with _conn() as conn:
        # WAL: readers don't block the writer, and a crash can't corrupt the cursor.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS round_robin (
                group_id INTEGER PRIMARY KEY,
                last_user_id INTEGER
            )
            """
        )


def get_last_user(group_id: int):
    with _conn() as conn:
        row = conn.execute(
            "SELECT last_user_id FROM round_robin WHERE group_id = ?", (group_id,)
        ).fetchone()
        return row[0] if row else None


def set_last_user(group_id: int, user_id: int):
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO round_robin (group_id, last_user_id)
            VALUES (?, ?)
            ON CONFLICT(group_id) DO UPDATE SET last_user_id = excluded.last_user_id
            """,
            (group_id, user_id),
        )


def claim_next_user(group_id: int, agent_ids) -> int:
    """
    Atomically pick the next agent after the stored cursor and advance the cursor.

    Read + write happen in one BEGIN IMMEDIATE transaction, so two concurrent
    tickets can never be handed the same agent. "Next" means the first agent id
    greater than the last one used, wrapping around, so the rotation stays fair
    even when the previous agent was removed or deactivated.
    """
    ordered = sorted(agent_ids)
    if not ordered:
        raise ValueError("agent_ids must not be empty")

    with _conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT last_user_id FROM round_robin WHERE group_id = ?", (group_id,)
            ).fetchone()
            last = row[0] if row else None
            chosen = next(
                (a for a in ordered if last is not None and a > last), ordered[0]
            )
            conn.execute(
                """
                INSERT INTO round_robin (group_id, last_user_id)
                VALUES (?, ?)
                ON CONFLICT(group_id) DO UPDATE SET last_user_id = excluded.last_user_id
                """,
                (group_id, chosen),
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
    return chosen
