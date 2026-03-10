"""
core/sqlite_indexer.py
───────────────────────
Converts parsed transaction list → in-memory SQLite DB.
Raw PDF / full transactions list NEVER leaves this module.
Haiku only sees SQL query results (15-20 rows max).
"""

import sqlite3
import threading
from typing import Optional

# ── Per-session SQLite connections ────────────────────────
_DBS: dict[str, sqlite3.Connection] = {}
_DB_LOCK = threading.Lock()


def build_index(session_id: str, transactions: list) -> int:
    """
    Build an in-memory SQLite DB for this session.
    Returns number of rows inserted.
    """
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE transactions (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            date    TEXT,
            desc    TEXT,
            type    TEXT,
            amount  REAL,
            balance REAL
        )
    """)

    rows = [
        (
            t.get('date', ''),
            t.get('desc', ''),
            t.get('type', ''),
            t.get('amount') or 0.0,
            t.get('balance') or 0.0,
        )
        for t in transactions
    ]

    conn.executemany(
        "INSERT INTO transactions (date, desc, type, amount, balance) VALUES (?,?,?,?,?)",
        rows
    )
    conn.commit()

    with _DB_LOCK:
        # Close old connection if exists
        if session_id in _DBS:
            _DBS[session_id].close()
        _DBS[session_id] = conn

    return len(rows)


def get_db(session_id: str) -> Optional[sqlite3.Connection]:
    with _DB_LOCK:
        return _DBS.get(session_id)


def run_query(session_id: str, sql: str, limit: int = 50) -> list[dict]:
    """
    Run a SELECT query on session's SQLite DB.
    Always enforces LIMIT for safety.
    Returns list of dicts.
    """
    conn = get_db(session_id)
    if not conn:
        return []

    # Safety: only allow SELECT
    clean = sql.strip().upper()
    if not clean.startswith('SELECT'):
        return []

    # Inject LIMIT if missing
    if 'LIMIT' not in clean:
        sql = sql.rstrip(';') + f' LIMIT {limit}'

    try:
        cursor = conn.execute(sql)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        return [{'error': str(e)}]


def get_summary(session_id: str) -> dict:
    """
    Returns quick stats about the indexed DB.
    Used to give Haiku context without sending raw data.
    """
    conn = get_db(session_id)
    if not conn:
        return {}

    try:
        stats = conn.execute("""
            SELECT
                COUNT(*)                          AS total,
                SUM(CASE WHEN type='CR' THEN amount ELSE 0 END) AS total_cr,
                SUM(CASE WHEN type='DR' THEN amount ELSE 0 END) AS total_dr,
                MIN(date)                         AS from_date,
                MAX(date)                         AS to_date,
                MIN(balance)                      AS min_bal,
                MAX(balance)                      AS max_bal
            FROM transactions
        """).fetchone()

        return dict(stats) if stats else {}
    except Exception:
        return {}


def drop_index(session_id: str):
    """Clean up session DB from memory."""
    with _DB_LOCK:
        conn = _DBS.pop(session_id, None)
        if conn:
            conn.close()
            