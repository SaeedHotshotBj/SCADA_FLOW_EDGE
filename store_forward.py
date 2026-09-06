"""Durable local Store & Forward queue for SCADA_FLOW_EDGE."""

import json
import os
import sqlite3
import threading
import uuid

import config


_LOCK = threading.Lock()


def _db_path():
    path = getattr(config, "STORE_FORWARD_DB", "store_forward.db")
    if not os.path.isabs(path):
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            path,
        )
    return path


def _connect():
    path = _db_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    conn = sqlite3.connect(
        path,
        timeout=30,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_queue():
    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS StoreForwardQueue (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    EventID TEXT NOT NULL UNIQUE,
                    PLC_ID INTEGER NOT NULL,
                    TagName TEXT NOT NULL,
                    Value REAL,
                    Timestamp TEXT NOT NULL,
                    CommunicationTimeout REAL,
                    RetryCount INTEGER NOT NULL DEFAULT 0,
                    CreatedAt TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_store_forward_queue_id
                ON StoreForwardQueue (ID)
                """
            )
            conn.commit()
        finally:
            conn.close()


def enqueue(plc_id, tag, value, timestamp, communication_timeout=None):
    event_id = uuid.uuid4().hex

    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO StoreForwardQueue
                (
                    EventID,
                    PLC_ID,
                    TagName,
                    Value,
                    Timestamp,
                    CommunicationTimeout
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    int(plc_id),
                    str(tag),
                    value,
                    str(timestamp),
                    communication_timeout,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    return event_id


def enqueue_many(items):
    if not items:
        return 0

    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue

        plc_id = item.get("PLC_ID")
        tag = item.get("TagName")
        if plc_id is None or tag is None:
            continue

        rows.append(
            (
                uuid.uuid4().hex,
                int(plc_id),
                str(tag),
                item.get("Value"),
                str(item.get("Timestamp")),
                item.get("CommunicationTimeout"),
            )
        )

    if not rows:
        return 0

    with _LOCK:
        conn = _connect()
        try:
            conn.executemany(
                """
                INSERT INTO StoreForwardQueue
                (
                    EventID,
                    PLC_ID,
                    TagName,
                    Value,
                    Timestamp,
                    CommunicationTimeout
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        finally:
            conn.close()

    return len(rows)


def get_batch(limit=100):
    with _LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT
                    ID,
                    EventID,
                    PLC_ID,
                    TagName,
                    Value,
                    Timestamp,
                    CommunicationTimeout
                FROM StoreForwardQueue
                ORDER BY ID ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()


def delete_acked(event_ids):
    if not event_ids:
        return 0

    ids = [str(item) for item in event_ids if str(item).strip()]
    if not ids:
        return 0

    with _LOCK:
        conn = _connect()
        try:
            conn.executemany(
                "DELETE FROM StoreForwardQueue WHERE EventID = ?",
                [(event_id,) for event_id in ids],
            )
            count = conn.total_changes
            conn.commit()
            return count
        finally:
            conn.close()


def increment_retries(event_ids):
    if not event_ids:
        return

    ids = [str(item) for item in event_ids if str(item).strip()]
    if not ids:
        return

    with _LOCK:
        conn = _connect()
        try:
            conn.executemany(
                "UPDATE StoreForwardQueue SET RetryCount = RetryCount + 1 WHERE EventID = ?",
                [(event_id,) for event_id in ids],
            )
            conn.commit()
        finally:
            conn.close()


def pending_count():
    with _LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS Count FROM StoreForwardQueue"
            ).fetchone()
            return int(row["Count"])
        finally:
            conn.close()


def rows_to_payload(rows):
    return [
        {
            "EventID": row["EventID"],
            "PLC_ID": row["PLC_ID"],
            "TagName": row["TagName"],
            "Value": row["Value"],
            "Timestamp": row["Timestamp"],
        }
        for row in rows
    ]


__all__ = [
    "init_queue",
    "enqueue",
    "enqueue_many",
    "get_batch",
    "delete_acked",
    "increment_retries",
    "pending_count",
    "rows_to_payload",
]
