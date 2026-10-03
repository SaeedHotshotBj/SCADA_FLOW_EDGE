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
                    StorageType TEXT,
                    HistoryResolution TEXT,
                    PeriodEnd TEXT,
                    FirstValue REAL,
                    LastValue REAL,
                    MinValue REAL,
                    MaxValue REAL,
                    WeightedAverage REAL,
                    DurationSeconds REAL,
                    SampleCount INTEGER,
                    CommunicationTimeout REAL,
                    RetryCount INTEGER NOT NULL DEFAULT 0,
                    CreatedAt TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
                )
                """
            )
            required_columns = {
                "StorageType": "TEXT",
                "HistoryResolution": "TEXT",
                "PeriodEnd": "TEXT",
                "FirstValue": "REAL",
                "LastValue": "REAL",
                "MinValue": "REAL",
                "MaxValue": "REAL",
                "WeightedAverage": "REAL",
                "DurationSeconds": "REAL",
                "SampleCount": "INTEGER",
            }
            existing_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(StoreForwardQueue)").fetchall()
            }
            for column, column_type in required_columns.items():
                if column not in existing_columns:
                    conn.execute(
                        f"ALTER TABLE StoreForwardQueue ADD COLUMN {column} {column_type}"
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
                    StorageType,
                    HistoryResolution,
                    PeriodEnd,
                    FirstValue,
                    LastValue,
                    MinValue,
                    MaxValue,
                    WeightedAverage,
                    DurationSeconds,
                    SampleCount,
                    CommunicationTimeout
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    int(plc_id),
                    str(tag),
                    value,
                    str(timestamp),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
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
                item.get("StorageType"),
                item.get("HistoryResolution"),
                item.get("PeriodEnd"),
                item.get("FirstValue"),
                item.get("LastValue"),
                item.get("MinValue"),
                item.get("MaxValue"),
                item.get("WeightedAverage"),
                item.get("DurationSeconds"),
                item.get("SampleCount"),
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
                    StorageType,
                    HistoryResolution,
                    PeriodEnd,
                    FirstValue,
                    LastValue,
                    MinValue,
                    MaxValue,
                    WeightedAverage,
                    DurationSeconds,
                    SampleCount,
                    CommunicationTimeout
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    StorageType,
                    HistoryResolution,
                    PeriodEnd,
                    FirstValue,
                    LastValue,
                    MinValue,
                    MaxValue,
                    WeightedAverage,
                    DurationSeconds,
                    SampleCount,
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
            "StorageType": row.get("StorageType"),
            "HistoryResolution": row.get("HistoryResolution"),
            "PeriodEnd": row.get("PeriodEnd"),
            "FirstValue": row.get("FirstValue"),
            "LastValue": row.get("LastValue"),
            "MinValue": row.get("MinValue"),
            "MaxValue": row.get("MaxValue"),
            "WeightedAverage": row.get("WeightedAverage"),
            "DurationSeconds": row.get("DurationSeconds"),
            "SampleCount": row.get("SampleCount"),
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
