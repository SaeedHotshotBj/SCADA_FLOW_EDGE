"""Local Edge database for calculated samples and precomputed averages."""

import os
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import config

TZ = ZoneInfo("Asia/Tehran")
MINUTE_RETENTION_HOURS = max(2, int(getattr(config, "CALCULATED_MINUTE_RETENTION_HOURS", 2)))
HOUR_RETENTION_DAYS = max(2, int(getattr(config, "CALCULATED_HOUR_RETENTION_DAYS", 2)))
DAY_RETENTION_DAYS = max(1, int(getattr(config, "CALCULATED_DAY_RETENTION_DAYS", 600)))
RAW_RETENTION_DAYS = max(2, int(getattr(config, "CALCULATED_SAMPLE_RETENTION_DAYS", 2)))
DB_PATH = getattr(config, "LOCAL_CALCULATED_DB", "edge_calculated.db")


def _db_path():
    path = DB_PATH
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    return path


def _connect():
    conn = sqlite3.connect(_db_path(), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _parse_ts(value):
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip().replace("T", " ")
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            dt = None
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    pass
        if dt is None:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ).replace(tzinfo=None)
    return dt


def _ts(value):
    dt = _parse_ts(value)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".") if dt else None


def _bucket_start(dt, resolution):
    if resolution == "minute":
        return dt.replace(second=0, microsecond=0)
    if resolution == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _bucket_end(start, resolution):
    if resolution == "minute":
        return start + timedelta(minutes=1)
    if resolution == "hour":
        return start + timedelta(hours=1)
    return start + timedelta(days=1)


def init_calculated_db():
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS CalculatedSamples (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                PLC_ID INTEGER NOT NULL,
                TagName TEXT NOT NULL,
                Value REAL NOT NULL,
                Timestamp TEXT NOT NULL,
                CreatedAt TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );

            CREATE INDEX IF NOT EXISTS idx_calc_samples_lookup
            ON CalculatedSamples (PLC_ID, TagName, Timestamp);

            CREATE TABLE IF NOT EXISTS CalculatedAggregates (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                PLC_ID INTEGER NOT NULL,
                TagName TEXT NOT NULL,
                Resolution TEXT NOT NULL,
                PeriodStart TEXT NOT NULL,
                PeriodEnd TEXT NOT NULL,
                AverageValue REAL NOT NULL,
                MinValue REAL,
                MaxValue REAL,
                SampleCount INTEGER NOT NULL DEFAULT 0,
                Queued INTEGER NOT NULL DEFAULT 0,
                CreatedAt TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE (PLC_ID, TagName, Resolution, PeriodStart)
            );

            CREATE INDEX IF NOT EXISTS idx_calc_agg_pending
            ON CalculatedAggregates (Queued, Resolution, PeriodStart);

            CREATE INDEX IF NOT EXISTS idx_calc_agg_lookup
            ON CalculatedAggregates (PLC_ID, TagName, Resolution, PeriodStart);
            """
        )
        conn.commit()
    finally:
        conn.close()


def record_samples(items):
    if not items:
        return 0
    init_calculated_db()
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            plc_id = int(item["PLC_ID"])
            value = float(item["Value"])
            tag = str(item["TagName"]).strip()
            timestamp = _ts(item.get("Timestamp") or datetime.now(TZ).replace(tzinfo=None))
            if not tag or timestamp is None:
                continue
            rows.append((plc_id, tag, value, timestamp))
        except (KeyError, TypeError, ValueError):
            continue
    if not rows:
        return 0
    conn = _connect()
    try:
        conn.executemany(
            "INSERT INTO CalculatedSamples (PLC_ID,TagName,Value,Timestamp) VALUES (?,?,?,?)",
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _aggregate_bucket(conn, plc_id, tag_name, resolution, start, end):
    rows = conn.execute(
        """
        SELECT Value
        FROM CalculatedSamples
        WHERE PLC_ID=? AND TagName=?
          AND Timestamp>=? AND Timestamp<?
        ORDER BY Timestamp, ID
        """,
        (int(plc_id), tag_name, _ts(start), _ts(end)),
    ).fetchall()
    if not rows:
        return None

    values = []
    for row in rows:
        try:
            values.append(float(row["Value"]))
        except (TypeError, ValueError):
            pass
    if not values:
        return None

    average = sum(values) / len(values)
    return (
        average,
        min(values),
        max(values),
        len(values),
    )


def rollup_completed():
    init_calculated_db()
    now = datetime.now(TZ).replace(tzinfo=None, microsecond=0)
    results = 0
    conn = _connect()
    try:
        tag_rows = conn.execute(
            "SELECT DISTINCT PLC_ID, TagName FROM CalculatedSamples"
        ).fetchall()

        for tag_row in tag_rows:
            plc_id = int(tag_row["PLC_ID"])
            tag = str(tag_row["TagName"])

            for resolution in ("minute", "hour", "day"):
                completed = _bucket_start(now, resolution) - {
                    "minute": timedelta(minutes=1),
                    "hour": timedelta(hours=1),
                    "day": timedelta(days=1),
                }[resolution]

                last = conn.execute(
                    """
                    SELECT MAX(PeriodStart) AS LastStart
                    FROM CalculatedAggregates
                    WHERE PLC_ID=? AND TagName=? AND Resolution=?
                    """,
                    (plc_id, tag, resolution),
                ).fetchone()

                if last and last["LastStart"]:
                    cursor = _parse_ts(last["LastStart"]) + {
                        "minute": timedelta(minutes=1),
                        "hour": timedelta(hours=1),
                        "day": timedelta(days=1),
                    }[resolution]
                else:
                    oldest = conn.execute(
                        """
                        SELECT MIN(Timestamp) AS Oldest
                        FROM CalculatedSamples
                        WHERE PLC_ID=? AND TagName=?
                        """,
                        (plc_id, tag),
                    ).fetchone()["Oldest"]
                    cursor = _bucket_start(_parse_ts(oldest), resolution) if oldest else None

                if cursor is None:
                    continue

                retention_floor = {
                    "minute": now - timedelta(hours=MINUTE_RETENTION_HOURS),
                    "hour": now - timedelta(days=HOUR_RETENTION_DAYS),
                    "day": now - timedelta(days=DAY_RETENTION_DAYS),
                }[resolution]
                cursor = max(cursor, _bucket_start(retention_floor, resolution))

                limit = 240 if resolution == "minute" else 48 if resolution == "hour" else 10
                loops = 0
                while cursor <= completed and loops < limit:
                    end = _bucket_end(cursor, resolution)
                    aggregate = _aggregate_bucket(conn, plc_id, tag, resolution, cursor, end)
                    if aggregate is not None:
                        conn.execute(
                            """
                            INSERT INTO CalculatedAggregates
                            (PLC_ID,TagName,Resolution,PeriodStart,PeriodEnd,
                             AverageValue,MinValue,MaxValue,SampleCount,Queued)
                            VALUES (?,?,?,?,?,?,?,?,?,0)
                            ON CONFLICT(PLC_ID,TagName,Resolution,PeriodStart)
                            DO UPDATE SET
                                PeriodEnd=excluded.PeriodEnd,
                                AverageValue=excluded.AverageValue,
                                MinValue=excluded.MinValue,
                                MaxValue=excluded.MaxValue,
                                SampleCount=excluded.SampleCount
                            """,
                            (
                                plc_id,
                                tag,
                                resolution,
                                _ts(cursor),
                                _ts(end),
                                aggregate[0],
                                aggregate[1],
                                aggregate[2],
                                aggregate[3],
                            ),
                        )
                        results += 1
                    cursor = end
                    loops += 1

        conn.commit()
        return results
    finally:
        conn.close()


def pending_aggregates(limit=500):
    init_calculated_db()
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT ID, PLC_ID, TagName, Resolution, PeriodStart, PeriodEnd,
                   AverageValue, MinValue, MaxValue, SampleCount
            FROM CalculatedAggregates
            WHERE Queued=0
            ORDER BY ID
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def mark_queued(ids):
    ids = [int(item) for item in ids if item is not None]
    if not ids:
        return 0
    init_calculated_db()
    conn = _connect()
    try:
        conn.executemany(
            "UPDATE CalculatedAggregates SET Queued=1 WHERE ID=? AND Queued=0",
            [(item,) for item in ids],
        )
        count = conn.total_changes
        conn.commit()
        return count
    finally:
        conn.close()


def cleanup():
    init_calculated_db()
    now = datetime.now(TZ).replace(tzinfo=None)
    conn = _connect()
    try:
        conn.execute(
            "DELETE FROM CalculatedSamples WHERE Timestamp < ?",
            (_ts(now - timedelta(days=RAW_RETENTION_DAYS)),),
        )
        conn.execute(
            "DELETE FROM CalculatedAggregates WHERE Resolution='minute' AND PeriodStart < ?",
            (_ts(now - timedelta(hours=MINUTE_RETENTION_HOURS)),),
        )
        conn.execute(
            "DELETE FROM CalculatedAggregates WHERE Resolution='hour' AND PeriodStart < ?",
            (_ts(now - timedelta(days=HOUR_RETENTION_DAYS)),),
        )
        conn.execute(
            "DELETE FROM CalculatedAggregates WHERE Resolution='day' AND PeriodStart < ?",
            (_ts(now - timedelta(days=DAY_RETENTION_DAYS)),),
        )
        conn.commit()
    finally:
        conn.close()


def aggregate_storage_type(resolution):
    return {
        "minute": "CALCULATED_MINUTE",
        "hour": "CALCULATED_HOUR",
        "day": "CALCULATED_DAY",
    }.get(str(resolution).lower())


__all__ = [
    "init_calculated_db",
    "record_samples",
    "rollup_completed",
    "pending_aggregates",
    "mark_queued",
    "cleanup",
    "aggregate_storage_type",
]
