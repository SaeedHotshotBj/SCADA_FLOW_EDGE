"""Local Edge historian and time-weighted aggregates for TIME tags."""

from datetime import datetime, timedelta
import os
import sqlite3
import threading

import config


_LOCK = threading.RLock()
_RESOLUTIONS = ("minute", "hour", "day")


def _db_path():
    path = getattr(config, "STORE_FORWARD_DB", "store_forward.db")
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    return path


def _connect():
    path = _db_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_edge_data():
    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS PLC_Data (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    PLC_ID INTEGER NOT NULL,
                    TagName TEXT NOT NULL,
                    Value REAL NOT NULL,
                    StorageType TEXT NOT NULL CHECK(StorageType='TIME'),
                    Timestamp TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_edge_plc_data_tag_time ON PLC_Data(PLC_ID, TagName, Timestamp)"
            )
            for table in ("TrendMinute", "TrendHour", "TrendDay"):
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                        PLC_ID INTEGER NOT NULL,
                        TagName TEXT NOT NULL,
                        PeriodStart TEXT NOT NULL,
                        PeriodEnd TEXT NOT NULL,
                        FirstValue REAL,
                        LastValue REAL,
                        MinValue REAL,
                        MaxValue REAL,
                        WeightedAverage REAL,
                        DurationSeconds REAL NOT NULL DEFAULT 0,
                        SampleCount INTEGER NOT NULL DEFAULT 0,
                        UploadQueued INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(PLC_ID, TagName, PeriodStart)
                    )
                    """
                )
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table.lower()}_tag_time ON {table}(PLC_ID, TagName, PeriodStart)"
                )
            conn.commit()
        finally:
            conn.close()


def _parse_ts(value):
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def _format_ts(value):
    return value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")


def _bucket_start(dt, resolution):
    if resolution == "minute":
        return dt.replace(second=0, microsecond=0)
    if resolution == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if resolution == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError("Unsupported resolution")


def _period_end(start, resolution):
    if resolution == "minute":
        return start + timedelta(minutes=1)
    if resolution == "hour":
        return start + timedelta(hours=1)
    if resolution == "day":
        return start + timedelta(days=1)
    raise ValueError("Unsupported resolution")


def _table(resolution):
    return {
        "minute": "TrendMinute",
        "hour": "TrendHour",
        "day": "TrendDay",
    }[resolution]


def _previous_sample(conn, plc_id, tag, timestamp):
    row = conn.execute(
        """
        SELECT ID, Value, Timestamp
        FROM PLC_Data
        WHERE PLC_ID=? AND LOWER(TagName)=LOWER(?) AND Timestamp < ?
        ORDER BY Timestamp DESC, ID DESC
        LIMIT 1
        """,
        (int(plc_id), str(tag), timestamp),
    ).fetchone()
    return dict(row) if row else None


def _calculate_period(conn, plc_id, tag, start, resolution, as_of):
    end = _period_end(start, resolution)
    effective_end = min(end, as_of)
    if effective_end <= start:
        return None

    carry = conn.execute(
        """
        SELECT ID, Value, Timestamp
        FROM PLC_Data
        WHERE PLC_ID=? AND LOWER(TagName)=LOWER(?) AND Timestamp <= ?
        ORDER BY Timestamp DESC, ID DESC
        LIMIT 1
        """,
        (int(plc_id), str(tag), _format_ts(start)),
    ).fetchone()

    rows = conn.execute(
        """
        SELECT ID, Value, Timestamp
        FROM PLC_Data
        WHERE PLC_ID=? AND LOWER(TagName)=LOWER(?)
          AND Timestamp > ? AND Timestamp < ?
        ORDER BY Timestamp ASC, ID ASC
        """,
        (int(plc_id), str(tag), _format_ts(start), _format_ts(effective_end)),
    ).fetchall()

    samples = []
    if carry is not None:
        samples.append(dict(carry))
    samples.extend(dict(row) for row in rows)
    if not samples:
        return None

    segments = []
    integral = 0.0
    duration = 0.0
    minimum = None
    maximum = None

    for index, sample in enumerate(samples):
        ts = _parse_ts(sample["Timestamp"])
        if ts is None:
            continue
        next_ts = effective_end
        if index + 1 < len(samples):
            candidate = _parse_ts(samples[index + 1]["Timestamp"])
            if candidate is not None:
                next_ts = min(next_ts, candidate)
        seg_start = max(ts, start)
        seg_end = min(next_ts, effective_end)
        if seg_end <= seg_start:
            continue
        try:
            value = float(sample["Value"])
        except (TypeError, ValueError):
            continue
        seconds = (seg_end - seg_start).total_seconds()
        if seconds <= 0:
            continue
        integral += value * seconds
        duration += seconds
        minimum = value if minimum is None else min(minimum, value)
        maximum = value if maximum is None else max(maximum, value)
        segments.append((seg_start, seg_end, value))

    if not segments or duration <= 0:
        return None

    return {
        "PLC_ID": int(plc_id),
        "TagName": str(tag),
        "PeriodStart": _format_ts(start),
        "PeriodEnd": _format_ts(end),
        "FirstValue": segments[0][2],
        "LastValue": segments[-1][2],
        "MinValue": minimum,
        "MaxValue": maximum,
        "WeightedAverage": integral / duration,
        "DurationSeconds": duration,
        "SampleCount": len(segments),
    }


def _upsert_period(conn, aggregate):
    table = _table(str(aggregate.get("Resolution")))
    conn.execute(
        f"""
        INSERT INTO {table}
        (PLC_ID, TagName, PeriodStart, PeriodEnd, FirstValue, LastValue,
         MinValue, MaxValue, WeightedAverage, DurationSeconds, SampleCount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(PLC_ID, TagName, PeriodStart) DO UPDATE SET
            PeriodEnd=excluded.PeriodEnd,
            FirstValue=excluded.FirstValue,
            LastValue=excluded.LastValue,
            MinValue=excluded.MinValue,
            MaxValue=excluded.MaxValue,
            WeightedAverage=excluded.WeightedAverage,
            DurationSeconds=excluded.DurationSeconds,
            SampleCount=excluded.SampleCount
        """,
        (
            aggregate["PLC_ID"], aggregate["TagName"], aggregate["PeriodStart"],
            aggregate["PeriodEnd"], aggregate["FirstValue"], aggregate["LastValue"],
            aggregate["MinValue"], aggregate["MaxValue"], aggregate["WeightedAverage"],
            aggregate["DurationSeconds"], aggregate["SampleCount"],
        ),
    )


def record_time_sample(plc_id, tag, value, timestamp, history_resolution="minute"):
    """Persist one TIME sample and refresh all minute/hour/day buckets it touches."""
    init_edge_data()
    resolution = str(history_resolution or "minute").strip().lower()
    if resolution not in _RESOLUTIONS:
        resolution = "minute"
    ts = _parse_ts(timestamp) or datetime.now()
    timestamp_text = _format_ts(ts)

    with _LOCK:
        conn = _connect()
        try:
            previous = _previous_sample(conn, plc_id, tag, timestamp_text)
            conn.execute(
                "INSERT INTO PLC_Data(PLC_ID, TagName, Value, StorageType, Timestamp) VALUES(?,?,?,?,?)",
                (int(plc_id), str(tag), float(value), "TIME", timestamp_text),
            )

            previous_ts = _parse_ts(previous["Timestamp"]) if previous else None
            for current_resolution in _RESOLUTIONS:
                start = _bucket_start(previous_ts, current_resolution) if previous_ts else _bucket_start(ts, current_resolution)
                last_bucket = _bucket_start(ts, current_resolution)
                while start <= last_bucket:
                    aggregate = _calculate_period(
                        conn, plc_id, tag, start, current_resolution, ts
                    )
                    if aggregate is not None:
                        aggregate["Resolution"] = current_resolution
                        _upsert_period(conn, aggregate)
                    start = _period_end(start, current_resolution)

            rows = []
            table = _table(resolution)
            for row in conn.execute(
                f"""
                SELECT ID, PLC_ID, TagName, PeriodStart, PeriodEnd,
                       FirstValue, LastValue, MinValue, MaxValue,
                       WeightedAverage, DurationSeconds, SampleCount
                FROM {table}
                WHERE PLC_ID=? AND LOWER(TagName)=LOWER(?)
                  AND UploadQueued=0 AND PeriodEnd <= ?
                ORDER BY PeriodStart ASC
                """,
                (int(plc_id), str(tag), timestamp_text),
            ).fetchall():
                item = dict(row)
                item["HistoryResolution"] = resolution
                rows.append(item)

            conn.commit()
            return rows
        finally:
            conn.close()


def mark_aggregates_queued(aggregates):
    if not aggregates:
        return 0
    count = 0
    with _LOCK:
        conn = _connect()
        try:
            for item in aggregates:
                if not isinstance(item, dict):
                    continue
                try:
                    aggregate_id = int(item["ID"])
                except (KeyError, TypeError, ValueError):
                    continue
                resolution = str(item.get("HistoryResolution", "")).strip().lower()
                if resolution not in _RESOLUTIONS:
                    continue
                table = _table(resolution)
                cursor = conn.execute(
                    f"UPDATE {table} SET UploadQueued=1 WHERE ID=? AND UploadQueued=0",
                    (aggregate_id,),
                )
                count += cursor.rowcount
            conn.commit()
        finally:
            conn.close()
    return count


__all__ = ["init_edge_data", "record_time_sample", "mark_aggregates_queued"]
