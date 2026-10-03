import hashlib
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import config

from edge_calculated_db import (
    aggregate_storage_type,
    mark_queued,
    pending_aggregates,
    record_live_values,
    record_samples,
    rollup_completed,
    cleanup,
)
from store_forward import (
    delete_acked,
    enqueue_many,
    get_batch,
    increment_retries,
    init_queue,
    pending_count,
    rows_to_payload,
)

from timezone_utils import TZ
BATCH_SIZE = max(1, int(getattr(config, "STORE_FORWARD_BATCH_SIZE", 100)))
SEND_TIMEOUT = float(getattr(config, "STORE_FORWARD_SEND_TIMEOUT", 10.0))
RETRY_BACKOFF_SECONDS = max(1.0, float(getattr(config, "STORE_FORWARD_RETRY_BACKOFF_SECONDS", 5.0)))
_next_flush_attempt = 0.0


def _timestamp():
    return datetime.now(TZ).replace(tzinfo=None).isoformat()


def _send_live(items):
    if not items:
        return True

    url = config.SERVER_URL.rstrip("/") + "/api/edge/live"
    try:
        response = requests.post(
            url,
            json={"items": items},
            timeout=SEND_TIMEOUT,
        )
        if response.status_code != 200:
            print("EDGE LIVE SERVER ERROR:", response.status_code, response.text)
            return False

        result = response.json()
        if result.get("status") != "ok":
            print("EDGE LIVE REJECTED:", result)
            return False

        return True
    except Exception as exc:
        print("EDGE LIVE CONNECTION ERROR:", exc)
        return False


def _aggregate_event_id(item):
    raw = "|".join(
        [
            str(item.get("PLC_ID", "")),
            str(item.get("TagName", "")),
            str(item.get("Resolution", "")),
            str(item.get("PeriodStart", "")),
        ]
    )
    return "calc-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _queue_calculated_aggregates():
    pending = pending_aggregates(BATCH_SIZE)
    if not pending:
        return 0

    queue_items = []
    ids = []
    for item in pending:
        resolution = str(item.get("Resolution", "")).lower()
        storage_type = aggregate_storage_type(resolution)
        if storage_type is None:
            continue

        queue_items.append(
            {
                "EventID": _aggregate_event_id(item),
                "PLC_ID": item["PLC_ID"],
                "TagName": item["TagName"],
                "Value": item["AverageValue"],
                "Timestamp": item["PeriodStart"],
                "StorageType": storage_type,
                "HistoryResolution": resolution,
                "PeriodEnd": item.get("PeriodEnd"),
                "AverageValue": item.get("AverageValue"),
                "MinValue": item.get("MinValue"),
                "MaxValue": item.get("MaxValue"),
                "SampleCount": item.get("SampleCount"),
            }
        )
        ids.append(item["ID"])

    if not queue_items:
        return 0

    added = enqueue_many(queue_items)
    mark_queued(ids)
    return added


def flush_queue():
    """Send historical aggregates/events oldest-first; delete only after ACK."""
    global _next_flush_attempt

    now = time.monotonic()
    if now < _next_flush_attempt:
        return False

    while True:
        rows = get_batch(BATCH_SIZE)
        if not rows:
            _next_flush_attempt = 0.0
            return True

        if not _send_historical_batch(rows):
            increment_retries([row["EventID"] for row in rows])
            _next_flush_attempt = time.monotonic() + RETRY_BACKOFF_SECONDS
            return False

        _next_flush_attempt = 0.0


def _send_historical_batch(rows):
    if not rows:
        return True

    url = config.SERVER_URL.rstrip("/") + "/api/store_forward"
    payload = {"items": rows_to_payload(rows)}

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=SEND_TIMEOUT,
        )
        if response.status_code != 200:
            print("STORE & FORWARD SERVER ERROR:", response.status_code, response.text)
            return False

        result = response.json()
        if result.get("status") != "ok":
            print("STORE & FORWARD REJECTED:", result)
            return False

        acks = result.get("acks", [])
        if not isinstance(acks, list):
            print("STORE & FORWARD INVALID ACK LIST:", result)
            return False

        ack_ids = {str(item).strip() for item in acks if str(item).strip()}
        all_ids = {str(row.get("EventID", "")).strip() for row in rows}

        delete_acked(ack_ids)

        errors = result.get("errors") or []
        error_ids = set()
        if isinstance(errors, list):
            error_ids = {
                str(item.get("EventID")).strip()
                for item in errors
                if isinstance(item, dict) and str(item.get("EventID")).strip()
            }
            if errors:
                print(
                    "STORE & FORWARD SERVER REJECTIONS:",
                    errors,
                )

        unresolved_ids = all_ids - ack_ids
        if unresolved_ids:
            unexpected = unresolved_ids - error_ids
            if unexpected:
                print(
                    "STORE & FORWARD MISSING ACKS:",
                    sorted(unexpected),
                )
            return False

        print(
            "STORE & FORWARD ACK:",
            len(ack_ids),
            "PENDING:",
            pending_count(),
        )
        return True

    except Exception as exc:
        print("STORE & FORWARD CONNECTION ERROR:", exc)
        return False


def send_all(data, calculated=None):
    init_queue()

    live_items = []
    historical_items = []

    for item in data or []:
        if not isinstance(item, dict):
            continue
        plc_id = item.get("PLC_ID")
        tag = item.get("TagName")
        if plc_id is None or tag is None:
            continue

        storage = str(item.get("StorageType", "LIVE")).strip().upper()
        outgoing = {
            "PLC_ID": plc_id,
            "TagName": tag,
            "Value": item.get("Value"),
            "Timestamp": item.get("Timestamp") or _timestamp(),
            "StorageType": storage,
            "HistoryResolution": item.get(
                "HistoryResolution",
                item.get("history_resolution", "ALL"),
            ),
            "CommunicationTimeout": item.get("CommunicationTimeout"),
        }

        if storage in {"LIVE", "TIME"}:
            live_items.append(outgoing)
        else:
            historical_items.append(outgoing)

    calculated = calculated or []

    # Raw TIME/LIVE values are aggregated locally in memory. Their samples
    # never enter the local or server historian.
    if live_items:
        try:
            record_live_values(live_items)
        except Exception as exc:
            print("EDGE LIVE AGGREGATION ERROR:", exc)

    if calculated:
        try:
            record_samples(calculated)
            rollup_completed()
        except Exception as exc:
            print("EDGE CALCULATED DB ERROR:", exc)

        for item in calculated:
            if not isinstance(item, dict):
                continue
            if item.get("PLC_ID") is None or item.get("TagName") is None:
                continue
            live_items.append(
                {
                    "PLC_ID": item["PLC_ID"],
                    "TagName": item["TagName"],
                    "Value": item.get("Value"),
                    "Timestamp": item.get("Timestamp") or _timestamp(),
                    "StorageType": "CALCULATED",
                    "CommunicationTimeout": item.get("CommunicationTimeout"),
                }
            )

    if live_items or calculated:
        try:
            cleanup()
        except Exception as exc:
            print("EDGE CALCULATED DB CLEANUP ERROR:", exc)

    if live_items:
        _send_live(live_items)

    if historical_items:
        try:
            added = enqueue_many(historical_items)
            print("STORE & FORWARD QUEUED:", added, "PENDING:", pending_count())
        except Exception as exc:
            print("STORE & FORWARD LOCAL QUEUE ERROR:", exc)

    try:
        added = _queue_calculated_aggregates()
        if added:
            print("CALCULATED AGGREGATES QUEUED:", added)
    except Exception as exc:
        print("CALCULATED AGGREGATE QUEUE ERROR:", exc)

    flush_queue()


__all__ = ["send_all", "flush_queue"]
