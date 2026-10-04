import requests

import config

from store_forward import (
    delete_acked,
    enqueue_many,
    get_batch,
    increment_retries,
    init_queue,
    pending_count,
    rows_to_payload,
)
from edge_data import mark_aggregates_queued


BATCH_SIZE = max(1, int(getattr(config, "STORE_FORWARD_BATCH_SIZE", 100)))
SEND_TIMEOUT = float(getattr(config, "STORE_FORWARD_SEND_TIMEOUT", 10.0))
TIME_RESOLUTION_RANK = {"minute": 0, "hour": 1, "day": 2}


def _send_live_batch(items):
    if not items:
        return True

    url = config.SERVER_URL.rstrip("/") + "/api/edge/live"
    payload = {"items": items}

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=SEND_TIMEOUT,
        )
        if response.status_code != 200:
            print("EDGE LIVE SEND ERROR:", response.status_code, response.text)
            return False

        result = response.json()
        if result.get("status") != "ok":
            print("EDGE LIVE SEND REJECTED:", result)
            return False

        print("EDGE LIVE SENT:", len(items), "ACCEPTED:", result.get("accepted", 0))
        return True
    except Exception as exc:
        # LIVE is intentionally not persisted. A failed live request is dropped.
        print("EDGE LIVE CONNECTION ERROR:", exc)
        return False


def _send_batch(rows):
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
            increment_retries([row["EventID"] for row in rows])
            return False

        result = response.json()
        if result.get("status") != "ok":
            print("STORE & FORWARD REJECTED:", result)
            increment_retries([row["EventID"] for row in rows])
            return False

        acks = result.get("acks", [])
        errors = result.get("errors") or []
        if not isinstance(acks, list) or not isinstance(errors, list):
            increment_retries([row["EventID"] for row in rows])
            return False

        delete_acked(acks)

        error_ids = [
            item.get("EventID")
            for item in errors
            if isinstance(item, dict) and item.get("EventID")
        ]
        if error_ids:
            increment_retries(error_ids)
            print("STORE & FORWARD ITEM ERRORS:", errors)

        print(
            "STORE & FORWARD ACK:", len(acks),
            "ERRORS:", len(errors),
            "PENDING:", pending_count(),
        )

        # Stop flushing when the server explicitly rejected items. They remain
        # queued for a later retry instead of spinning on HTTP 200 + errors.
        return not errors

    except Exception as exc:
        print("STORE & FORWARD CONNECTION ERROR:", exc)
        increment_retries([row["EventID"] for row in rows])
        return False


def flush_queue():
    """Send oldest queued records first; delete only after server ACK."""
    while True:
        rows = get_batch(BATCH_SIZE)
        if not rows:
            return True
        if not _send_batch(rows):
            return False


def _normalize_items(data):
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [
            {
                "PLC_ID": config.PLC_ID,
                "TagName": tag,
                "Value": value,
                "Timestamp": None,
                "StorageType": "LIVE",
            }
            for tag, value in data.items()
        ]
    return []


def send_all(data):
    init_queue()

    items = _normalize_items(data)
    if not items:
        flush_queue()
        return

    live_items = []
    queue_items = []
    aggregate_keys = []

    for item in items:
        plc_id = item.get("PLC_ID")
        tag = item.get("TagName")
        if plc_id is None or tag is None:
            continue

        storage = str(item.get("StorageType", "") or "").strip().upper()
        if storage == "LIVE":
            live_items.append({
                "PLC_ID": plc_id,
                "TagName": tag,
                "Value": item.get("Value"),
                "Timestamp": item.get("Timestamp"),
                "StorageType": "LIVE",
            })
            continue

        if storage == "TIME":
            # TIME samples are also displayed live, but the raw sample is never
            # placed in the network queue or persisted on the server.
            live_items.append({
                "PLC_ID": plc_id,
                "TagName": tag,
                "Value": item.get("Value"),
                "Timestamp": item.get("Timestamp"),
                "StorageType": "TIME",
            })

            resolution = str(item.get("HistoryResolution", "") or "").strip().lower()
            for aggregate in item.get("_ClosedAggregates", []) or []:
                if not isinstance(aggregate, dict):
                    continue
                aggregate_resolution = str(
                    aggregate.get("HistoryResolution", resolution)
                ).strip().lower()
                if aggregate_resolution not in TIME_RESOLUTION_RANK:
                    continue
                if (
                    resolution in TIME_RESOLUTION_RANK
                    and TIME_RESOLUTION_RANK[aggregate_resolution]
                    < TIME_RESOLUTION_RANK[resolution]
                ):
                    continue
                queue_items.append({
                    "PLC_ID": plc_id,
                    "TagName": tag,
                    "Value": aggregate.get("WeightedAverage"),
                    "Timestamp": aggregate.get("PeriodStart"),
                    "StorageType": "TIME",
                    "HistoryResolution": aggregate_resolution,
                    "PeriodEnd": aggregate.get("PeriodEnd"),
                    "FirstValue": aggregate.get("FirstValue"),
                    "LastValue": aggregate.get("LastValue"),
                    "MinValue": aggregate.get("MinValue"),
                    "MaxValue": aggregate.get("MaxValue"),
                    "WeightedAverage": aggregate.get("WeightedAverage"),
                    "DurationSeconds": aggregate.get("DurationSeconds"),
                    "SampleCount": aggregate.get("SampleCount"),
                })
                aggregate_keys.append(aggregate)
            continue

        if storage in {"TRIGGER", "TRIGGER_SIGNAL"}:
            # Keep dependent TRIGGER samples and their synthetic signal in
            # exactly the order produced by plc_parallel.read_all().
            queue_items.append({
                "PLC_ID": plc_id,
                "TagName": tag,
                "Value": item.get("Value"),
                "Timestamp": item.get("Timestamp"),
                "StorageType": storage,
                "CommunicationTimeout": item.get("CommunicationTimeout"),
            })

    # LIVE/TIME values intentionally bypass Store & Forward.
    _send_live_batch(live_items)

    if queue_items:
        try:
            added = enqueue_many(queue_items)
            if added != len(queue_items):
                print("STORE & FORWARD QUEUE COUNT MISMATCH:", added, len(queue_items))
            else:
                # Mark local aggregate rows only after they have become durable
                # in Store & Forward. A retry therefore cannot lose a period.
                mark_aggregates_queued(aggregate_keys)
            print("STORE & FORWARD QUEUED:", added, "PENDING:", pending_count())
        except Exception as exc:
            print("STORE & FORWARD LOCAL QUEUE ERROR:", exc)
            return

    flush_queue()
