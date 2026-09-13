import requests
from datetime import datetime

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


# =====================================================
# STORE & FORWARD CONFIG
# =====================================================

BATCH_SIZE = max(1, int(getattr(config, "STORE_FORWARD_BATCH_SIZE", 100)))
SEND_TIMEOUT = float(getattr(config, "STORE_FORWARD_SEND_TIMEOUT", 10.0))


# =====================================================
# SERVER SEND
# =====================================================

def _send_batch(rows):
    if not rows:
        return False

    url = (
        config.SERVER_URL.rstrip("/")
        + "/api/store_forward"
    )

    payload = {
        "items": rows_to_payload(rows)
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=SEND_TIMEOUT,
        )

        if response.status_code != 200:
            print(
                "STORE & FORWARD SERVER ERROR:",
                response.status_code,
                response.text
            )
            return False

        result = response.json()
        if result.get("status") != "ok":
            print(
                "STORE & FORWARD REJECTED:",
                result
            )
            return False

        acks = result.get("acks", [])
        if not isinstance(acks, list):
            return False

        delete_acked(acks)

        errors = result.get("errors") or []
        if errors:
            # Keep failed events queued, but stop this flush cycle.
            # flush_queue() increments retries once for the events that
            # remain in the local queue instead of hot-looping the same
            # application-level failures.
            return False

        print(
            "STORE & FORWARD ACK:",
            len(acks),
            "PENDING:",
            pending_count()
        )

        return True

    except Exception as exc:
        print(
            "STORE & FORWARD CONNECTION ERROR:",
            exc
        )
        return False


# =====================================================
# FLUSH LOCAL QUEUE
# =====================================================

def flush_queue():
    """Send oldest queued records first; delete only after server ACK."""
    while True:
        rows = get_batch(BATCH_SIZE)
        if not rows:
            return True

        if not _send_batch(rows):
            increment_retries([
                row["EventID"]
                for row in rows
            ])
            return False


# =====================================================
# SEND ALL DUE TAGS
# =====================================================

def send_all(data):
    init_queue()

    if not data:
        flush_queue()
        return

    items = []

    # New multi-PLC format from plc_parallel.read_all().
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue

            plc_id = item.get("PLC_ID")
            tag = item.get("TagName")
            if plc_id is None or tag is None:
                continue

            items.append({
                "PLC_ID": plc_id,
                "TagName": tag,
                "Value": item.get("Value"),
                "Timestamp": datetime.now().isoformat(),
                "CommunicationTimeout": item.get("CommunicationTimeout"),
            })

    # Backward compatibility with the old dictionary format.
    elif isinstance(data, dict):
        for tag, value in data.items():
            items.append({
                "PLC_ID": config.PLC_ID,
                "TagName": tag,
                "Value": value,
                "Timestamp": datetime.now().isoformat(),
            })

    if items:
        try:
            added = enqueue_many(items)
            print(
                "STORE & FORWARD QUEUED:",
                added,
                "PENDING:",
                pending_count()
            )
        except Exception as exc:
            print(
                "STORE & FORWARD LOCAL QUEUE ERROR:",
                exc
            )
            return

    # This also attempts delivery when the server was previously offline.
    flush_queue()
