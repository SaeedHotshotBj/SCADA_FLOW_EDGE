import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import config


# =====================================================
# SEND ONE TAG
# =====================================================

def send_tag(plc_id, tag, value, communication_timeout=None):

    payload = {
        "PLC_ID": plc_id,
        "TagName": tag,
        "Value": value,
        "Timestamp": datetime.now().isoformat()
    }

    url = (
        config.SERVER_URL.rstrip("/")
        + "/api/data"
    )

    try:

        request_kwargs = {
            "json": payload
        }

        # The timeout is part of the PLCReader Flow configuration.
        # When it is blank in Flow, no client-side timeout is imposed here.
        if communication_timeout not in (None, ""):
            request_kwargs["timeout"] = float(communication_timeout)

        response = requests.post(
            url,
            **request_kwargs
        )

        if response.status_code != 200:

            print(
                "SERVER ERROR:",
                response.status_code,
                response.text
            )

        return response.status_code

    except Exception as e:

        print(
            "SERVER CONNECTION ERROR:",
            e
        )

        return None


# =====================================================
# SEND ALL DUE TAGS
# =====================================================

def send_all(data):

    if not data:
        return

    # New multi-PLC format from plc.read_all():
    # [
    #   {
    #       "PLC_ID": 1,
    #       "TagName": "voltage",
    #       "Value": 220,
    #       "CommunicationTimeout": 10
    #   }
    # ]
    if isinstance(data, list):

        items = []
        for item in data:
            if not isinstance(item, dict):
                continue

            plc_id = item.get("PLC_ID")
            tag = item.get("TagName")
            value = item.get("Value")
            communication_timeout = item.get("CommunicationTimeout")

            if plc_id is None or tag is None:
                continue

            items.append((
                plc_id,
                tag,
                value,
                communication_timeout,
            ))

        if not items:
            return

        # Each HTTP POST is independent. Sending them concurrently prevents
        # one PLC or one slow request from delaying every other PLC.
        max_workers = max(1, min(len(items), 32))

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="SCADA_EDGE_SEND"
        ) as executor:
            futures = {
                executor.submit(
                    send_tag,
                    plc_id,
                    tag,
                    value,
                    communication_timeout,
                ): (plc_id, tag, value)
                for plc_id, tag, value, communication_timeout in items
            }

            for future in as_completed(futures):
                plc_id, tag, value = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = None
                    print(
                        "SEND TASK ERROR:",
                        "PLC_ID:", plc_id,
                        tag,
                        exc
                    )

                print(
                    "SENT:",
                    "PLC_ID:", plc_id,
                    tag,
                    value,
                    result
                )

        return

    # -----------------------------------------------------
    # Backward compatibility with the old dictionary format.
    # -----------------------------------------------------
    if isinstance(data, dict):

        for tag, value in data.items():

            result = send_tag(
                config.PLC_ID,
                tag,
                value
            )

            print(
                "SENT:",
                "PLC_ID:", config.PLC_ID,
                tag,
                value,
                result
            )
