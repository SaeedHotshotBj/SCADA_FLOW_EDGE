import requests
from datetime import datetime

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

        for item in data:

            if not isinstance(item, dict):
                continue

            plc_id = item.get("PLC_ID")
            tag = item.get("TagName")
            value = item.get("Value")
            communication_timeout = item.get("CommunicationTimeout")

            if plc_id is None or tag is None:
                continue

            result = send_tag(
                plc_id,
                tag,
                value,
                communication_timeout=communication_timeout
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
