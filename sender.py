
import requests
from datetime import datetime

import config


# =====================================================
# SEND ONE TAG
# =====================================================

def send_tag(tag, value):

    payload = {
        "PLC_ID": config.PLC_ID,
        "TagName": tag,
        "Value": value,
        "Timestamp": datetime.now().isoformat()
    }

    url = (
        config.SERVER_URL.rstrip("/")
        + "/api/data"
    )

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=5
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

    for tag, value in data.items():

        result = send_tag(
            tag,
            value
        )

        print(
            "SENT:",
            tag,
            value,
            result
        )

