import time

import requests

import config
import plc


POLL_TIMEOUT = 5
POLL_INTERVAL = 0.5
RESULT_TIMEOUT = 5
_next_poll_time = 0.0


def _write_register(client, address, value, slave):
    try:
        result = client.write_register(
            address=int(address),
            value=int(value),
            unit=int(slave),
        )
    except TypeError:
        result = client.write_register(
            int(address),
            int(value),
            slave=int(slave),
        )

    if result is None or result.isError():
        raise RuntimeError("PLC rejected the Modbus write")


def _report_result(command_id, plc_id, success, error_message=None):
    url = config.SERVER_URL.rstrip("/") + "/api/edge/write_result"

    payload = {
        "CommandID": int(command_id),
        "PLC_ID": int(plc_id),
        "Success": bool(success),
    }

    if error_message:
        payload["ErrorMessage"] = str(error_message)[:1000]

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=RESULT_TIMEOUT,
        )
        if response.status_code != 200:
            print(
                "WRITE RESULT SERVER ERROR:",
                response.status_code,
                response.text,
            )
    except Exception as exc:
        print("WRITE RESULT CONNECTION ERROR:", exc)


def process_one_write_command():
    """Fetch at most one pending Master write and execute it on this Edge PLC."""
    global _next_poll_time

    now = time.monotonic()
    if now < _next_poll_time:
        return
    _next_poll_time = now + POLL_INTERVAL

    url = config.SERVER_URL.rstrip("/") + "/api/edge/write_command"

    try:
        response = requests.get(
            url,
            params={"PLC_ID": int(config.PLC_ID)},
            timeout=POLL_TIMEOUT,
        )

        if response.status_code != 200:
            print(
                "WRITE COMMAND SERVER ERROR:",
                response.status_code,
                response.text,
            )
            return

        payload = response.json()
    except Exception as exc:
        print("WRITE COMMAND POLL ERROR:", exc)
        return

    if payload.get("status") != "ok":
        return

    command = payload.get("command") or {}

    try:
        command_id = int(command["CommandID"])
        plc_id = int(command["PLC_ID"])
        register = int(command["Register"])
        value = int(command["Value"])
    except (KeyError, TypeError, ValueError) as exc:
        print("INVALID WRITE COMMAND:", exc, command)
        return

    if not 0 <= register <= 65535 or not 0 <= value <= 65535:
        _report_result(
            command_id,
            plc_id,
            False,
            "Register and value must be between 0 and 65535",
        )
        return

    try:
        plc_configs, _ = plc.get_runtime_configuration()
        plc_config = next(
            (
                item
                for item in plc_configs
                if int(item["plc_id"]) == plc_id
            ),
            None,
        )

        if plc_config is None:
            raise RuntimeError("PLC is not present in the current Flow configuration")

        client = plc.get_client(plc_config)
        if client is None:
            raise RuntimeError("PLC connection failed")

        _write_register(
            client,
            register,
            value,
            plc_config["slave"],
        )

        print(
            "PLC WRITE SUCCESS:",
            "CommandID:", command_id,
            "PLC_ID:", plc_id,
            "REGISTER:", register,
            "VALUE:", value,
        )
        _report_result(command_id, plc_id, True)

    except Exception as exc:
        print(
            "PLC WRITE FAILED:",
            "CommandID:", command_id,
            "PLC_ID:", plc_id,
            exc,
        )
        _report_result(command_id, plc_id, False, str(exc))
