import time

import plc


# Pulse state is keyed by Drawflow node id so multiple Pulse nodes can
# operate independently, including multiple pulses targeting the same PLC.
_states = {}


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


def _parse_positive_number(value, name, node_id):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name} in Pulse node {node_id}")

    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero in Pulse node {node_id}")

    return parsed


def _extract_pulses(flow):
    try:
        nodes = flow["drawflow"]["Home"]["data"]
    except Exception:
        return []

    pulses = []

    for node_id, node in nodes.items():
        if not isinstance(node, dict):
            continue

        node_name = node.get("class") or node.get("name")
        if str(node_name).strip() != "Pulse":
            continue

        data = node.get("data", {})
        if not isinstance(data, dict):
            data = {}

        try:
            plc_id = int(data.get("plc_id"))
            register = int(data.get("register"))
            bandwidth = int(data.get("bandwidth", 1))
            pulse_width_ms = _parse_positive_number(
                data.get("pulse_width"),
                "pulse_width",
                node_id,
            )
            interval_ms = _parse_positive_number(
                data.get("interval"),
                "interval",
                node_id,
            )
        except (TypeError, ValueError) as exc:
            print("PULSE CONFIG ERROR:", exc)
            continue

        if plc_id <= 0:
            print("PULSE CONFIG ERROR: invalid PLC_ID", node_id, plc_id)
            continue

        if register < 0 or register > 65535:
            print("PULSE CONFIG ERROR: invalid register", node_id, register)
            continue

        if bandwidth < 0 or bandwidth > 65535:
            print(
                "PULSE CONFIG ERROR: bandwidth must be between 0 and 65535",
                node_id,
            )
            continue

        if interval_ms <= pulse_width_ms:
            print(
                "PULSE CONFIG ERROR:",
                "Interval must be greater than Pulse Width",
                "NODE:", node_id,
            )
            continue

        pulses.append({
            "node_id": str(node_id),
            "plc_id": plc_id,
            "register": register,
            "bandwidth": bandwidth,
            "pulse_width": pulse_width_ms / 1000.0,
            "interval": interval_ms / 1000.0,
        })

    return pulses


def _get_plc_configs(flow):
    try:
        nodes = flow["drawflow"]["Home"]["data"]
    except Exception:
        return {}

    try:
        return plc._extract_plc_configs(nodes)
    except Exception as exc:
        print("PULSE PLC CONFIG ERROR:", exc)
        return {}


def _state_signature(pulse):
    return (
        pulse["plc_id"],
        pulse["register"],
        pulse["bandwidth"],
        pulse["pulse_width"],
        pulse["interval"],
    )


def _force_off(state, plc_configs):
    if not state.get("is_on"):
        return

    plc_config = plc_configs.get(int(state["plc_id"]))
    if plc_config is None:
        return

    client = plc.get_client(plc_config)
    if client is None:
        return

    try:
        _write_register(
            client,
            state["register"],
            0,
            plc_config["slave"],
        )
        state["is_on"] = False
        print(
            "PULSE FORCED OFF:",
            "NODE:", state["node_id"],
            "PLC_ID:", state["plc_id"],
            "REGISTER:", state["register"],
        )
    except Exception as exc:
        print(
            "PULSE FORCE OFF ERROR:",
            "NODE:", state["node_id"],
            exc,
        )


def process_pulses():
    """Run all Pulse nodes defined by the current Flow configuration.

    Interval is the time from the start of one pulse to the start of the
    next pulse. Pulse Width must therefore be smaller than Interval.
    The ON value is the configured Bandwidth value; the OFF value is zero.
    """
    flow = plc.get_flow_config()
    if not flow:
        return

    pulses = _extract_pulses(flow)
    plc_configs = _get_plc_configs(flow)
    now = time.monotonic()

    active_ids = {pulse["node_id"] for pulse in pulses}

    # A removed or invalid Pulse node must not leave its PLC register ON.
    for node_id in list(_states):
        if node_id in active_ids:
            continue
        state = _states[node_id]
        _force_off(state, plc_configs)
        _states.pop(node_id, None)

    for pulse in pulses:
        node_id = pulse["node_id"]
        plc_config = plc_configs.get(pulse["plc_id"])

        if plc_config is None:
            print(
                "PULSE PLC NOT FOUND:",
                "NODE:", node_id,
                "PLC_ID:", pulse["plc_id"],
            )
            continue

        signature = _state_signature(pulse)
        state = _states.get(node_id)

        if state is None or state.get("signature") != signature:
            if state is not None:
                _force_off(state, plc_configs)

            state = {
                "node_id": node_id,
                "plc_id": pulse["plc_id"],
                "register": pulse["register"],
                "is_on": False,
                "off_at": 0.0,
                "next_start": now,
                "signature": signature,
            }
            _states[node_id] = state
            print(
                "PULSE CONFIG LOADED:",
                "NODE:", node_id,
                "PLC_ID:", pulse["plc_id"],
                "REGISTER:", pulse["register"],
                "BANDWIDTH:", pulse["bandwidth"],
                "PULSE_WIDTH_MS:", int(pulse["pulse_width"] * 1000),
                "INTERVAL_MS:", int(pulse["interval"] * 1000),
            )

        if state["is_on"] and now >= state["off_at"]:
            client = plc.get_client(plc_config)
            if client is not None:
                try:
                    _write_register(
                        client,
                        pulse["register"],
                        0,
                        plc_config["slave"],
                    )
                    state["is_on"] = False
                    print(
                        "PULSE OFF:",
                        "NODE:", node_id,
                        "PLC_ID:", pulse["plc_id"],
                        "REGISTER:", pulse["register"],
                    )
                except Exception as exc:
                    print(
                        "PULSE OFF ERROR:",
                        "NODE:", node_id,
                        exc,
                    )
                    continue

        if not state["is_on"] and now >= state["next_start"]:
            client = plc.get_client(plc_config)
            if client is None:
                continue

            try:
                _write_register(
                    client,
                    pulse["register"],
                    pulse["bandwidth"],
                    plc_config["slave"],
                )
                state["is_on"] = True
                state["off_at"] = now + pulse["pulse_width"]
                state["next_start"] = now + pulse["interval"]
                print(
                    "PULSE ON:",
                    "NODE:", node_id,
                    "PLC_ID:", pulse["plc_id"],
                    "REGISTER:", pulse["register"],
                    "VALUE:", pulse["bandwidth"],
                )
            except Exception as exc:
                print(
                    "PULSE ON ERROR:",
                    "NODE:", node_id,
                    exc,
                )
