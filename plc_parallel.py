from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import plc as _plc


TRIGGER_SIGNAL_PREFIX = "__TRIGGER_REGISTER_"


def _read_register_block(client, start_register, count, slave):
    """Read a contiguous Flow-defined register range in one Modbus request."""
    try:
        result = client.read_holding_registers(
            address=int(start_register),
            count=int(count),
            unit=int(slave),
        )
    except TypeError:
        result = client.read_holding_registers(
            address=int(start_register),
            count=int(count),
            slave=int(slave),
        )

    if result.isError() or not getattr(result, "registers", None):
        return None

    return list(result.registers)


def _read_one_plc(plc_config, plc_mappings, now):
    """Read one PLC independently using the same Flow-driven rules as plc.py."""
    plc_id = plc_config["plc_id"]
    client = _plc.get_client(plc_config)

    if client is None:
        return []

    slave = plc_config["slave"]
    communication_timeout = plc_config.get("communication_timeout")
    data = []

    # --------------------------------------------------------
    # TRIGGER STORAGE
    # --------------------------------------------------------
    # Trigger reads are handled before LIVE/TIME reads. For each Flow-defined
    # trigger register, read that register and all dependent TRIGGER registers
    # in one contiguous Modbus request whenever the range fits in one Modbus
    # holding-register read. This removes the previous serial-read latency and
    # keeps the dependent values ordered before the synthetic signal.
    trigger_mappings = [
        mapping
        for mapping in plc_mappings
        if mapping["storage"] == "TRIGGER"
        and mapping.get("trigger_register") not in (None, "", 0)
    ]

    trigger_registers = sorted({
        int(mapping["trigger_register"])
        for mapping in trigger_mappings
    })

    for trigger_register in trigger_registers:
        dependent = [
            mapping
            for mapping in trigger_mappings
            if int(mapping["trigger_register"]) == trigger_register
        ]

        registers = [trigger_register]
        registers.extend(
            int(mapping["register"])
            for mapping in dependent
            if mapping.get("register") not in (None, "")
        )
        start_register = min(registers)
        end_register = max(registers)
        span = end_register - start_register + 1

        block_values = None
        if span <= 125:
            try:
                block_values = _read_register_block(
                    client,
                    start_register,
                    span,
                    slave,
                )
            except Exception as exc:
                print(
                    "TRIGGER BLOCK READ ERROR:",
                    "PLC_ID:", plc_id,
                    "START:", start_register,
                    "COUNT:", span,
                    exc,
                )

        if block_values is not None:
            trigger_value = block_values[trigger_register - start_register]

            for mapping in dependent:
                expected = mapping.get("trigger_value", 0)
                try:
                    condition_met = float(trigger_value) == float(expected)
                except Exception:
                    condition_met = trigger_value == expected
                if not condition_met:
                    continue

                register = int(mapping["register"])
                offset = register - start_register
                if offset < 0 or offset >= len(block_values):
                    continue
                value = block_values[offset]
                name = mapping["name"]
                try:
                    value = _plc.convert_value(value, mapping)
                except Exception as exc:
                    print("VALUE CONVERSION ERROR:", "PLC_ID:", plc_id, name, exc)
                    continue

                data.append({
                    "PLC_ID": plc_id,
                    "TagName": name,
                    "Value": value,
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip("."),
                    "StorageType": "TRIGGER",
                    "CommunicationTimeout": communication_timeout,
                })

                print(
                    "TRIGGER:",
                    "PLC_ID:", plc_id,
                    name,
                    value,
                    "REGISTER:", register,
                    "TRIGGER REGISTER:", trigger_register,
                    "TRIGGER VALUE:", expected,
                )
        else:
            # Generic fallback for unusually wide Flow-defined register ranges.
            # The trigger register is still read before the rest of the PLC scan.
            trigger_value = _plc.read_register(client, trigger_register, slave)
            if trigger_value is not None:
                for mapping in dependent:
                    expected = mapping.get("trigger_value", 0)
                    try:
                        condition_met = float(trigger_value) == float(expected)
                    except Exception:
                        condition_met = trigger_value == expected
                    if not condition_met:
                        continue

                    register = mapping["register"]
                    name = mapping["name"]
                    value = _plc.read_register(client, register, slave)
                    if value is None:
                        continue
                    try:
                        value = _plc.convert_value(value, mapping)
                    except Exception as exc:
                        print("VALUE CONVERSION ERROR:", "PLC_ID:", plc_id, name, exc)
                        continue

                    data.append({
                        "PLC_ID": plc_id,
                        "TagName": name,
                        "Value": value,
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip("."),
                        "StorageType": "TRIGGER",
                        "CommunicationTimeout": communication_timeout,
                    })

                    print(
                        "TRIGGER:",
                        "PLC_ID:", plc_id,
                        name,
                        value,
                        "REGISTER:", register,
                        "TRIGGER REGISTER:", trigger_register,
                        "TRIGGER VALUE:", expected,
                    )

        if block_values is not None or trigger_value is not None:
            data.append({
                "PLC_ID": plc_id,
                "TagName": f"{TRIGGER_SIGNAL_PREFIX}{trigger_register}",
                "Value": trigger_value,
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip("."),
                "StorageType": "TRIGGER_SIGNAL",
                "CommunicationTimeout": communication_timeout,
            })

    # --------------------------------------------------------
    # LIVE STORAGE
    # --------------------------------------------------------
    # --------------------------------------------------------
    for mapping in plc_mappings:
        if mapping["storage"] != "LIVE":
            continue

        if not _plc.tag_is_due(mapping, now):
            continue

        register = mapping["register"]
        name = mapping["name"]
        value = _plc.read_register(client, register, slave)
        _plc.schedule_next(mapping, now)
        if value is None:
            continue

        try:
            value = _plc.convert_value(value, mapping)
        except Exception as e:
            print("VALUE CONVERSION ERROR:", "PLC_ID:", plc_id, name, e)
            continue

        data.append({
            "PLC_ID": plc_id,
            "TagName": name,
            "Value": value,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip("."),
            "StorageType": "LIVE",
            "CommunicationTimeout": communication_timeout,
        })

        print("LIVE:", "PLC_ID:", plc_id, name, value, "REGISTER:", register, "INTERVAL:", mapping.get("live_interval"))

    # --------------------------------------------------------
    # TIME STORAGE
    # --------------------------------------------------------
    for mapping in plc_mappings:
        if mapping["storage"] != "TIME":
            continue

        if not _plc.tag_is_due(mapping, now):
            continue

        register = mapping["register"]
        name = mapping["name"]

        value = _plc.read_register(
            client,
            register,
            slave
        )

        _plc.schedule_next(mapping, now)

        if value is None:
            continue

        try:
            value = _plc.convert_value(value, mapping)
        except Exception as e:
            print(
                "VALUE CONVERSION ERROR:",
                "PLC_ID:", plc_id,
                name,
                e
            )
            continue

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        closed_aggregates = _plc.edge_data.record_time_sample(
            plc_id,
            name,
            value,
            timestamp,
            mapping.get("history_resolution", "minute"),
        )
        item = {
            "PLC_ID": plc_id,
            "TagName": name,
            "Value": value,
            "Timestamp": timestamp,
            "StorageType": "TIME",
            "HistoryResolution": mapping.get("history_resolution", "minute"),
            "CommunicationTimeout": communication_timeout,
        }
        if closed_aggregates:
            item["_ClosedAggregates"] = closed_aggregates
        data.append(item)

        print(
            "DUE:",
            "PLC_ID:", plc_id,
            name,
            value,
            "REGISTER:", register,
            "INTERVAL:", mapping["interval"]
        )


    return data