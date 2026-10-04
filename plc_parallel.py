from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import plc as _plc


TRIGGER_SIGNAL_PREFIX = "__TRIGGER_REGISTER_"


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
    # LIVE STORAGE
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

    # --------------------------------------------------------
    # TRIGGER STORAGE
    # --------------------------------------------------------
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
        trigger_value = _plc.read_register(
            client,
            trigger_register,
            slave
        )

        if trigger_value is None:
            continue

        dependent = [
            mapping
            for mapping in trigger_mappings
            if int(mapping["trigger_register"]) == trigger_register
        ]

        # Dependent TRIGGER samples must be queued before the synthetic
        # trigger signal. The server uses their Store & Forward order to
        # reconstruct the exact snapshot belonging to this signal.
        for mapping in dependent:
            expected = mapping.get("trigger_value", 0)

            try:
                condition_met = (
                    float(trigger_value) == float(expected)
                )
            except Exception:
                condition_met = trigger_value == expected

            if not condition_met:
                continue

            register = mapping["register"]
            name = mapping["name"]

            value = _plc.read_register(
                client,
                register,
                slave
            )

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
                "TRIGGER VALUE:", expected
            )

        # Emit the trigger-register sample on every scan, including both
        # inactive and active states. This is the signal consumed by the
        # server-side Rise/Fall state machine. A repeated 0->1 transition
        # therefore produces a new independent production event.
        data.append({
            "PLC_ID": plc_id,
            "TagName": f"{TRIGGER_SIGNAL_PREFIX}{trigger_register}",
            "Value": trigger_value,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip("."),
            "StorageType": "TRIGGER_SIGNAL",
            "CommunicationTimeout": communication_timeout,
        })

    return data


def read_all():
    """Read every configured PLC independently and concurrently.

    Flow remains the sole source for PLC identity, register mappings,
    intervals, storage type and communication timeout. This function only
    changes execution scheduling: one slow PLC cannot block another PLC.
    """
    plc_configs, mappings = _plc.get_runtime_configuration()

    if not plc_configs or not mappings:
        return []

    _plc.update_scheduler(mappings)

    now = __import__("time").time()

    mappings_by_plc = {}
    for mapping in mappings:
        mappings_by_plc.setdefault(mapping["plc_id"], []).append(mapping)

    tasks = [
        (
            plc_config,
            mappings_by_plc.get(plc_config["plc_id"], [])
        )
        for plc_config in plc_configs
    ]
    tasks = [item for item in tasks if item[1]]

    if not tasks:
        return []

    results = []

    # One worker per PLC. PLCs are therefore read in parallel, while the
    # per-PLC tag order and all Flow-defined scheduling rules remain unchanged.
    max_workers = max(1, len(tasks))
    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="SCADA_EDGE_PLC"
    ) as executor:
        futures = {
            executor.submit(_read_one_plc, plc_config, plc_mappings, now):
            plc_config["plc_id"]
            for plc_config, plc_mappings in tasks
        }

        for future in as_completed(futures):
            plc_id = futures[future]
            try:
                results.extend(future.result())
            except Exception as exc:
                print(
                    "PLC READ TASK ERROR:",
                    "PLC_ID:", plc_id,
                    exc
                )

    return results
