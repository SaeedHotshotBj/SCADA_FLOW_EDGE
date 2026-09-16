from concurrent.futures import ThreadPoolExecutor, as_completed

import plc as _plc


TRIGGER_SIGNAL_PREFIX = "__TRIGGER_REGISTER_"


def _read_one_plc(plc_config, plc_mappings, now):
    """Read one PLC independently using Flow-defined rules."""
    plc_id = plc_config["plc_id"]
    client = _plc.get_client(plc_config)

    if client is None:
        return []

    slave = plc_config["slave"]
    communication_timeout = plc_config.get("communication_timeout")
    data = []

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
        value = _plc.read_register(client, register, slave)
        _plc.schedule_next(mapping, now)

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
            "CommunicationTimeout": communication_timeout,
        })

        print(
            "DUE:", "PLC_ID:", plc_id, name, value,
            "REGISTER:", register, "INTERVAL:", mapping["interval"]
        )

    # --------------------------------------------------------
    # TRIGGER STORAGE
    #
    # The trigger signal itself is persisted on every scan.  This is
    # intentional: the server needs the complete 0/1 transition sequence
    # to implement reliable Rise/Fall state tracking, even after an
    # internet outage when Store & Forward replays the samples in order.
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
        trigger_value = _plc.read_register(client, trigger_register, slave)
        if trigger_value is None:
            continue

        data.append({
            "PLC_ID": plc_id,
            "TagName": f"{TRIGGER_SIGNAL_PREFIX}{trigger_register}",
            "Value": trigger_value,
            "CommunicationTimeout": communication_timeout,
        })

        dependent = [
            mapping
            for mapping in trigger_mappings
            if int(mapping["trigger_register"]) == trigger_register
        ]

        # Dependent TRIGGER tags are sampled while the configured trigger is
        # active.  They therefore form the historian trace for the whole
        # production cycle rather than a single trigger snapshot.
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
                "CommunicationTimeout": communication_timeout,
            })

            print(
                "TRIGGER TRACE:", "PLC_ID:", plc_id, name, value,
                "REGISTER:", register,
                "TRIGGER REGISTER:", trigger_register,
                "TRIGGER VALUE:", expected,
            )

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
            mappings_by_plc.get(plc_config["plc_id"], []),
        )
        for plc_config in plc_configs
    ]
    tasks = [item for item in tasks if item[1]]
    if not tasks:
        return []

    results = []
    max_workers = max(1, len(tasks))
    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="SCADA_EDGE_PLC",
    ) as executor:
        futures = {
            executor.submit(_read_one_plc, plc_config, plc_mappings, now): plc_config["plc_id"]
            for plc_config, plc_mappings in tasks
        }
        for future in as_completed(futures):
            plc_id = futures[future]
            try:
                results.extend(future.result())
            except Exception as exc:
                print("PLC READ TASK ERROR:", "PLC_ID:", plc_id, exc)

    return results
