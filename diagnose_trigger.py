"""Focused diagnostic for Trigger storage mappings.
Does not modify runtime logic or queued data.
"""

from plc import get_runtime_configuration, get_client, read_register


def main():
    plc_configs, mappings = get_runtime_configuration()
    trigger_mappings = [
        m for m in mappings
        if str(m.get("storage", "")).upper() == "TRIGGER"
        and m.get("trigger_register") not in (None, "", 0)
    ]

    print("=== TRIGGER DIAGNOSTIC START ===")
    print("PLC_CONFIGS:", plc_configs)
    print("TRIGGER_MAPPINGS:", trigger_mappings)

    by_plc = {}
    for mapping in trigger_mappings:
        by_plc.setdefault(mapping["plc_id"], []).append(mapping)

    for plc_id, items in by_plc.items():
        plc_config = next((p for p in plc_configs if p["plc_id"] == plc_id), None)
        if plc_config is None:
            print("PLC CONFIG NOT FOUND:", plc_id)
            continue

        client = get_client(plc_config)
        if client is None:
            print("PLC CONNECTION FAILED:", plc_id)
            continue

        registers = sorted({int(item["trigger_register"]) for item in items})
        for trigger_register in registers:
            actual = read_register(client, trigger_register, plc_config["slave"])
            expected = sorted({
                item.get("trigger_value", 0)
                for item in items
                if int(item["trigger_register"]) == trigger_register
            }, key=str)
            print(
                "TRIGGER CHECK:",
                "PLC_ID=", plc_id,
                "REGISTER=", trigger_register,
                "ACTUAL=", actual,
                "EXPECTED=", expected,
            )

        for item in items:
            print(
                "TRIGGER TAG:",
                "PLC_ID=", item["plc_id"],
                "TAG=", item["name"],
                "DATA_REGISTER=", item["register"],
                "TRIGGER_REGISTER=", item["trigger_register"],
                "TRIGGER_VALUE=", item["trigger_value"],
            )

    print("=== TRIGGER DIAGNOSTIC END ===")


if __name__ == "__main__":
    main()
