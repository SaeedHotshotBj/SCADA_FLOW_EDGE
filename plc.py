import time
import requests

from pymodbus.client.sync import ModbusTcpClient

import config


# ============================================================
# FLOW CONFIGURATION CACHE
# ============================================================

_flow_cache = None
_flow_cache_time = 0


# ============================================================
# TAG SCHEDULER
# ============================================================

_next_read_time = {}
_scheduler_signature = None


# ============================================================
# PLC CLIENTS
# One persistent Modbus client per PLC.
# ============================================================

_clients = {}
_client_configs = {}


# ============================================================
# GET FLOW CONFIGURATION FROM VPS
# ============================================================

def get_flow_config(force=False):

    global _flow_cache
    global _flow_cache_time

    now = time.time()

    if (
        not force
        and _flow_cache is not None
        and (now - _flow_cache_time) < config.FLOW_REFRESH_INTERVAL
    ):
        return _flow_cache

    url = config.SERVER_URL.rstrip("/") + "/api/edge/config"

    try:
        response = requests.get(
            url,
            params={"PLC_ID": config.PLC_ID},
            timeout=5
        )
        response.raise_for_status()

        flow = response.json()

        if not flow:
            print("EMPTY FLOW CONFIGURATION")
            return _flow_cache

        _flow_cache = flow
        _flow_cache_time = now
        print("FLOW CONFIG RECEIVED")
        return flow

    except Exception as e:
        print("FLOW CONFIG ERROR:", e)
        return _flow_cache


# ============================================================
# HELPERS
# ============================================================

def _node_connections(nodes, node_id, direction="inputs"):
    node = nodes.get(str(node_id), {})
    result = []

    section = node.get(direction, {})
    if not isinstance(section, dict):
        return result

    for item in section.values():
        if not isinstance(item, dict):
            continue
        connections = item.get("connections", [])
        if not isinstance(connections, list):
            continue
        for connection in connections:
            target = connection.get("node")
            if target is not None:
                result.append(str(target))

    return result


def _extract_plc_configs(nodes):
    """Build every PLCReader configuration.

    PLC ID is taken from PLCReader.data.plc_id when present. For legacy
    flows where it is missing, IDs are assigned deterministically in
    PLCReader order, preserving the first PLC as config.PLC_ID.
    """
    plc_configs = {}
    used_ids = set()
    fallback_id = int(getattr(config, "PLC_ID", 1))

    for node_id, node in nodes.items():
        if node.get("class") != "PLCReader":
            continue

        data = node.get("data", {})
        required = ["ip", "port", "slave", "register", "count"]
        missing = [
            key for key in required
            if data.get(key) is None or data.get(key) == ""
        ]

        if missing:
            print(
                "PLCReader CONFIGURATION INCOMPLETE:",
                "NODE:", node_id,
                "MISSING:", missing
            )
            continue

        try:
            port = int(data["port"])
            slave = int(data["slave"])
            register = int(data["register"])
            count = int(data["count"])
        except Exception as e:
            print(
                "PLCReader CONFIGURATION ERROR:",
                "NODE:", node_id,
                e
            )
            continue

        if port <= 0 or slave < 0 or register < 0 or count <= 0:
            print("INVALID PLCReader CONFIGURATION:", node_id)
            continue

        raw_plc_id = data.get("plc_id")

        try:
            if raw_plc_id is not None and str(raw_plc_id).strip() != "":
                plc_id = int(raw_plc_id)
            else:
                while fallback_id in used_ids:
                    fallback_id += 1
                plc_id = fallback_id
                fallback_id += 1
        except (TypeError, ValueError):
            while fallback_id in used_ids:
                fallback_id += 1
            plc_id = fallback_id
            fallback_id += 1

        if plc_id <= 0:
            print("INVALID PLC_ID:", node_id, plc_id)
            continue

        if plc_id in used_ids:
            print(
                "DUPLICATE PLC_ID; USING AUTOMATIC ID:",
                "NODE:", node_id,
                "REQUESTED:", plc_id
            )
            while fallback_id in used_ids:
                fallback_id += 1
            plc_id = fallback_id
            fallback_id += 1

        used_ids.add(plc_id)

        plc_configs[plc_id] = {
            "plc_id": plc_id,
            "node_id": str(node_id),
            "ip": str(data["ip"]),
            "port": port,
            "slave": slave,
            "register": register,
            "count": count,
        }

    return plc_configs


def _find_upstream_plc_ids(nodes, tagmapper_id, plc_node_to_id):
    """Return PLC IDs connected directly to a TagMapper.

    This follows the actual Drawflow graph and therefore also works when
    a TagMapper is connected to more than one PLCReader.
    """
    result = []

    for source_node_id, plc_id in plc_node_to_id.items():
        if str(tagmapper_id) in _node_connections(
            nodes,
            source_node_id,
            direction="outputs"
        ):
            result.append(plc_id)

    return result


# ============================================================
# EXTRACT RUNTIME CONFIGURATION
# ============================================================

def get_runtime_configuration():
    flow = get_flow_config()

    if not flow:
        return [], []

    try:
        nodes = flow["drawflow"]["Home"]["data"]
    except Exception as e:
        print("FLOW FORMAT ERROR:", e)
        return [], []

    plc_configs = _extract_plc_configs(nodes)

    if not plc_configs:
        print("NO PLCReader NODE FOUND")
        return [], []

    plc_node_to_id = {
        config_item["node_id"]: plc_id
        for plc_id, config_item in plc_configs.items()
    }

    # --------------------------------------------------------
    # TAG MAPPERS
    # --------------------------------------------------------

    mappings = []

    for node_id, node in nodes.items():
        if node.get("class") != "TagMapper":
            continue

        data = node.get("data", {})
        node_mappings = data.get("mappings", [])
        if not isinstance(node_mappings, list):
            continue

        upstream_plc_ids = _find_upstream_plc_ids(
            nodes,
            node_id,
            plc_node_to_id
        )

        for raw_mapping in node_mappings:
            if not isinstance(raw_mapping, dict):
                continue

            name = raw_mapping.get("name")
            register_value = raw_mapping.get("register")

            if name is None or str(name).strip() == "":
                continue
            if register_value is None:
                continue

            try:
                register = int(register_value)
            except Exception:
                print("INVALID TAG REGISTER:", raw_mapping)
                continue

            if register < 0:
                print("NEGATIVE TAG REGISTER:", register)
                continue

            try:
                scale = float(raw_mapping.get("scale", 1))
            except Exception:
                scale = 1.0

            try:
                interval = float(raw_mapping.get("interval", 1))
            except Exception:
                interval = 1.0
            if interval <= 0:
                interval = 1.0

            datatype = str(raw_mapping.get("datatype", "INT")).upper()
            storage = str(raw_mapping.get("storage", "TIME")).upper()

            trigger_register = raw_mapping.get("trigger_register", 0)
            trigger_value = raw_mapping.get("trigger_value", 0)

            try:
                if trigger_register not in (None, ""):
                    trigger_register = int(trigger_register)
                else:
                    trigger_register = 0
            except Exception:
                print("INVALID TRIGGER REGISTER:", raw_mapping)
                trigger_register = 0

            try:
                if trigger_value not in (None, ""):
                    trigger_value = float(trigger_value)
                else:
                    trigger_value = 0
            except Exception:
                print("INVALID TRIGGER VALUE:", raw_mapping)
                trigger_value = 0

            explicit_plc_id = raw_mapping.get("plc_id")
            mapping_plc_ids = []

            if explicit_plc_id not in (None, ""):
                try:
                    mapping_plc_ids = [int(explicit_plc_id)]
                except (TypeError, ValueError):
                    print(
                        "INVALID TAG PLC_ID:",
                        raw_mapping
                    )
                    continue
            elif upstream_plc_ids:
                mapping_plc_ids = list(dict.fromkeys(upstream_plc_ids))
            elif len(plc_configs) == 1:
                mapping_plc_ids = [next(iter(plc_configs))]
            else:
                print(
                    "TAG PLC_ID AMBIGUOUS; SKIPPING TAG:",
                    name,
                    "NODE:", node_id
                )
                continue

            for plc_id in mapping_plc_ids:
                if plc_id not in plc_configs:
                    print(
                        "TAG REFERENCES UNKNOWN PLC_ID:",
                        plc_id,
                        "TAG:", name
                    )
                    continue

                mappings.append({
                    "plc_id": plc_id,
                    "register": register,
                    "name": str(name),
                    "datatype": datatype,
                    "scale": scale,
                    "storage": storage,
                    "interval": interval,
                    "trigger_register": trigger_register,
                    "trigger_value": trigger_value,
                })

    if not mappings:
        print("NO TAG MAPPINGS FOUND")

    return list(plc_configs.values()), mappings


# ============================================================
# PLC CLIENT MANAGEMENT
# ============================================================

def get_client(plc_config):
    plc_id = int(plc_config["plc_id"])
    current_config = (
        plc_config["ip"],
        plc_config["port"],
        plc_config["slave"]
    )

    old_config = _client_configs.get(plc_id)

    if old_config != current_config:
        old_client = _clients.get(plc_id)
        if old_client is not None:
            try:
                old_client.close()
            except Exception:
                pass

        _clients.pop(plc_id, None)
        _client_configs[plc_id] = current_config
        print(
            "PLC CLIENT CONFIGURATION UPDATED:",
            "PLC_ID:", plc_id,
            current_config
        )

    client = _clients.get(plc_id)

    if client is None:
        client = ModbusTcpClient(
            plc_config["ip"],
            port=plc_config["port"],
            timeout=3
        )
        _clients[plc_id] = client

    try:
        if not client.is_socket_open():
            if not client.connect():
                print(
                    "PLC CONNECTION FAILED:",
                    "PLC_ID:", plc_id,
                    plc_config["ip"],
                    plc_config["port"]
                )
                return None
    except Exception as e:
        print(
            "PLC CONNECTION ERROR:",
            "PLC_ID:", plc_id,
            e
        )
        return None

    return client


# ============================================================
# READ ONE PLC REGISTER
# ============================================================

def read_register(client, address, slave):
    try:
        result = client.read_holding_registers(
            address=int(address),
            count=1,
            unit=int(slave)
        )

        if result.isError() or not getattr(result, "registers", None):
            return None

        return result.registers[0]

    except TypeError:
        try:
            result = client.read_holding_registers(
                address=int(address),
                count=1,
                slave=int(slave)
            )
            if result.isError() or not getattr(result, "registers", None):
                return None
            return result.registers[0]
        except Exception as e:
            print("PLC READ ERROR:", e)
            return None

    except Exception as e:
        print("PLC READ ERROR:", e)
        return None


# ============================================================
# CREATE SCHEDULER SIGNATURE
# ============================================================

def update_scheduler(mappings):
    global _scheduler_signature
    global _next_read_time

    signature = tuple(
        (
            mapping["plc_id"],
            mapping["name"],
            mapping["register"],
            mapping["interval"],
            mapping["datatype"],
            mapping["scale"],
            mapping["storage"],
            mapping["trigger_register"],
            mapping["trigger_value"],
        )
        for mapping in mappings
    )

    if signature == _scheduler_signature:
        return

    old_schedule = dict(_next_read_time)
    _scheduler_signature = signature
    now = time.time()

    _next_read_time = {
        (
            mapping["plc_id"],
            mapping["name"]
        ): old_schedule.get(
            (mapping["plc_id"], mapping["name"]),
            now
        )
        for mapping in mappings
    }

    print("TAG SCHEDULE UPDATED")


# ============================================================
# CHECK IF TAG IS DUE
# ============================================================

def tag_is_due(mapping, now):
    key = (mapping["plc_id"], mapping["name"])
    return now >= _next_read_time.get(key, 0)


# ============================================================
# UPDATE NEXT EXECUTION TIME
# ============================================================

def schedule_next(mapping, now):
    try:
        interval = float(mapping.get("interval", 1))
    except Exception:
        interval = 1.0

    if interval <= 0:
        interval = 1.0

    key = (mapping["plc_id"], mapping["name"])
    _next_read_time[key] = now + interval


# ============================================================
# CONVERT DATATYPE
# ============================================================

def convert_value(value, mapping):
    try:
        value = float(value) * float(mapping.get("scale", 1))
    except Exception:
        pass

    datatype = str(mapping.get("datatype", "INT")).upper()

    if datatype == "INT":
        return int(value)

    if datatype == "FLOAT":
        return float(value)

    if datatype == "BOOL":
        return bool(value)

    return value


# ============================================================
# READ ALL TAGS FROM ALL PLCS
# ============================================================

def read_all():
    plc_configs, mappings = get_runtime_configuration()

    if not plc_configs or not mappings:
        return []

    update_scheduler(mappings)

    now = time.time()
    data = []

    mappings_by_plc = {}
    for mapping in mappings:
        mappings_by_plc.setdefault(mapping["plc_id"], []).append(mapping)

    for plc_config in plc_configs:
        plc_id = plc_config["plc_id"]
        plc_mappings = mappings_by_plc.get(plc_id, [])

        if not plc_mappings:
            continue

        client = get_client(plc_config)
        if client is None:
            continue

        slave = plc_config["slave"]

        # ----------------------------------------------------
        # TIME STORAGE
        # ----------------------------------------------------
        for mapping in plc_mappings:
            if mapping["storage"] != "TIME":
                continue

            if not tag_is_due(mapping, now):
                continue

            register = mapping["register"]
            name = mapping["name"]

            value = read_register(
                client,
                register,
                slave
            )

            schedule_next(mapping, now)

            if value is None:
                continue

            try:
                value = convert_value(value, mapping)
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
            })

            print(
                "DUE:",
                "PLC_ID:", plc_id,
                name,
                value,
                "REGISTER:", register,
                "INTERVAL:", mapping["interval"]
            )

        # ----------------------------------------------------
        # TRIGGER STORAGE
        # ----------------------------------------------------
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
            trigger_value = read_register(
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

                value = read_register(
                    client,
                    register,
                    slave
                )

                if value is None:
                    continue

                try:
                    value = convert_value(value, mapping)
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

    return data
