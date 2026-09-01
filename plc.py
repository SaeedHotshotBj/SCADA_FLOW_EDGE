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
# TRIGGER STATE
# ============================================================

_last_trigger_value = {}


# ============================================================
# PLC CLIENT
# ============================================================

_client = None
_client_config = None


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
# EXTRACT RUNTIME CONFIGURATION
# ============================================================

def get_runtime_configuration():

    flow = get_flow_config()

    if not flow:
        return None, []

    try:
        nodes = flow["drawflow"]["Home"]["data"]
    except Exception as e:
        print("FLOW FORMAT ERROR:", e)
        return None, []

    # --------------------------------------------------------
    # PLC READER
    # --------------------------------------------------------

    plc_config = None

    for node in nodes.values():
        if node.get("class") != "PLCReader":
            continue

        data = node.get("data", {})
        required = ["ip", "port", "slave", "register", "count"]
        missing = [
            key for key in required
            if data.get(key) is None or data.get(key) == ""
        ]

        if missing:
            print("PLCReader CONFIGURATION INCOMPLETE:", missing)
            return None, []

        try:
            port = int(data["port"])
            slave = int(data["slave"])
            register = int(data["register"])
            count = int(data["count"])
        except Exception as e:
            print("PLCReader CONFIGURATION ERROR:", e)
            return None, []

        if port <= 0 or slave < 0 or register < 0 or count <= 0:
            print("INVALID PLCReader CONFIGURATION")
            return None, []

        plc_config = {
            "ip": str(data["ip"]),
            "port": port,
            "slave": slave,
            "register": register,
            "count": count,
        }
        break

    if plc_config is None:
        print("NO PLCReader NODE FOUND")
        return None, []

    # --------------------------------------------------------
    # TAG MAPPER
    # --------------------------------------------------------

    mappings = []

    for node in nodes.values():
        if node.get("class") != "TagMapper":
            continue

        data = node.get("data", {})
        node_mappings = data.get("mappings", [])

        for mapping in node_mappings:
            if not isinstance(mapping, dict):
                continue

            name = mapping.get("name")
            register_value = mapping.get("register")

            if name is None or str(name).strip() == "":
                continue
            if register_value is None:
                continue

            try:
                register = int(register_value)
            except Exception:
                print("INVALID TAG REGISTER:", mapping)
                continue

            if register < 0:
                print("NEGATIVE TAG REGISTER:", register)
                continue

            try:
                scale = float(mapping.get("scale", 1))
            except Exception:
                scale = 1.0

            try:
                interval = float(mapping.get("interval", 1))
            except Exception:
                interval = 1.0
            if interval <= 0:
                interval = 1.0

            datatype = str(mapping.get("datatype", "INT")).upper()
            storage = str(mapping.get("storage", "TIME")).upper()

            trigger_register = mapping.get("trigger_register", 0)
            trigger_value = mapping.get("trigger_value", 0)

            try:
                if trigger_register not in (None, ""):
                    trigger_register = int(trigger_register)
            except Exception:
                print("INVALID TRIGGER REGISTER:", mapping)
                trigger_register = 0

            try:
                if trigger_value not in (None, ""):
                    trigger_value = float(trigger_value)
            except Exception:
                print("INVALID TRIGGER VALUE:", mapping)
                trigger_value = 0

            mappings.append({
                "register": register,
                "name": str(name),
                "datatype": datatype,
                "scale": scale,
                "storage": storage,
                "interval": interval,
                "trigger_register": trigger_register,
                "trigger_value": trigger_value,
            })

        break

    if not mappings:
        print("NO TAG MAPPINGS FOUND")
        return plc_config, []

    return plc_config, mappings


# ============================================================
# PLC CLIENT MANAGEMENT
# ============================================================

def get_client(plc_config):

    global _client
    global _client_config

    current_config = (
        plc_config["ip"],
        plc_config["port"],
        plc_config["slave"]
    )

    if _client_config != current_config:
        if _client is not None:
            try:
                _client.close()
            except Exception:
                pass

        _client = None
        _client_config = current_config
        print("PLC CLIENT CONFIGURATION UPDATED:", current_config)

    if _client is None:
        _client = ModbusTcpClient(
            plc_config["ip"],
            port=plc_config["port"],
            timeout=3
        )

    try:
        if not _client.is_socket_open():
            if not _client.connect():
                print("PLC CONNECTION FAILED:", plc_config["ip"], plc_config["port"])
                return None
    except Exception as e:
        print("PLC CONNECTION ERROR:", e)
        return None

    return _client


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
        mapping["name"]: old_schedule.get(mapping["name"], now)
        for mapping in mappings
    }
    print("TAG SCHEDULE UPDATED")


# ============================================================
# CHECK IF TAG IS DUE
# ============================================================

def tag_is_due(mapping, now):
    return now >= _next_read_time.get(mapping["name"], 0)


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
    _next_read_time[mapping["name"]] = now + interval


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
# TRIGGER HANDLING
# ============================================================

def _read_trigger_state(client, mappings, slave):
    """Read each distinct trigger register once and return its values."""

    trigger_registers = sorted({
        int(mapping["trigger_register"])
        for mapping in mappings
        if mapping["storage"] == "TRIGGER"
        and mapping.get("trigger_register") not in (None, "", 0)
    })

    trigger_states = {}

    for register in trigger_registers:
        value = read_register(client, register, slave)
        if value is not None:
            trigger_states[register] = value

    return trigger_states


def _trigger_rose(mapping, trigger_states):
    register = mapping.get("trigger_register")
    if register in (None, "", 0):
        return False

    try:
        register = int(register)
    except Exception:
        return False

    if register not in trigger_states:
        return False

    current_value = trigger_states[register]
    last_value = _last_trigger_value.get(register)

    # First observation establishes state but does not create a false rising edge.
    _last_trigger_value[register] = current_value

    return last_value is not None and current_value != last_value and float(current_value) == float(mapping.get("trigger_value", 0))


# ============================================================
# READ ALL DUE TAGS
# ============================================================

def read_all():

    plc_config, mappings = get_runtime_configuration()

    if not plc_config or not mappings:
        return {}

    update_scheduler(mappings)
    now = time.time()
    client = get_client(plc_config)

    if client is None:
        return {}

    slave = plc_config["slave"]
    data = {}

    # --------------------------------------------------------
    # TIME TAGS
    # --------------------------------------------------------

    for mapping in mappings:
        if mapping["storage"] != "TIME":
            continue

        if not tag_is_due(mapping, now):
            continue

        register = mapping["register"]
        name = mapping["name"]
        value = read_register(client, register, slave)
        schedule_next(mapping, now)

        if value is None:
            continue

        try:
            value = convert_value(value, mapping)
        except Exception as e:
            print("VALUE CONVERSION ERROR:", name, e)
            continue

        data[name] = value
        print("DUE:", name, value, "REGISTER:", register, "INTERVAL:", mapping["interval"])

    # --------------------------------------------------------
    # TRIGGER TAGS
    # --------------------------------------------------------
    # Every TRIGGER mapping is handled here. Once its configured
    # trigger changes to the configured trigger value, its own
    # register is read and added to the same outgoing payload.
    # This includes ContractCode/ProductCode and any future tag.
    # --------------------------------------------------------

    trigger_states = _read_trigger_state(client, mappings, slave)

    if trigger_states:
        for mapping in mappings:
            if mapping["storage"] != "TRIGGER":
                continue

            if not _trigger_rose(mapping, trigger_states):
                continue

            register = mapping["register"]
            name = mapping["name"]
            value = read_register(client, register, slave)

            if value is None:
                continue

            try:
                value = convert_value(value, mapping)
            except Exception as e:
                print("VALUE CONVERSION ERROR:", name, e)
                continue

            data[name] = value
            print(
                "TRIGGER:", name, value,
                "REGISTER:", register,
                "TRIGGER REGISTER:", mapping["trigger_register"],
                "TRIGGER VALUE:", mapping["trigger_value"]
            )

    return data
