
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
        and (now - _flow_cache_time)
        < config.FLOW_REFRESH_INTERVAL
    ):
        return _flow_cache

    url = (
        config.SERVER_URL.rstrip("/")
        + "/api/edge/config"
    )

    try:

        response = requests.get(
            url,
            params={
                "PLC_ID": config.PLC_ID
            },
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

        print(
            "FLOW CONFIG ERROR:",
            e
        )

        return _flow_cache


# ============================================================
# EXTRACT RUNTIME CONFIGURATION
#
# PLC configuration comes ONLY from PLCReader.
# Tag configuration comes ONLY from TagMapper.
# ============================================================

def get_runtime_configuration():

    flow = get_flow_config()

    if not flow:
        return None, []

    try:

        nodes = (
            flow
            ["drawflow"]
            ["Home"]
            ["data"]
        )

    except Exception as e:

        print(
            "FLOW FORMAT ERROR:",
            e
        )

        return None, []


    # ========================================================
    # PLC READER
    # ========================================================

    plc_config = None

    for node in nodes.values():

        node_type = node.get("class")

        if node_type != "PLCReader":
            continue

        data = node.get(
            "data",
            {}
        )

        required = [
            "ip",
            "port",
            "slave",
            "register",
            "count"
        ]

        missing = [
            key
            for key in required
            if data.get(key) is None
            or data.get(key) == ""
        ]

        if missing:

            print(
                "PLCReader CONFIGURATION INCOMPLETE:",
                missing
            )

            return None, []

        try:

            port = int(
                data["port"]
            )

            slave = int(
                data["slave"]
            )

            register = int(
                data["register"]
            )

            count = int(
                data["count"]
            )

        except Exception as e:

            print(
                "PLCReader CONFIGURATION ERROR:",
                e
            )

            return None, []

        if port <= 0:
            print("INVALID PLC PORT")
            return None, []

        if slave < 0:
            print("INVALID PLC SLAVE")
            return None, []

        if register < 0:
            print("INVALID PLC REGISTER")
            return None, []

        if count <= 0:
            print("INVALID PLC COUNT")
            return None, []

        plc_config = {

            "ip":
                str(data["ip"]),

            "port":
                port,

            "slave":
                slave,

            "register":
                register,

            "count":
                count

        }

        break


    if plc_config is None:

        print(
            "NO PLCReader NODE FOUND"
        )

        return None, []


    # ========================================================
    # TAG MAPPER
    # ========================================================

    mappings = []

    for node in nodes.values():

        node_type = node.get("class")

        if node_type != "TagMapper":
            continue

        data = node.get(
            "data",
            {}
        )

        node_mappings = data.get(
            "mappings",
            []
        )

        for mapping in node_mappings:

            if not isinstance(
                mapping,
                dict
            ):
                continue

            if (
                mapping.get("register")
                is None
            ):
                continue

            if (
                mapping.get("name")
                is None
                or str(mapping.get("name")).strip() == ""
            ):
                continue

            try:

                register = int(
                    mapping["register"]
                )

            except Exception:

                print(
                    "INVALID TAG REGISTER:",
                    mapping
                )

                continue


            if register < 0:

                print(
                    "NEGATIVE TAG REGISTER:",
                    register
                )

                continue


            try:

                scale = float(
                    mapping.get(
                        "scale",
                        1
                    )
                )

            except Exception:

                scale = 1.0


            try:

                interval = float(
                    mapping.get(
                        "interval",
                        1
                    )
                )

            except Exception:

                interval = 1.0


            if interval <= 0:
                interval = 1.0


            datatype = str(
                mapping.get(
                    "datatype",
                    "INT"
                )
            ).upper()


            storage = str(
                mapping.get(
                    "storage",
                    "TIME"
                )
            ).upper()


            mappings.append({

                "register":
                    register,

                "name":
                    str(
                        mapping["name"]
                    ),

                "datatype":
                    datatype,

                "scale":
                    scale,

                "storage":
                    storage,

                "interval":
                    interval,

                "trigger_register":
                    mapping.get(
                        "trigger_register",
                        0
                    ),

                "trigger_value":
                    mapping.get(
                        "trigger_value",
                        0
                    )

            })

        break


    if not mappings:

        print(
            "NO TAG MAPPINGS FOUND"
        )

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


    # --------------------------------------------------------
    # PLC CONFIGURATION CHANGED
    # --------------------------------------------------------

    if _client_config != current_config:

        if _client is not None:

            try:
                _client.close()

            except Exception:
                pass

        _client = None

        _client_config = current_config

        print(
            "PLC CLIENT CONFIGURATION UPDATED:",
            current_config
        )


    # --------------------------------------------------------
    # CREATE CLIENT
    # --------------------------------------------------------

    if _client is None:

        _client = ModbusTcpClient(

            plc_config["ip"],

            port=plc_config["port"],

            timeout=3

        )


    # --------------------------------------------------------
    # CONNECT
    # --------------------------------------------------------

    try:

        if not _client.is_socket_open():

            if not _client.connect():

                print(
                    "PLC CONNECTION FAILED:",
                    plc_config["ip"],
                    plc_config["port"]
                )

                return None

    except Exception as e:

        print(
            "PLC CONNECTION ERROR:",
            e
        )

        return None


    return _client


# ============================================================
# READ ONE PLC REGISTER
# ============================================================

def read_register(
    client,
    address,
    slave
):

    try:

        result = client.read_holding_registers(

            address=int(address),

            count=1,

            unit=int(slave)

        )


        if result.isError():

            print(
                "MODBUS ERROR:",
                address
            )

            return None


        if not hasattr(
            result,
            "registers"
        ):

            return None


        if not result.registers:

            return None


        return result.registers[0]


    except TypeError:

        # Compatibility with newer PyModbus versions

        try:

            result = client.read_holding_registers(

                address=int(address),

                count=1,

                slave=int(slave)

            )

            if result.isError():
                return None

            if not result.registers:
                return None

            return result.registers[0]

        except Exception as e:

            print(
                "PLC READ ERROR:",
                e
            )

            return None


    except Exception as e:

        print(
            "PLC READ ERROR:",
            e
        )

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

            mapping["scale"]

        )

        for mapping in mappings

    )


    if signature == _scheduler_signature:

        return


    old_schedule = dict(
        _next_read_time
    )


    _scheduler_signature = signature


    now = time.time()

    new_schedule = {}


    for mapping in mappings:

        name = mapping["name"]


        if name in old_schedule:

            new_schedule[name] = (
                old_schedule[name]
            )

        else:

            # New tag:
            # execute immediately.

            new_schedule[name] = now


    _next_read_time = new_schedule


    print(
        "TAG SCHEDULE UPDATED"
    )


# ============================================================
# CHECK IF TAG IS DUE
# ============================================================

def tag_is_due(
    mapping,
    now
):

    name = mapping["name"]


    next_time = _next_read_time.get(
        name,
        0
    )


    return now >= next_time


# ============================================================
# UPDATE NEXT EXECUTION TIME
# ============================================================

def schedule_next(
    mapping,
    now
):

    name = mapping["name"]


    try:

        interval = float(
            mapping.get(
                "interval",
                1
            )
        )

    except Exception:

        interval = 1.0


    if interval <= 0:
        interval = 1.0


    _next_read_time[name] = (
        now + interval
    )


# ============================================================
# CONVERT DATATYPE
# ============================================================

def convert_value(
    value,
    mapping
):

    scale = mapping.get(
        "scale",
        1
    )

    try:

        value = (
            float(value)
            * float(scale)
        )

    except Exception:

        pass


    datatype = str(
        mapping.get(
            "datatype",
            "INT"
        )
    ).upper()


    if datatype == "INT":

        return int(value)


    if datatype == "FLOAT":

        return float(value)


    if datatype == "BOOL":

        return bool(value)


    return value


# ============================================================
# READ ALL DUE TAGS
# ============================================================

def read_all():

    plc_config, mappings = (
        get_runtime_configuration()
    )


    if not plc_config:

        return {}


    if not mappings:

        return {}


    # ========================================================
    # UPDATE SCHEDULER
    # ========================================================

    update_scheduler(
        mappings
    )


    now = time.time()


    due_mappings = [

        mapping

        for mapping in mappings

        if tag_is_due(
            mapping,
            now
        )

    ]


    if not due_mappings:

        return {}


    # ========================================================
    # PLC CLIENT
    # ========================================================

    client = get_client(
        plc_config
    )


    if client is None:

        return {}


    slave = plc_config["slave"]


    data = {}


    # ========================================================
    # READ DUE TAGS
    # ========================================================

    for mapping in due_mappings:

        register = mapping["register"]

        name = mapping["name"]


        value = read_register(

            client,

            register,

            slave

        )


        # ----------------------------------------------------
        # Schedule next execution regardless of read result.
        # ----------------------------------------------------

        schedule_next(

            mapping,

            now

        )


        if value is None:

            continue


        try:

            value = convert_value(
                value,
                mapping
            )

        except Exception as e:

            print(
                "VALUE CONVERSION ERROR:",
                name,
                e
            )

            continue


        data[name] = value


        print(

            "DUE:",

            name,

            value,

            "REGISTER:",

            register,

            "INTERVAL:",

            mapping.get(
                "interval",
                1
            )

        )


    return data

