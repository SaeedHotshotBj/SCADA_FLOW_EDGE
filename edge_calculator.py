"""Flow-driven local calculation engine for SCADA_FLOW_EDGE."""

import math
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import config
import plc

TZ = ZoneInfo("Asia/Tehran")
_calculation_cache = None
_calculation_cache_time = 0.0
_latest_tags = {}
_next_run = 0.0


def _nodes(flow):
    return (
        (flow or {})
        .get("drawflow", {})
        .get("Home", {})
        .get("data", {})
        or {}
    )


def _config(node):
    data = node.get("data", {}) or {}
    config_data = data.get("config", data)
    return config_data if isinstance(config_data, dict) else {}


def _node_inputs(nodes, node_id):
    node = nodes.get(str(node_id), {})
    result = []
    for item in (node.get("inputs", {}) or {}).values():
        if not isinstance(item, dict):
            continue
        for connection in item.get("connections", []) or []:
            if isinstance(connection, dict) and connection.get("node") is not None:
                result.append(str(connection["node"]))
    return result


def _plc_reader_ids(nodes):
    result = {}
    fallback = int(getattr(config, "PLC_ID", 1))
    for node_id, node in nodes.items():
        if (node.get("name") or node.get("class")) != "PLCReader":
            continue
        raw = (node.get("data", {}) or {}).get("plc_id", (node.get("data", {}) or {}).get("PLC_ID"))
        try:
            plc_id = int(raw) if raw not in (None, "") else fallback
        except (TypeError, ValueError):
            plc_id = fallback
        result[str(node_id)] = plc_id
        fallback += 1
    return result


def _tagmapper_plcs(nodes, reader_ids):
    result = {}
    for node_id, node in nodes.items():
        if node.get("name", node.get("class")) != "TagMapper":
            continue
        data = _config(node)
        mappings = data.get("mappings", [])
        explicit = {
            int(item.get("plc_id", item.get("PLC_ID")))
            for item in mappings
            if isinstance(item, dict)
            and item.get("name")
            and item.get("plc_id", item.get("PLC_ID")) not in (None, "")
            and str(item.get("plc_id", item.get("PLC_ID"))).strip().lstrip("-").isdigit()
        }
        upstream = []
        queue = list(_node_inputs(nodes, node_id))
        seen = set()
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            if current in reader_ids:
                upstream.append(reader_ids[current])
                continue
            queue.extend(_node_inputs(nodes, current))

        ids = sorted(set(explicit) | set(upstream))
        if not ids and len(reader_ids) == 1:
            ids = [next(iter(reader_ids.values()))]
        result[str(node_id)] = ids
    return result


def _expression_plan(flow):
    global _calculation_cache, _calculation_cache_time
    now = time.monotonic()
    refresh = float(getattr(config, "FLOW_REFRESH_INTERVAL", 30))
    if _calculation_cache is not None and now - _calculation_cache_time < refresh:
        return _calculation_cache

    nodes = _nodes(flow)
    reader_ids = _plc_reader_ids(nodes)
    mapper_plcs = _tagmapper_plcs(nodes, reader_ids)
    plan = []

    for node_id, node in nodes.items():
        if node.get("name") != "ExpressionNode":
            continue

        cfg = _config(node)
        expressions = cfg.get("expressions", [])
        if not isinstance(expressions, list):
            continue

        upstream_ids = []
        queue = list(_node_inputs(nodes, node_id))
        seen = set()
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            node_data = nodes.get(current, {})
            node_name = node_data.get("name") if isinstance(node_data, dict) else None
            if node_name == "PLCReader" and current in reader_ids:
                upstream_ids.append(reader_ids[current])
                continue
            if node_name == "TagMapper":
                upstream_ids.extend(mapper_plcs.get(current, []))
            queue.extend(_node_inputs(nodes, current))

        upstream_ids = sorted(set(upstream_ids))

        for item in expressions:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", item.get("result_name", ""))).strip()
            expression = str(item.get("expression", "")).strip()
            if not name or not expression:
                continue
            explicit = item.get("plc_id", item.get("PLC_ID"))
            if explicit not in (None, ""):
                try:
                    plc_ids = [int(explicit)]
                except (TypeError, ValueError):
                    plc_ids = list(upstream_ids)
            else:
                plc_ids = list(upstream_ids)

            if not plc_ids:
                plc_ids = sorted({int(key[0]) for key in _latest_tags})

            plan.append({
                "name": name,
                "expression": expression,
                "plc_ids": plc_ids,
                "label": str(item.get("label", name)).strip() or name,
                "unit": str(item.get("unit", "")).strip(),
            })

    _calculation_cache = plan
    _calculation_cache_time = now
    return plan


def _math_scope():
    scope = {
        "math": math,
        "pi": math.pi,
        "e": math.e,
        "sqrt": math.sqrt,
        "abs": abs,
        "min": min,
        "max": max,
        "round": round,
        "pow": pow,
        "floor": math.floor,
        "ceil": math.ceil,
        "sin": math.sin,
        "cos": math.cos,
        "tan": math.tan,
        "asin": math.asin,
        "acos": math.acos,
        "atan": math.atan,
        "log": math.log,
        "log10": math.log10,
        "exp": math.exp,
    }
    return scope


def _refresh_latest(data):
    if not isinstance(data, list):
        return
    for item in data:
        if not isinstance(item, dict):
            continue
        storage = str(item.get("StorageType", "LIVE")).upper()
        if storage not in {"LIVE", "TIME"}:
            continue
        tag = str(item.get("TagName", "")).strip()
        if not tag:
            continue
        try:
            plc_id = int(item["PLC_ID"])
            value = float(item["Value"])
        except (KeyError, TypeError, ValueError):
            continue
        _latest_tags[(plc_id, tag)] = value


def calculate(data):
    global _next_run
    _refresh_latest(data)

    now_monotonic = time.monotonic()
    interval = max(0.1, float(getattr(config, "CALCULATION_INTERVAL", 1.0)))
    if now_monotonic < _next_run:
        return []

    _next_run = now_monotonic + interval

    flow = plc.get_flow_config()
    if not flow:
        return []

    results = []
    plan = _expression_plan(flow)
    scopes = {}

    for item in plan:
        for plc_id in item["plc_ids"]:
            if plc_id not in scopes:
                scope = _math_scope()
                scope.update({
                    tag: value
                    for (pid, tag), value in _latest_tags.items()
                    if pid == plc_id
                })
                scopes[plc_id] = scope

            scope = scopes[plc_id]

            try:
                value = eval(
                    item["expression"],
                    {"__builtins__": {}},
                    scope,
                )
                value = float(value)
            except Exception as exc:
                print(
                    "EDGE CALCULATION ERROR:",
                    "PLC_ID:", plc_id,
                    "Result:", item["name"],
                    "Expression:", item["expression"],
                    "Error:", exc,
                )
                continue

            if not math.isfinite(value):
                continue

            # Calculated results become variables for the next expression
            # in the same PLC scope, preserving Flow-driven formula chains.
            scope[item["name"]] = value

            results.append({
                "PLC_ID": plc_id,
                "TagName": item["name"],
                "Value": value,
                "StorageType": "CALCULATED",
                "Timestamp": datetime.now(TZ).replace(tzinfo=None).isoformat(),
                "title": item["label"],
                "unit": item["unit"],
            })

    if results:
        print("EDGE CALCULATIONS:", len(results))
    return results


__all__ = ["calculate"]
