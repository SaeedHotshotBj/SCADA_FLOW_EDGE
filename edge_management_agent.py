"""SCADA FLOW Edge Management Agent.

Runs on the Edge computer and polls the SCADA FLOW server for a restricted
management command set. It never executes arbitrary shell commands.
"""

import base64
import json
import os
import platform
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psutil
import requests


AGENT_VERSION = "1.0.0"
CONFIG_NAME = "edge_management_config.json"
MAX_WRITE_BYTES = 10 * 1024 * 1024
MAX_READ_BYTES = 2 * 1024 * 1024

PROTECTED_NAMES = {
    CONFIG_NAME.lower(),
    "edge_management_agent.py",
    "edge_management_config.py",
    "run_edge_management.bat",
    "run_edge_management.vbs",
    "install_edge_management.bat",
    "uninstall_edge_management.bat",
    "configure_edge_management.bat",
}

PROTECTED_EXTENSIONS = {
    ".db",
    ".sqlite",
    ".sqlite3",
}

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
}

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / CONFIG_NAME


def load_config():
    if not CONFIG_PATH.exists():
        raise RuntimeError(
            f"Missing {CONFIG_NAME}. Run configure_edge_management.bat first."
        )

    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    server_url = str(config.get("server_url") or "").strip().rstrip("/")
    edge_id = str(config.get("edge_id") or "").strip()
    pairing_token = str(config.get("pairing_token") or "").strip()

    if not server_url or not edge_id or not pairing_token:
        raise RuntimeError(
            "server_url, edge_id and pairing_token are required in "
            f"{CONFIG_NAME}."
        )

    company_id = int(config.get("company_id"))
    poll_interval = float(config.get("poll_interval", 2.0))
    poll_interval = max(1.0, min(poll_interval, 30.0))
    auto_start = bool(config.get("auto_start", True))

    return {
        "server_url": server_url,
        "edge_id": edge_id,
        "company_id": company_id,
        "pairing_token": pairing_token,
        "poll_interval": poll_interval,
        "auto_start": auto_start,
    }


def auth_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _find_edge_process():
    root_text = str(ROOT.resolve()).lower()

    for process in psutil.process_iter(["pid", "cmdline", "cwd", "name"]):
        try:
            cwd = process.info.get("cwd") or ""
            if str(Path(cwd).resolve()).lower() != root_text:
                continue

            cmdline = process.info.get("cmdline") or []
            joined = " ".join(str(item) for item in cmdline).lower()
            if "app.py" not in joined:
                continue
            if "edge_management_agent.py" in joined:
                continue

            return process
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError, ValueError):
            continue

    return None


def _edge_process_running():
    process = _find_edge_process()
    if process is None:
        return False, None
    try:
        return process.is_running(), int(process.pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False, None


def start_edge():
    existing = _find_edge_process()
    if existing is not None:
        return {
            "ok": True,
            "message": "SCADA FLOW EDGE is already running.",
            "pid": int(existing.pid),
        }

    creationflags = 0
    startupinfo = None

    if os.name == "nt":
        creationflags = (
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

    command = [sys.executable, str(ROOT / "app.py")]

    process = subprocess.Popen(
        command,
        cwd=str(ROOT),
        creationflags=creationflags,
        startupinfo=startupinfo,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    time.sleep(1.0)

    if process.poll() is not None:
        raise RuntimeError(
            f"SCADA FLOW EDGE stopped during startup (exit code {process.returncode})."
        )

    return {
        "ok": True,
        "message": "SCADA FLOW EDGE started.",
        "pid": int(process.pid),
    }


def stop_edge():
    process = _find_edge_process()
    if process is None:
        return {
            "ok": True,
            "message": "SCADA FLOW EDGE is already stopped.",
            "pid": None,
        }

    try:
        children = process.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        children = []

    for child in children:
        try:
            child.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    try:
        process.terminate()
        process.wait(timeout=10)
    except psutil.TimeoutExpired:
        try:
            process.kill()
            process.wait(timeout=5)
        except (psutil.NoSuchProcess, psutil.TimeoutExpired):
            pass

    for child in children:
        try:
            if child.is_running():
                child.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return {
        "ok": True,
        "message": "SCADA FLOW EDGE stopped.",
        "pid": int(process.pid),
    }


def restart_edge():
    stop_edge()
    return start_edge()


def status_edge():
    running, pid = _edge_process_running()
    return {
        "ok": True,
        "message": "SCADA FLOW EDGE is running." if running else "SCADA FLOW EDGE is stopped.",
        "pid": pid,
        "running": running,
    }


def _safe_path(relative_path):
    relative_path = str(relative_path or "").strip().replace("\\", "/")
    if not relative_path:
        raise ValueError("File path is required")

    candidate = (ROOT / Path(relative_path)).resolve()
    root_resolved = ROOT.resolve()

    try:
        candidate.relative_to(root_resolved)
    except ValueError:
        raise ValueError("Path traversal is not allowed")

    parts = [part for part in candidate.relative_to(root_resolved).parts if part not in ("", ".")]
    if not parts:
        raise ValueError("A file path is required")

    return candidate, "/".join(parts)


def _is_protected(candidate, relative_path):
    lower = relative_path.lower().replace("\\", "/")
    if any(lower == name or lower.startswith(name + "/") for name in (".git", ".venv", "venv", "__pycache__")):
        return True
    if candidate.name.lower() in PROTECTED_NAMES:
        return True
    if candidate.suffix.lower() in PROTECTED_EXTENSIONS:
        return True
    return False


def list_files():
    files = []

    for current_root, dirs, names in os.walk(ROOT):
        dirs[:] = [
            name for name in dirs
            if name.lower() not in EXCLUDED_DIRS
        ]

        for name in names:
            path = Path(current_root) / name
            try:
                relative = path.relative_to(ROOT).as_posix()
                stat = path.stat()
            except OSError:
                continue

            files.append({
                "path": relative,
                "size": int(stat.st_size),
                "modified": float(stat.st_mtime),
                "protected": _is_protected(path, relative),
            })

    files.sort(key=lambda item: item["path"].lower())
    return {
        "ok": True,
        "message": f"{len(files)} files found.",
        "files": files[:500],
    }


def read_file(relative_path):
    path, normalized = _safe_path(relative_path)
    if not path.is_file():
        raise FileNotFoundError("File does not exist")

    if _is_protected(path, normalized):
        raise PermissionError("This file is protected from remote reading")

    size = path.stat().st_size
    if size > MAX_READ_BYTES:
        raise ValueError("File is too large to read remotely")

    data = path.read_bytes()

    return {
        "ok": True,
        "message": f"Read {normalized}.",
        "path": normalized,
        "size": len(data),
        "content_b64": base64.b64encode(data).decode("ascii"),
    }


def write_file(relative_path, content_b64):
    path, normalized = _safe_path(relative_path)

    if _is_protected(path, normalized):
        raise PermissionError("This file is protected from remote replacement")

    try:
        data = base64.b64decode(str(content_b64 or ""), validate=True)
    except Exception as exc:
        raise ValueError(f"Invalid base64 file content: {exc}")

    if len(data) > MAX_WRITE_BYTES:
        raise ValueError("File is larger than the 10 MB limit")

    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_name = tempfile.mkstemp(
        prefix=".edge-upload-",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            if os.path.exists(temp_name):
                os.remove(temp_name)
        except OSError:
            pass

    return {
        "ok": True,
        "message": f"Saved {normalized}.",
        "path": normalized,
        "size": len(data),
    }


def delete_file(relative_path):
    path, normalized = _safe_path(relative_path)

    if _is_protected(path, normalized):
        raise PermissionError("This file is protected from remote deletion")

    if not path.exists():
        raise FileNotFoundError("File does not exist")
    if not path.is_file():
        raise ValueError("Only files can be deleted")

    path.unlink()

    return {
        "ok": True,
        "message": f"Deleted {normalized}.",
        "path": normalized,
    }


def execute_command(command):
    command_name = str(command.get("command") or "").strip().upper()
    path = command.get("path") or ""
    payload = command.get("payload") or {}

    if command_name == "STATUS":
        return status_edge()
    if command_name == "START":
        return start_edge()
    if command_name == "STOP":
        return stop_edge()
    if command_name == "RESTART":
        return restart_edge()
    if command_name == "LIST_FILES":
        return list_files()
    if command_name == "READ_FILE":
        return read_file(path)
    if command_name == "WRITE_FILE":
        return write_file(path, payload.get("content_b64"))
    if command_name == "DELETE_FILE":
        return delete_file(path)

    raise ValueError(f"Unsupported management command: {command_name}")


def heartbeat(config):
    running, pid = _edge_process_running()
    payload = {
        "edge_id": config["edge_id"],
        "company_id": config["company_id"],
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "agent_version": AGENT_VERSION,
        "root_path": str(ROOT),
        "app_running": bool(running),
        "app_pid": pid,
    }

    response = requests.post(
        config["server_url"] + "/api/edge/management/heartbeat",
        json=payload,
        headers=auth_headers(config["pairing_token"]),
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def poll(config):
    payload = {
        "edge_id": config["edge_id"],
    }

    response = requests.post(
        config["server_url"] + "/api/edge/management/poll",
        json=payload,
        headers=auth_headers(config["pairing_token"]),
        timeout=15,
    )
    response.raise_for_status()
    return response.json().get("command")


def report_result(config, command_id, success, result):
    payload = {
        "edge_id": config["edge_id"],
        "command_id": command_id,
        "success": bool(success),
        "result": result if isinstance(result, dict) else {"message": str(result)},
    }

    response = requests.post(
        config["server_url"] + "/api/edge/management/result",
        json=payload,
        headers=auth_headers(config["pairing_token"]),
        timeout=15,
    )
    response.raise_for_status()


def main():
    config = load_config()
    print("SCADA FLOW EDGE MANAGEMENT AGENT STARTED")
    print("Edge ID:", config["edge_id"])
    print("Company ID:", config["company_id"])

    registered = False

    while True:
        try:
            heartbeat(config)

            if not registered:
                registered = True
                if config["auto_start"]:
                    try:
                        start_edge()
                    except Exception as exc:
                        print("AUTO START ERROR:", exc)

            command = poll(config)

            if command:
                command_id = command.get("command_id")
                print(
                    "MANAGEMENT COMMAND:",
                    command.get("command"),
                    command.get("path") or "",
                )

                try:
                    result = execute_command(command)
                    report_result(config, command_id, True, result)
                except Exception as exc:
                    print("MANAGEMENT COMMAND ERROR:", exc)
                    report_result(
                        config,
                        command_id,
                        False,
                        {
                            "ok": False,
                            "message": str(exc),
                        },
                    )

            time.sleep(config["poll_interval"])
        except KeyboardInterrupt:
            print("SCADA FLOW EDGE MANAGEMENT AGENT STOPPED")
            return
        except Exception as exc:
            registered = False
            print("MANAGEMENT LOOP ERROR:", exc)
            time.sleep(max(config["poll_interval"], 5.0))


if __name__ == "__main__":
    main()
