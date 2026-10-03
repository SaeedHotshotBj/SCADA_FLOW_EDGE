import json
import socket
from pathlib import Path


ROOT = Path(__file__).resolve().parent
TARGET = ROOT / "edge_management_config.json"


def ask(prompt, default=""):
    suffix = f" [{default}]" if default else ""
    value = input(prompt + suffix + ": ").strip()
    return value or default


def main():
    print("SCADA FLOW EDGE MANAGEMENT CONFIGURATION")
    print()

    server_url = ask(
        "SCADA FLOW Server URL",
        "https://scada.khze.org",
    )
    company_id = int(ask("Company ID"))
    edge_id = ask("Edge ID", socket.gethostname())
    pairing_token = ask("Pairing code")
    poll_interval = float(ask("Management poll interval (seconds)", "2"))
    auto_start = ask("Auto-start SCADA FLOW EDGE after Agent starts (Y/N)", "Y").upper() != "N"

    if not server_url or not pairing_token:
        raise SystemExit("Server URL and pairing code are required.")

    config = {
        "server_url": server_url.rstrip("/"),
        "company_id": company_id,
        "edge_id": edge_id,
        "pairing_token": pairing_token,
        "poll_interval": max(1.0, min(poll_interval, 30.0)),
        "auto_start": auto_start,
    }

    with TARGET.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, ensure_ascii=False, indent=2)
        handle.write("\\n")

    print()
    print("Saved:", TARGET)
    print("Keep this file private. It contains the Edge management credential.")


if __name__ == "__main__":
    main()
