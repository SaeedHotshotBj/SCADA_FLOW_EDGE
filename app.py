from flask import Flask, jsonify
from flask_cors import CORS

from plc_parallel import read_all
from sender import send_all
from write_commands import process_one_write_command
from store_forward import init_queue, pending_count, _db_path

import html
import sqlite3
import threading
import time

import config


# ======================================
# FLASK
# ======================================

app = Flask(__name__)

CORS(app)


# ======================================
# RUNNING FLAG
# ======================================

running = True


# ======================================
# PLC LOOP
# ======================================

def plc_loop():

    print("SCADA FLOW EDGE STARTED")

    while running:

        try:

            # Master PLC writes are handled through the same outbound
            # Edge -> VPS connection used by the normal data pipeline.
            process_one_write_command()

            data = read_all()

            if data:

                send_all(data)

            else:

                # No tag is due at this moment.
                pass

        except Exception as e:

            print(
                "LOOP ERROR:",
                e
            )

        time.sleep(
            config.SCHEDULER_TICK
        )


# ======================================
# LOCAL DATABASE VIEW
# ======================================

def _local_database_html():

    init_queue()
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row

    try:
        tables = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()

        sections = []

        for table_row in tables:
            table_name = table_row["name"]
            safe_table = '"' + table_name.replace('"', '""') + '"'

            columns = conn.execute(
                f"PRAGMA table_info({safe_table})"
            ).fetchall()
            column_names = [column["name"] for column in columns]

            rows = conn.execute(
                f"SELECT * FROM {safe_table} ORDER BY rowid DESC"
            ).fetchall()

            header = "".join(
                f"<th>{html.escape(str(name))}</th>"
                for name in column_names
            )

            body = []
            for row in rows:
                body.append(
                    "<tr>"
                    + "".join(
                        f"<td>{html.escape('' if row[name] is None else str(row[name]))}</td>"
                        for name in column_names
                    )
                    + "</tr>"
                )

            if not body:
                body.append(
                    f'<tr><td colspan="{max(1, len(column_names))}">No records</td></tr>'
                )

            sections.append(
                f"""
                <section class="table-section">
                    <h2>{html.escape(table_name)}</h2>
                    <div class="table-wrap">
                        <table>
                            <thead><tr>{header}</tr></thead>
                            <tbody>{''.join(body)}</tbody>
                        </table>
                    </div>
                    <div class="count">Records: {len(rows)}</div>
                </section>
                """
            )

        if not sections:
            sections.append("<p>No local database tables found.</p>")

        return f"""
        <!doctype html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <meta http-equiv="refresh" content="5">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>SCADA FLOW EDGE - Local Database</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 24px;
                    background: #f5f5f5;
                    color: #222;
                }}
                h1 {{ margin-bottom: 6px; }}
                .info {{ margin-bottom: 24px; color: #555; }}
                .table-section {{
                    background: white;
                    margin-bottom: 28px;
                    padding: 18px;
                    border-radius: 8px;
                    box-shadow: 0 1px 5px rgba(0,0,0,.12);
                }}
                .table-wrap {{ overflow-x: auto; }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    white-space: nowrap;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px 10px;
                    text-align: left;
                }}
                th {{ background: #eee; }}
                .count {{ margin-top: 10px; color: #666; }}
            </style>
        </head>
        <body>
            <h1>LOCAL DATABASE</h1>
            <div class="info">
                Database: <b>{html.escape(_db_path())}</b><br>
                Pending Records: <b>{pending_count()}</b><br>
                Auto refresh: 5 seconds
            </div>
            {''.join(sections)}
        </body>
        </html>
        """
    finally:
        conn.close()


# ======================================
# HOME
# ======================================

@app.route("/")
def home():

    try:
        return _local_database_html()
    except Exception as e:
        return (
            "<h1>LOCAL DATABASE ERROR</h1>"
            f"<pre>{html.escape(str(e))}</pre>",
            500,
        )


# ======================================
# STATUS
# ======================================

@app.route("/status")
def status():

    return jsonify({

        "status":
            "running",

        "PLC_ID":
            config.PLC_ID,

        "server":
            config.SERVER_URL

    })


# ======================================
# MAIN
# ======================================

if __name__ == "__main__":

    thread = threading.Thread(

        target=plc_loop,

        daemon=True
    )

    thread.start()

    app.run(

        host="0.0.0.0",

        port=5001,

        debug=False
    )