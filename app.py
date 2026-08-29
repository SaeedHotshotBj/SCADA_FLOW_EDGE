from flask import Flask, jsonify
from flask_cors import CORS

from plc import read_all
from sender import send_all

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
# HOME
# ======================================

@app.route("/")
def home():

    return "SCADA FLOW EDGE ONLINE"


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