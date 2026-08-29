# ======================================
# SCADA FLOW EDGE CONFIGURATION
# ======================================

# VPS SCADA FLOW SERVER
SERVER_URL = "https://scada.khze.org"

# EDGE DEVICE ID
PLC_ID = 1

# How often Edge checks the per-tag scheduler
# This is NOT the tag interval.
# Actual tag intervals come from Flow Editor.
SCHEDULER_TICK = 0.1

# How often Edge refreshes Flow configuration
FLOW_REFRESH_INTERVAL = 30