# ======================================
# SCADA FLOW EDGE CONFIGURATION
# ======================================

# VPS SCADA FLOW SERVER
SERVER_URL = "https://scada.khze.org"

# EDGE DEVICE ID / default PLC ID
PLC_ID = 1

# Scheduler tick. Actual PLC tag intervals come from Flow.
SCHEDULER_TICK = 0.1

# How often Edge refreshes Flow configuration
FLOW_REFRESH_INTERVAL = 30

# Local calculated historian.
LOCAL_CALCULATED_DB = "edge_calculated.db"
CALCULATION_INTERVAL = 1.0

# Retention for local calculated data/aggregates.
CALCULATED_SAMPLE_RETENTION_DAYS = 2
CALCULATED_MINUTE_RETENTION_HOURS = 2
CALCULATED_HOUR_RETENTION_DAYS = 2
CALCULATED_DAY_RETENTION_DAYS = 600

# Store & Forward transport.
STORE_FORWARD_BATCH_SIZE = 100
STORE_FORWARD_SEND_TIMEOUT = 10.0
