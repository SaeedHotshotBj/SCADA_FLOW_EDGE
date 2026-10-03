"""SCADA Flow Edge timezone helpers.

Prefer the IANA Asia/Tehran timezone when the Windows tzdata package is
available. On current Iran dates (2023 onward), Tehran is fixed at UTC+03:30,
so the fallback keeps the Edge runnable even when tzdata is not installed.
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    TZ = ZoneInfo("Asia/Tehran")
except ZoneInfoNotFoundError:
    TZ = timezone(timedelta(hours=3, minutes=30))


def now_local():
    """Return the current Tehran-local aware datetime."""
    return datetime.now(TZ)


__all__ = ["TZ", "now_local"]
