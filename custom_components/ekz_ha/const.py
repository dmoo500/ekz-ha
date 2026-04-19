"""Constants for EKZ integration."""

from datetime import timedelta

DOMAIN = "ekz_ha"

DEFAULT_SCAN_INTERVAL = timedelta(minutes=20)
CATCHUP_SCAN_INTERVAL = timedelta(minutes=5)
NORMAL_SCAN_INTERVAL = timedelta(hours=4)
