"""Utils relating to Time."""

import datetime


def format_api_date(dt: datetime.date) -> str:
    """Format date as YYYY-MM-DD, i.e. what the API expects."""
    return dt.strftime("%Y-%m-%d")


def parse_api_timestamp(timestamp: int) -> datetime.datetime:
    """Parse UTC timestamp from API."""
    return datetime.datetime.strptime(str(timestamp), "%Y%m%d%H%M%S")
