"""Data validation utilities."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import zoneinfo

from ..api.models import ApiValue

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")


def is_dst(dt: datetime, timezone: zoneinfo.ZoneInfo) -> bool:
    """Determine whether the given date is during daylight savings or not."""
    aware_dt = dt.replace(tzinfo=timezone)
    return aware_dt.dst() != timedelta(0, 0)


def is_dst_switchover_date(dt: datetime, timezone: zoneinfo.ZoneInfo) -> bool:
    """Determine whether a day is the day on which daylight savings starts/ends."""
    day_after = dt + timedelta(days=1)
    return is_dst(day_after, timezone) != is_dst(dt, timezone)


class DataValidator:
    """Validates measurement data for completeness and consistency."""

    @staticmethod
    def count_slots_per_day(values: list[ApiValue]) -> dict[str, int]:
        """
        Count how many 15-minute slots exist for each day.

        Args:
            values: List of ApiValue objects

        Returns:
            Dictionary mapping date string to slot count
        """
        slot_counts: dict[str, int] = defaultdict(int)
        for value in values:
            slot_counts[value.date] += 1
        return dict(slot_counts)

    @staticmethod
    def expected_slots_for_date(date_str: str, timezone: zoneinfo.ZoneInfo) -> int:
        """
        Calculate expected number of 15-minute slots for a given date.

        Regular days have 96 slots (24h * 4 slots/h).
        DST spring forward: 92 slots (23h * 4 slots/h)
        DST fall back: 100 slots (25h * 4 slots/h)

        Args:
            date_str: Date in YYYY-MM-DD format
            timezone: Timezone to check for DST

        Returns:
            Expected number of 15-minute slots
        """
        date = datetime.strptime(date_str, "%Y-%m-%d")

        if is_dst_switchover_date(date, timezone):
            # Spring forward (March): 92 slots
            if date.month < 6:
                return 92
            # Fall back (October): 100 slots
            return 100

        # Regular day: 96 slots
        return 96

    @staticmethod
    def find_complete_days(
        values: list[ApiValue], timezone: zoneinfo.ZoneInfo = ZRH
    ) -> list[str]:
        """
        Find dates that have all expected 15-minute slots.

        Args:
            values: List of ApiValue objects
            timezone: Timezone for DST calculation

        Returns:
            List of date strings (YYYY-MM-DD) with complete data
        """
        slot_counts = DataValidator.count_slots_per_day(values)
        today_str = datetime.now(tz=timezone).strftime("%Y-%m-%d")

        complete_days = []
        for date_str, count in slot_counts.items():
            # Skip today (may be partial)
            if date_str >= today_str:
                continue

            expected = DataValidator.expected_slots_for_date(date_str, timezone)
            if count == expected:
                complete_days.append(date_str)

        _LOGGER.debug(
            "[DataValidator] Found %d complete days out of %d total days",
            len(complete_days),
            len(slot_counts),
        )
        return complete_days

    @staticmethod
    def get_latest_complete_date(
        values: list[ApiValue], timezone: zoneinfo.ZoneInfo = ZRH
    ) -> datetime | None:
        """
        Find the latest date with complete 15-minute data.

        Args:
            values: List of ApiValue objects
            timezone: Timezone for DST calculation

        Returns:
            Latest complete date or None if no complete days exist
        """
        complete_days = DataValidator.find_complete_days(values, timezone)
        if not complete_days:
            return None

        latest_str = max(complete_days)
        return datetime.strptime(latest_str, "%Y-%m-%d")
