"""Data aggregation utilities."""

import itertools
import logging
from datetime import datetime, timedelta
from typing import Callable

import zoneinfo

from ..api.models import ApiValue

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")


def normalize_timestamp(ts: str) -> str:
    """
    Normalize a timestamp to 14-digit format (YYYYMMDDHHmmss).

    Args:
        ts: Timestamp in various formats

    Returns:
        Normalized 14-character timestamp string
    """
    s = str(ts).replace("-", "").replace("T", "").replace(":", "").replace(" ", "")
    return s[:14].ljust(14, "0")


class DataAggregator:
    """Handles aggregation of measurement values."""

    @staticmethod
    def merge_tariffs(values: list[ApiValue]) -> list[ApiValue]:
        """
        Merge HT and NT values for the same timestamp.

        When both HT (high tariff) and NT (low tariff) values exist for the same
        timestamp, they are summed into a single entry.

        Args:
            values: List of ApiValue objects

        Returns:
            List of merged ApiValue objects
        """
        merged = []
        for ts, group in itertools.groupby(
            sorted(values, key=lambda v: v.timestamp), lambda v: v.timestamp
        ):
            group_list = list(group)
            if len(group_list) == 1:
                merged.append(group_list[0])
            else:
                # Sum values for same timestamp (HT + NT)
                total_value = sum(v.value for v in group_list)
                first = group_list[0]
                merged.append(
                    ApiValue(
                        timestamp=first.timestamp,
                        value=total_value,
                        status=first.status,
                        date=first.date,
                        tariff="TOTAL",
                    )
                )
        return merged

    @staticmethod
    def aggregate_to_hourly(values: list[ApiValue]) -> list[ApiValue]:
        """
        Aggregate 15-minute values to hourly buckets.

        Home Assistant's statistics API requires timestamps at the top of the hour.
        This aggregates 4 x 15-minute slots into hourly buckets.

        Args:
            values: List of ApiValue objects (15-minute resolution)

        Returns:
            List of hourly aggregated ApiValue objects
        """
        hourly = []

        def hour_key(v: ApiValue) -> str:
            """Extract hour key (YYYYMMDDHH) from timestamp."""
            ts_norm = normalize_timestamp(v.timestamp)
            return ts_norm[:10]

        for hour_str, group in itertools.groupby(
            sorted(values, key=hour_key), hour_key
        ):
            group_list = list(group)
            total_value = sum(v.value for v in group_list)
            first = group_list[0]

            # Create timestamp at top of hour
            hour_ts = normalize_timestamp(hour_str + "0000")

            hourly.append(
                ApiValue(
                    timestamp=hour_ts,
                    value=total_value,
                    status=first.status,
                    date=first.date,
                    tariff=first.tariff,
                )
            )

        _LOGGER.debug(
            "[DataAggregator] Aggregated %d 15-min values to %d hourly values",
            len(values),
            len(hourly),
        )
        return hourly

    @staticmethod
    def aggregate_to_daily(values: list[ApiValue]) -> list[ApiValue]:
        """
        Aggregate values to daily buckets.

        Args:
            values: List of ApiValue objects

        Returns:
            List of daily aggregated ApiValue objects
        """
        daily = []

        for date_str, group in itertools.groupby(
            sorted(values, key=lambda v: v.date), lambda v: v.date
        ):
            group_list = list(group)
            total_value = sum(v.value for v in group_list)
            first = group_list[0]

            # Create timestamp at midnight
            day_ts = normalize_timestamp(date_str + "000000")

            daily.append(
                ApiValue(
                    timestamp=day_ts,
                    value=total_value,
                    status=first.status,
                    date=first.date,
                    tariff=first.tariff,
                )
            )

        _LOGGER.debug(
            "[DataAggregator] Aggregated %d values to %d daily values",
            len(values),
            len(daily),
        )
        return daily
