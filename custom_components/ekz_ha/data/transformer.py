"""Transform API data to Home Assistant statistics format."""

import logging
from datetime import datetime

import zoneinfo

from ..api.models import ApiValue

_LOGGER = logging.getLogger(__name__)

UTC = zoneinfo.ZoneInfo("UTC")


class StatisticsTransformer:
    """Transforms measurement values to Home Assistant statistics."""

    @staticmethod
    def values_to_statistics(
        values: list[ApiValue], running_sum_offset: float = 0.0
    ) -> list[dict]:
        """
        Convert ApiValue list to Home Assistant statistics format.

        Each statistic entry has:
        - start: UTC datetime
        - sum: Running sum (cumulative kWh)
        - state: Value for this period (kWh)

        Args:
            values: List of ApiValue objects (should be hourly or daily aggregated)
            running_sum_offset: Initial offset for running sum (from database)

        Returns:
            List of statistics dictionaries
        """
        statistics = []
        running_sum = running_sum_offset

        for i, value in enumerate(values):
            # Parse timestamp
            ts_str = str(value.timestamp)
            stat_dt_naive = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
            stat_dt = stat_dt_naive.replace(tzinfo=UTC)

            # Update running sum
            running_sum += value.value

            statistics.append(
                {
                    "start": stat_dt,
                    "sum": running_sum,
                    "state": value.value,
                }
            )

            # Log first few for debugging
            if i < 3:
                _LOGGER.debug(
                    "[StatisticsTransformer] Sample stat %d: date=%s, value=%.3f kWh, sum=%.3f kWh",
                    i,
                    value.date,
                    value.value,
                    running_sum,
                )

        _LOGGER.debug(
            "[StatisticsTransformer] Created %d statistics entries", len(statistics)
        )
        return statistics

    @staticmethod
    def get_last_import_date(statistics: list[dict]) -> datetime | None:
        """
        Extract the latest timestamp from statistics.

        Args:
            statistics: List of statistics dictionaries

        Returns:
            Latest datetime or None if empty
        """
        if not statistics:
            return None

        return max(stat["start"] for stat in statistics)
