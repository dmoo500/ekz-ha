"""Prediction service for gap-filling future consumption based on historical averages."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from custom_components.ekz_ha.api.models import ApiValue


class PredictionService:
    """Service to calculate consumption predictions based on historical averages.

    EKZ delivers consumption data with a delay of ~7-10 days. To fill this gap
    in Home Assistant's Energy Dashboard, we calculate predictions using historical
    hourly consumption averages grouped by month and hour.

    The service accumulates raw slot data (month*100+hour_utc -> sum, count)
    and converts it to hourly averages (kWh/hour) for prediction generation.
    """

    def __init__(self) -> None:
        """Initialize prediction service with empty accumulator."""
        self._hourly_raw: dict[int, tuple[float, int]] = {}
        self._hourly_averages: dict[int, float] = {}

    def accumulate_values(self, values: list[ApiValue]) -> None:
        """Accumulate 15-minute slot data for average calculation.

        Args:
            values: List of ApiValue objects with timestamp and value (kWh)
        """
        for value in values:
            # Extract month and UTC hour from timestamp (YYYYMMDDHHmmss)
            ts_str = str(value.timestamp)
            if len(ts_str) < 10:
                continue

            month = int(ts_str[4:6])  # MM
            hour_utc = int(ts_str[8:10])  # HH

            # Create bucket key: month * 100 + hour_utc
            # Example: March 14:00 UTC = 3 * 100 + 14 = 314
            bucket_key = month * 100 + hour_utc

            # Accumulate sum and count
            current_sum, current_count = self._hourly_raw.get(bucket_key, (0.0, 0))
            self._hourly_raw[bucket_key] = (
                current_sum + value.value,
                current_count + 1,
            )

    def calculate_averages(self) -> dict[int, float]:
        """Calculate hourly averages from accumulated raw data.

        Returns:
            Dictionary mapping bucket keys (month*100+hour) to average kWh/hour.
            Only includes buckets with at least 4 slots (1 complete hour).
        """
        self._hourly_averages = {
            key: total_kwh / (count_slots / 4)
            for key, (total_kwh, count_slots) in self._hourly_raw.items()
            if count_slots >= 4  # Require at least 1 complete hour
        }
        return self._hourly_averages

    def get_averages(self) -> dict[int, float]:
        """Get current hourly averages.

        Returns:
            Dictionary mapping bucket keys to average kWh/hour.
        """
        return self._hourly_averages

    def get_raw_accumulator(self) -> dict[int, tuple[float, int]]:
        """Get raw accumulator for persistence/debugging.

        Returns:
            Dictionary mapping bucket keys to (sum_kwh, count_slots) tuples.
        """
        return self._hourly_raw.copy()

    def merge_raw_accumulator(self, new_raw: dict[int, tuple[float, int]]) -> None:
        """Merge new raw data into existing accumulator.

        Args:
            new_raw: New raw accumulator data to merge
        """
        for key, (new_sum, new_count) in new_raw.items():
            existing_sum, existing_count = self._hourly_raw.get(key, (0.0, 0))
            self._hourly_raw[key] = (
                existing_sum + new_sum,
                existing_count + new_count,
            )

    def generate_predictions(
        self,
        last_real_datetime: datetime,
        target_datetime: datetime,
    ) -> list[dict]:
        """Generate prediction statistics from last real data to target time.

        Generates hourly predictions using historical averages. Each prediction
        is a statistics entry compatible with Home Assistant's statistics API.

        Args:
            last_real_datetime: Datetime of last real data point (timezone-aware)
            target_datetime: Target datetime to predict up to (timezone-aware)

        Returns:
            List of statistics entries with 'start', 'sum', and 'state' keys.
            Empty list if no averages available or time range invalid.
        """
        if not self._hourly_averages:
            return []

        # Start predictions at next full hour after last real data
        pred_start = last_real_datetime + timedelta(hours=1)
        # Normalize to top of the hour
        pred_start = pred_start.replace(minute=0, second=0, microsecond=0)

        if pred_start >= target_datetime:
            return []

        predictions = []
        running_total = 0.0

        while pred_start < target_datetime:
            # Calculate bucket key for this hour
            bucket_key = pred_start.month * 100 + pred_start.hour

            # Look up average for this month+hour combination
            hourly_kwh = self._hourly_averages.get(bucket_key, 0.0)
            running_total += hourly_kwh

            predictions.append(
                {
                    "start": pred_start,
                    "sum": running_total,
                    "state": hourly_kwh,
                }
            )

            pred_start = pred_start + timedelta(hours=1)

        return predictions

    def reset(self) -> None:
        """Reset all accumulated data and averages."""
        self._hourly_raw.clear()
        self._hourly_averages.clear()
