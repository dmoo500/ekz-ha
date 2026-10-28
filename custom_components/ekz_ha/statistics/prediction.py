"""Prediction service for gap-filling future consumption based on historical averages."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from ..data.aggregator import normalize_timestamp

_LOGGER = logging.getLogger(__name__)
# Cover the 30-day import lookback plus local/UTC day boundaries.
SLOT_CORRECTION_WINDOW = timedelta(days=32)

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
        self._slot_values: dict[str, float] = {}
        self._slot_cutoff: str | None = None
        self._seeded_hours: dict[str, float] = {}

    def accumulate_values(self, values: list[ApiValue]) -> None:
        """Accumulate 15-minute slot data for average calculation.

        Args:
            values: List of ApiValue objects with timestamp and value (kWh)

        Older contributions stay in the aggregate buckets. Only the latest
        32 days of individual slots are retained for deduplication/corrections;
        replays before that window are ignored.
        """
        frozen_slots = 0
        for value in sorted(values, key=lambda value: normalize_timestamp(value.timestamp)):
            # Extract month and UTC hour from timestamp (YYYYMMDDHHmmss)
            if len(str(value.timestamp)) < 10:
                continue
            ts_str = normalize_timestamp(value.timestamp)
            if self._slot_cutoff is not None and ts_str < self._slot_cutoff:
                frozen_slots += 1
                continue

            month = int(ts_str[4:6])  # MM
            hour_utc = int(ts_str[8:10])  # HH

            # Create bucket key: month * 100 + hour_utc
            # Example: March 14:00 UTC = 3 * 100 + 14 = 314
            bucket_key = month * 100 + hour_utc

            # Accumulate sum and count
            current_sum, current_count = self._hourly_raw.get(bucket_key, (0.0, 0))
            seeded = self._seeded_hours.pop(ts_str[:10] + "0000", None)
            if seeded is not None:
                current_sum -= seeded
                current_count -= 4
            previous_value = self._slot_values.get(ts_str)
            self._hourly_raw[bucket_key] = (
                current_sum + value.value - (previous_value or 0.0),
                current_count + (previous_value is None),
            )
            self._slot_values[ts_str] = value.value
        if self._slot_values:
            latest = datetime.strptime(max(self._slot_values), "%Y%m%d%H%M%S")
            cutoff = (latest - SLOT_CORRECTION_WINDOW).strftime("%Y%m%d%H%M%S")
            self._slot_cutoff = max(self._slot_cutoff or cutoff, cutoff)
            # Older contributions remain in the buckets, but cannot be counted again.
            self._slot_values = {
                timestamp: value
                for timestamp, value in self._slot_values.items()
                if timestamp >= self._slot_cutoff
            }
            self._seeded_hours = {
                timestamp: value
                for timestamp, value in self._seeded_hours.items()
                if timestamp >= self._slot_cutoff
            }
        if frozen_slots:
            _LOGGER.debug(
                "Ignored %d prediction slots before frozen-history cutoff %s",
                frozen_slots,
                self._slot_cutoff,
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
        initial_total: float = 0.0,
    ) -> list[dict]:
        """Generate prediction statistics from last real data to target time.

        Generates hourly predictions using historical averages. Each prediction
        is a statistics entry compatible with Home Assistant's statistics API.

        Args:
            last_real_datetime: Datetime of last real data point (timezone-aware)
            target_datetime: Target datetime to predict up to (timezone-aware)
            initial_total: Predicted consumption already accumulated in the last real hour.

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
        running_total = initial_total

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

    def estimate_missing_slots(self, hour_start: datetime, delivered_slots: int) -> float:
        """Estimate the missing quarter-hour consumption for a partially delivered hour."""
        missing_slots = 4 - delivered_slots
        if not 0 < missing_slots < 4:
            return 0.0
        bucket_key = hour_start.month * 100 + hour_start.hour
        return self._hourly_averages.get(bucket_key, 0.0) * missing_slots / 4

    def seed_hourly_values(self, values: list[tuple[datetime, float]]) -> None:
        """Rebuild missing history without double-counting later quarter-hour imports."""
        actual_hours = {slot[:10] for slot in self._slot_values}
        for start, energy in sorted(values):
            if not math.isfinite(energy) or energy < 0:
                continue
            timestamp = start.astimezone(UTC).strftime("%Y%m%d%H0000")
            if self._slot_cutoff is not None and timestamp < self._slot_cutoff:
                continue
            if timestamp in self._seeded_hours or timestamp[:10] in actual_hours:
                continue
            bucket = int(timestamp[4:6]) * 100 + int(timestamp[8:10])
            total, count = self._hourly_raw.get(bucket, (0.0, 0))
            self._hourly_raw[bucket] = (total + energy, count + 4)
            self._seeded_hours[timestamp] = energy
        if self._seeded_hours:
            latest = datetime.strptime(max(self._seeded_hours), "%Y%m%d%H%M%S")
            cutoff = (latest - SLOT_CORRECTION_WINDOW).strftime("%Y%m%d%H%M%S")
            self._slot_cutoff = max(self._slot_cutoff or cutoff, cutoff)
            self._seeded_hours = {
                timestamp: energy
                for timestamp, energy in self._seeded_hours.items()
                if timestamp >= self._slot_cutoff
            }
            self._slot_values = {
                timestamp: energy
                for timestamp, energy in self._slot_values.items()
                if timestamp >= self._slot_cutoff
            }
        self.calculate_averages()

    def to_dict(self) -> dict[str, Any]:
        """Serialize both aggregate history and recent correction bookkeeping."""
        return {
            "hourly_raw": {str(key): list(value) for key, value in self._hourly_raw.items()},
            "slot_values": dict(self._slot_values),
            "slot_cutoff": self._slot_cutoff,
            "seeded_hours": dict(self._seeded_hours),
        }

    def load_dict(self, data: dict[str, Any]) -> None:
        """Restore persisted learning state, rejecting invalid values."""
        for field in ("hourly_raw", "slot_values", "seeded_hours"):
            if not isinstance(data.get(field), dict):
                raise ValueError(f"Invalid prediction field: {field}")
        raw = {
            int(key): (float(total), int(count))
            for key, (total, count) in data["hourly_raw"].items()
        }
        slots = {str(key): float(value) for key, value in data["slot_values"].items()}
        seeded = {str(key): float(value) for key, value in data["seeded_hours"].items()}
        cutoff = data["slot_cutoff"]
        if any(
            not (1 <= key // 100 <= 12 and 0 <= key % 100 <= 23)
            or not math.isfinite(total)
            or count < 0
            for key, (total, count) in raw.items()
        ):
            raise ValueError("Invalid prediction accumulator")
        for timestamp, value in {**slots, **seeded}.items():
            datetime.strptime(timestamp, "%Y%m%d%H%M%S")
            if not math.isfinite(value):
                raise ValueError("Invalid prediction reading")
        if cutoff is not None:
            datetime.strptime(cutoff, "%Y%m%d%H%M%S")
        self._hourly_raw = raw
        self._slot_values = slots
        self._seeded_hours = seeded
        self._slot_cutoff = cutoff
        self.calculate_averages()

    def reset(self) -> None:
        """Reset all accumulated data and averages."""
        self._hourly_raw.clear()
        self._hourly_averages.clear()
        self._slot_values.clear()
        self._slot_cutoff = None
        self._seeded_hours.clear()
