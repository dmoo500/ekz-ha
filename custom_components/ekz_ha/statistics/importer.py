"""Base importer for statistics."""

import logging
import zoneinfo
from abc import ABC, abstractmethod
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import Any

from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.helpers.recorder import get_instance as get_recorder_instance

from ..api.client import EkzApiClient
from ..api.models import ApiValue, ConsumptionData, ValueOrigin
from ..data import DataAggregator, DataReconciler, DataValidator, StatisticsTransformer
from ..data.validator import day_bounds, day_slots

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")
MAX_HISTORY_YEARS = 10


def get_import_start(meta_entity: Any) -> date | None:
    if meta_entity is None:
        return None
    pending: date | None = getattr(meta_entity, "_pending_from", None)
    if pending is not None:
        return pending
    candidates: list[date] = [
        value.date() if isinstance(value, datetime) else value
        for value in (
            meta_entity._last_import,
            getattr(meta_entity, "_history_cursor", None),
        )
        if value is not None
    ]
    return max(candidates) if candidates else None


class BaseImporter(ABC):
    """Base class for consumption/production data importers."""

    def __init__(self, api_client: EkzApiClient):
        """Initialize the importer."""
        self.api = api_client
        self.aggregator = DataAggregator()
        self.reconciler = DataReconciler()
        self.validator = DataValidator()
        self.transformer = StatisticsTransformer()

    @abstractmethod
    async def fetch_data(
        self,
        installation_id: str,
        date_from: datetime,
        date_to: datetime,
    ) -> ConsumptionData:
        """
        Fetch data from the API.

        Must be implemented by subclasses.

        Args:
            installation_id: Installation ID
            date_from: Start date
            date_to: End date

        Returns:
            ConsumptionData object
        """
        pass

    @abstractmethod
    def get_data_type_name(self) -> str:
        """Get the name of this data type for logging."""
        pass

    async def _load_day_statistics(
        self,
        hass: Any,
        statistic_id: str,
        start: datetime,
        end: datetime,
        cached: list[dict[str, Any]],
    ) -> dict[datetime, dict[str, Any]]:
        recorded = await get_recorder_instance(hass).async_add_executor_job(
            statistics_during_period,
            hass,
            start,
            end,
            {statistic_id},
            "hour",
            None,
            {"state", "sum"},
        )
        rows = {}
        for row in [*(recorded or {}).get(statistic_id, []), *cached]:
            raw_start = row["start"]
            timestamp = (
                datetime.fromtimestamp(raw_start, tz=UTC)
                if isinstance(raw_start, (int, float))
                else raw_start.astimezone(UTC)
            )
            if start <= timestamp < end:
                rows[timestamp] = row
        return rows

    async def _reconcile_day_resolution(
        self,
        hass: Any,
        installation_id: str,
        values: list[ApiValue],
        level: str,
        meta_entity: Any,
    ) -> list[ApiValue]:
        if hass is None:
            return values
        preserved = []
        statistic_id = (
            f"sensor.electricity_{self.get_data_type_name().lower()}_ekz_{installation_id}"
        )
        cached = getattr(meta_entity, "_recent_statistics", [])
        daily_dates: set[str] = getattr(meta_entity, "_daily_statistics", set())
        by_day: dict[str, list[ApiValue]] = {}
        for value in values:
            by_day.setdefault(value.date, []).append(value)
        for day, day_values in sorted(by_day.items()):
            value = day_values[0]
            incoming_daily = value.origin is ValueOrigin.DAILY_TOTAL
            known_daily = day in daily_dates
            if not incoming_daily and not known_daily:
                preserved.extend(day_values)
                continue
            start, end = day_bounds(day)
            if not incoming_daily:
                expected = {slot.strftime("%Y%m%d%H%M%S") for slot in day_slots(day)}
                if level == "QUARTER_HOUR" and {item.timestamp for item in day_values} == expected:
                    preserved.extend(day_values)
                    continue
            rows = await self._load_day_statistics(hass, statistic_id, start, end, cached)
            if not incoming_daily:
                preserved.extend(self.reconciler.restore_daily_values(value, rows, statistic_id))
                continue
            if not rows:
                preserved.extend(day_values)
                continue
            if (
                len(rows) == 1
                and start in rows
                and (known_daily or rows[start].get("end", end) == end)
            ):
                preserved.append(value)
                continue
            preserved.extend(self.reconciler.reconcile_daily_total(value, rows, statistic_id))
        return sorted(preserved, key=lambda value: value.timestamp)

    def calculate_date_range(
        self, last_import: date | None, contract_start: date, max_days: int = 30
    ) -> tuple[datetime, datetime]:
        """
        Calculate the date range to fetch.

        Args:
            last_import: Last successful import date (None for first import)
            contract_start: Contract start date
            max_days: Maximum number of days to fetch in one request

        Returns:
            Tuple of (from_date, to_date)
        """
        if last_import:
            from_date = last_import
        else:
            # Start from contract begin
            from_date = contract_start

        # Convert date to datetime if needed
        if not isinstance(from_date, datetime):
            from_date = datetime.combine(from_date, datetime.min.time(), tzinfo=ZRH)
        # Ensure from_date is timezone-aware
        elif from_date.tzinfo is None:
            from_date = from_date.replace(tzinfo=ZRH)

        today = datetime.now(tz=ZRH).date()
        try:
            history_start_date = today.replace(year=today.year - MAX_HISTORY_YEARS)
        except ValueError:
            history_start_date = today.replace(year=today.year - MAX_HISTORY_YEARS, day=28)
        history_start = datetime.combine(history_start_date, datetime.min.time(), tzinfo=ZRH)
        from_date = max(from_date, history_start)

        # Fetch up to max_days or tomorrow, whichever is sooner
        tomorrow = datetime.now(tz=ZRH).date() + timedelta(days=1)
        tomorrow_dt = datetime.combine(tomorrow, datetime.min.time(), tzinfo=ZRH)
        to_date = min(from_date + timedelta(days=max_days), tomorrow_dt)

        return from_date, to_date

    async def import_statistics(
        self,
        hass: Any,
        installation_id: str,
        contract_start: datetime,
        meta_entity: Any = None,
        running_sum_offset: float = 0.0,
    ) -> dict:
        """
        Import statistics for an installation.

        Args:
            hass: Home Assistant instance
            installation_id: Installation ID
            contract_start: Contract start date
            meta_entity: Optional metadata entity for tracking
            running_sum_offset: Initial running sum offset from database

        Returns:
            Dictionary with import results
        """
        data_type = self.get_data_type_name()
        _LOGGER.debug(
            "[%s] Starting import: installation=%s, contract_start=%s",
            data_type,
            installation_id,
            contract_start,
        )

        # Determine date range
        last_import = get_import_start(meta_entity)
        from_date, to_date = self.calculate_date_range(last_import, contract_start, max_days=30)

        _LOGGER.debug("[%s] Fetching data: period %s to %s", data_type, from_date, to_date)

        # Fetch data from API
        consumption_data = await self.fetch_data(installation_id, from_date, to_date)

        api_values = consumption_data.get_all_values()
        if consumption_data.level == "DAY":
            api_values = [replace(value, origin=ValueOrigin.DAILY_TOTAL) for value in api_values]
        merged_values = self.aggregator.merge_tariffs(api_values)
        merged_values = await self._reconcile_day_resolution(
            hass, installation_id, merged_values, consumption_data.level, meta_entity
        )

        if meta_entity:
            recent_boundary = datetime.now(tz=ZRH).date() - timedelta(days=30)
            history_cursor = min(to_date.date() - timedelta(days=1), recent_boundary)
            if history_cursor > from_date.date():
                meta_entity.set_history_cursor(history_cursor)
            pending = getattr(meta_entity, "_pending_from", None)
            if pending is not None and pending < recent_boundary:
                meta_entity.set_pending(None)

        if consumption_data.is_empty():
            _LOGGER.info(
                "[%s] No data for installation %s, period %s to %s",
                data_type,
                installation_id,
                from_date,
                to_date,
            )
            if meta_entity:
                meta_entity.set_last_run_date(datetime.now(tz=ZRH))
            return {
                "statistics": [],
                "last_import": None,
                "from_date": from_date.date(),
                "to_date": to_date.date(),
            }

        # Get all values and merge tariffs (HT + NT)
        _LOGGER.debug("[%s] After merging tariffs: %d", data_type, len(merged_values))

        # Keep raw merged values for prediction accumulation
        raw_values_for_prediction = (
            [value for value in merged_values if value.origin is ValueOrigin.API]
            if consumption_data.level == "QUARTER_HOUR"
            else []
        )

        # Aggregate to hourly
        if consumption_data.level == "QUARTER_HOUR":
            values = self.aggregator.aggregate_to_hourly(merged_values)
        elif consumption_data.level == "DAY":
            values = merged_values
        else:
            values = merged_values

        # Convert to statistics
        statistics = self.transformer.values_to_statistics(values, running_sum_offset)
        for statistic, value in zip(statistics, values, strict=True):
            if value.origin is ValueOrigin.DAILY_TOTAL:
                _, statistic["end"] = day_bounds(value.date)
            else:
                statistic["end"] = statistic["start"] + timedelta(hours=1)
            statistic["source_resolution"] = "day" if value.origin.is_daily else "hour"

        # Update metadata
        last_import_dt = self.transformer.get_last_import_date(statistics)
        if meta_entity and statistics:
            if last_import_dt:
                meta_entity.set_last_import(last_import_dt.astimezone(ZRH).date())
                complete_days = set(self.validator.find_complete_days(raw_values_for_prediction))
                complete_days.update(value.date for value in merged_values if value.origin.is_daily)
                pending_day = max(
                    from_date.date(), datetime.now(tz=ZRH).date() - timedelta(days=30)
                )
                end_day = min(
                    to_date.date(),
                    last_import_dt.astimezone(ZRH).date() + timedelta(days=1),
                    datetime.now(tz=ZRH).date(),
                )
                while pending_day < end_day and pending_day.isoformat() in complete_days:
                    pending_day += timedelta(days=1)
                if pending_day < end_day:
                    boundary = datetime.combine(pending_day, datetime.min.time(), tzinfo=ZRH)
                    preceding = [
                        statistic for statistic in statistics if statistic["start"] < boundary
                    ]
                    offset = preceding[-1]["sum"] if preceding else running_sum_offset
                    meta_entity.set_pending(pending_day, offset)
                else:
                    meta_entity.set_pending(None)
            meta_entity.set_last_run_date(datetime.now(tz=ZRH))

        return {
            "statistics": statistics,
            "last_import": last_import_dt.astimezone(ZRH).date() if last_import_dt else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
            "raw_values": raw_values_for_prediction,  # For prediction accumulation
        }
