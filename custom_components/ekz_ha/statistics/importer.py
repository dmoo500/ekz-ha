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
from ..import_status import get_import_status, publish_import_status

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")
DEFAULT_IMPORT_CHUNK_DAYS = 30


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
        date_from: datetime,
        date_to: datetime,
    ) -> tuple[list[ApiValue], bool]:
        if hass is None:
            return values, bool(values)
        preserved = []
        accepted_data = False
        statistic_id = (
            f"sensor.electricity_{self.get_data_type_name().lower()}_ekz_{installation_id}"
        )
        cached = getattr(meta_entity, "_recent_statistics", [])
        daily_dates: set[str] = getattr(meta_entity, "_daily_statistics", set())
        returned_days = [day_bounds(day) for day in {value.date for value in values}]
        lookup_start = min([date_from] + [start for start, _ in returned_days])
        lookup_end = max([date_to] + [end for _, end in returned_days])
        recorded = await self._load_day_statistics(
            hass, statistic_id, lookup_start, lookup_end, cached
        )
        for hour, slots in getattr(meta_entity, "_hourly_slots", {}).items():
            timestamp = datetime.strptime(hour, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            if lookup_start <= timestamp < lookup_end and slots:
                recorded.setdefault(
                    timestamp, {"start": timestamp, "state": sum(slots.values()), "slots": slots}
                )
        recorded_days: dict[str, dict[datetime, dict[str, Any]]] = {}
        for timestamp, row in recorded.items():
            day = timestamp.astimezone(ZRH).date().isoformat()
            recorded_days.setdefault(day, {})[timestamp] = row
        by_day: dict[str, list[ApiValue]] = {}
        for value in values:
            by_day.setdefault(value.date, []).append(value)
        for day in recorded_days:
            by_day.setdefault(day, [])
        for day in daily_dates:
            if (
                lookup_start.astimezone(ZRH).date().isoformat()
                <= day
                < lookup_end.astimezone(ZRH).date().isoformat()
            ):
                by_day.setdefault(day, [])
        for day, day_values in sorted(by_day.items()):
            known_daily = day in daily_dates
            rows = recorded_days.get(day, {})
            if not day_values:
                template = ApiValue("", 0.0, "VALID", day, origin=ValueOrigin.RECORDED)
                if known_daily:
                    preserved.extend(
                        self.reconciler.restore_daily_values(template, rows, statistic_id)
                    )
                else:
                    preserved.extend(
                        self.reconciler.reconcile_hourly_values(
                            [], rows, statistic_id, getattr(meta_entity, "_hourly_slots", {})
                        )
                    )
                continue
            value = day_values[0]
            incoming_daily = value.origin is ValueOrigin.DAILY_TOTAL
            if not incoming_daily and not known_daily:
                reconciled = self.reconciler.reconcile_hourly_values(
                    day_values, rows, statistic_id, getattr(meta_entity, "_hourly_slots", {})
                )
                incoming_timestamps = {item.timestamp for item in day_values}
                accepted_data |= any(
                    item.origin is ValueOrigin.API and item.timestamp in incoming_timestamps
                    for item in reconciled
                )
                preserved.extend(reconciled)
                continue
            start, end = day_bounds(day)
            if not incoming_daily:
                expected = {slot.strftime("%Y%m%d%H%M%S") for slot in day_slots(day)}
                if level == "QUARTER_HOUR" and {item.timestamp for item in day_values} == expected:
                    preserved.extend(day_values)
                    accepted_data = True
                    continue
            if not incoming_daily:
                preserved.extend(self.reconciler.restore_daily_values(value, rows, statistic_id))
                continue
            if not rows:
                preserved.extend(day_values)
                accepted_data = True
                continue
            if (
                len(rows) == 1
                and start in rows
                and (known_daily or rows[start].get("end", end) == end)
            ):
                preserved.append(value)
                accepted_data = True
                continue
            preserved.extend(self.reconciler.reconcile_daily_total(value, rows, statistic_id))
            accepted_data = True
        return sorted(preserved, key=lambda value: value.timestamp), accepted_data

    def calculate_date_range(
        self,
        last_import: date | None,
        contract_start: date,
        max_days: int = DEFAULT_IMPORT_CHUNK_DAYS,
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

        # Fetch up to max_days or tomorrow, whichever is sooner
        tomorrow = datetime.now(tz=ZRH).date() + timedelta(days=1)
        tomorrow_dt = datetime.combine(tomorrow, datetime.min.time(), tzinfo=ZRH)
        to_date = min(from_date + timedelta(days=max_days), tomorrow_dt)

        return from_date, to_date

    @staticmethod
    def _as_local_date(value: ApiValue) -> date:
        timestamp = datetime.strptime(value.timestamp, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        return timestamp.astimezone(ZRH).date()

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

        # Search progressively farther from the contract start until the first
        # available data is found, then resume normal 30-day imports.
        last_import = get_import_start(meta_entity)
        contract_start_date = (
            contract_start.date() if isinstance(contract_start, datetime) else contract_start
        )
        stored_search_days = getattr(meta_entity, "_empty_search_days", None)
        search_days: int | None = (
            stored_search_days if isinstance(stored_search_days, int) else None
        )
        if (
            search_days is None
            and meta_entity is not None
            and getattr(meta_entity, "_history_cursor", None) is None
            and last_import in (None, contract_start_date)
        ):
            search_days = DEFAULT_IMPORT_CHUNK_DAYS
        searching_history = search_days is not None
        if search_days is not None:
            from_date, to_date = self.calculate_date_range(
                None, contract_start_date, max_days=search_days
            )
        else:
            from_date, to_date = self.calculate_date_range(
                last_import, contract_start_date, max_days=DEFAULT_IMPORT_CHUNK_DAYS
            )

        _LOGGER.debug("[%s] Fetching data: period %s to %s", data_type, from_date, to_date)

        # Fetch data from API
        if meta_entity is not None:
            get_import_status(meta_entity).begin(from_date, to_date, searching_history)
            publish_import_status(meta_entity)
        consumption_data = await self.fetch_data(installation_id, from_date, to_date)

        api_values = consumption_data.get_import_values()
        if search_days is not None and search_days <= DEFAULT_IMPORT_CHUNK_DAYS and api_values:
            search_days = None
            searching_history = False
            if meta_entity is not None:
                meta_entity._empty_search_days = None
        if search_days is not None and search_days > DEFAULT_IMPORT_CHUNK_DAYS and api_values:
            earliest_data_date = min(self._as_local_date(value) for value in api_values)
            bounded_from, bounded_to = self.calculate_date_range(
                earliest_data_date, earliest_data_date, max_days=DEFAULT_IMPORT_CHUNK_DAYS
            )
            if meta_entity is not None:
                get_import_status(meta_entity).begin(bounded_from, bounded_to, True)
                publish_import_status(meta_entity)
            bounded_data = await self.fetch_data(installation_id, bounded_from, bounded_to)
            bounded_values = bounded_data.get_import_values()
            if bounded_values:
                consumption_data = bounded_data
                from_date, to_date = bounded_from, bounded_to
                api_values = bounded_values
                search_days = None
                searching_history = False
                if meta_entity is not None:
                    meta_entity._empty_search_days = None
            else:
                consumption_data = bounded_data
                api_values = []

        if (
            searching_history
            and search_days is not None
            and not api_values
            and meta_entity is not None
        ):
            tomorrow = datetime.now(tz=ZRH).date() + timedelta(days=1)
            days_remaining = (tomorrow - contract_start_date).days
            if to_date.date() >= tomorrow:
                meta_entity._empty_search_days = None
            else:
                meta_entity._empty_search_days = min(search_days * 2, days_remaining)

        if consumption_data.level == "DAY":
            api_values = [replace(value, origin=ValueOrigin.DAILY_TOTAL) for value in api_values]
        merged_values = self.aggregator.merge_tariffs(api_values)
        merged_values, fetched_data = await self._reconcile_day_resolution(
            hass,
            installation_id,
            merged_values,
            consumption_data.level,
            meta_entity,
            from_date,
            to_date,
        )

        if meta_entity and not (searching_history and meta_entity._empty_search_days is not None):
            recent_boundary = datetime.now(tz=ZRH).date() - timedelta(days=30)
            history_cursor = min(to_date.date() - timedelta(days=1), recent_boundary)
            if history_cursor > from_date.date():
                meta_entity.set_history_cursor(history_cursor)
            pending = getattr(meta_entity, "_pending_from", None)
            if pending is not None and pending < recent_boundary:
                meta_entity.set_pending(None)

        if not merged_values:
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
                "fetched_data": fetched_data,
                "last_import": None,
                "from_date": from_date.date(),
                "to_date": to_date.date(),
            }

        # Get all values and merge tariffs (HT + NT)
        _LOGGER.debug("[%s] After merging tariffs: %d", data_type, len(merged_values))

        # Keep raw merged values for prediction accumulation
        raw_values_for_prediction = [
            value for value in merged_values if value.origin is ValueOrigin.API
        ]

        # Aggregate to hourly
        values = self.aggregator.aggregate_to_hourly(merged_values)

        # Convert to statistics
        statistics = self.transformer.values_to_statistics(values, running_sum_offset)
        hourly_slots: dict[str, dict[str, float]] = {}
        for value in raw_values_for_prediction:
            hour_key = value.timestamp[:10] + "0000"
            hourly_slots.setdefault(hour_key, {})[value.timestamp] = value.value
        for statistic, value in zip(statistics, values, strict=True):
            if value.timestamp in hourly_slots:
                statistic["slots"] = hourly_slots[value.timestamp]
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
            "fetched_data": fetched_data,
            "last_import": last_import_dt.astimezone(ZRH).date() if last_import_dt else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
            "raw_values": raw_values_for_prediction,  # For prediction accumulation
        }
