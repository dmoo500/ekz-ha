"""Base importer for statistics."""

import logging
import zoneinfo
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

from ..api.client import EkzApiClient
from ..api.models import ConsumptionData
from ..data import DataAggregator, DataValidator, StatisticsTransformer

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")


class BaseImporter(ABC):
    """Base class for consumption/production data importers."""

    def __init__(self, api_client: EkzApiClient):
        """Initialize the importer."""
        self.api = api_client
        self.aggregator = DataAggregator()
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

    def calculate_date_range(
        self, last_import: datetime | None, contract_start: datetime, max_days: int = 30
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
            # Continue from day after last import
            from_date = last_import + timedelta(days=1)
        else:
            # Start from contract begin
            from_date = contract_start

        # Fetch up to max_days or tomorrow, whichever is sooner
        tomorrow = datetime.now(tz=ZRH).date() + timedelta(days=1)
        tomorrow_dt = datetime.combine(tomorrow, datetime.min.time())
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
        last_import = meta_entity._last_import if meta_entity else None
        from_date, to_date = self.calculate_date_range(last_import, contract_start, max_days=30)

        _LOGGER.debug("[%s] Fetching data: period %s to %s", data_type, from_date, to_date)

        # Fetch data from API
        consumption_data = await self.fetch_data(installation_id, from_date, to_date)

        if consumption_data.is_empty():
            _LOGGER.info(
                "[%s] No data for installation %s, period %s to %s",
                data_type,
                installation_id,
                from_date,
                to_date,
            )
            if meta_entity:
                if to_date.date() < datetime.now(tz=ZRH).date():
                    meta_entity.set_last_import(to_date.date())
                meta_entity.set_last_run_date(datetime.now(tz=ZRH))
            return {
                "statistics": [],
                "last_import": None,
                "from_date": from_date.date(),
                "to_date": to_date.date(),
            }

        # Get all values and merge tariffs (HT + NT)
        values = consumption_data.get_all_values()
        _LOGGER.debug("[%s] Raw values from API: %d", data_type, len(values))

        values = self.aggregator.merge_tariffs(values)
        _LOGGER.debug("[%s] After merging tariffs: %d", data_type, len(values))

        # Aggregate to hourly
        if consumption_data.level == "QUARTER_HOUR":
            values = self.aggregator.aggregate_to_hourly(values)
        elif consumption_data.level == "DAY":
            # Daily data is already at the right granularity, but we still
            # aggregate to ensure consistent format
            values = self.aggregator.aggregate_to_daily(values)

        # Convert to statistics
        statistics = self.transformer.values_to_statistics(values, running_sum_offset)

        # Update metadata
        if meta_entity and statistics:
            last_import_dt = self.transformer.get_last_import_date(statistics)
            if last_import_dt:
                meta_entity.set_last_import(last_import_dt.astimezone(ZRH).date())
            meta_entity.set_last_run_date(datetime.now(tz=ZRH))

        return {
            "statistics": statistics,
            "last_import": (
                self.transformer.get_last_import_date(statistics).date() if statistics else None
            ),
            "from_date": from_date.date(),
            "to_date": to_date.date(),
        }
