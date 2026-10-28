"""Consumption data importer."""

import logging
import zoneinfo
from dataclasses import replace
from datetime import datetime

from ..api.client import EkzApiClient
from ..api.models import ConsumptionData, SeriesData, ValueOrigin
from .importer import BaseImporter

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")


class ConsumptionImporter(BaseImporter):
    """Imports electricity consumption data."""

    def __init__(
        self, api_client: EkzApiClient, daily_fallback_days: set[str] | None = None
    ) -> None:
        """Initialize with the dates currently represented by daily totals."""
        super().__init__(api_client)
        self.daily_fallback_days = set(daily_fallback_days or ())

    def get_data_type_name(self) -> str:
        """Get the name of this data type for logging."""
        return "Consumption"

    async def fetch_data(
        self,
        installation_id: str,
        date_from: datetime,
        date_to: datetime,
    ) -> ConsumptionData:
        """
        Fetch consumption data from the API.

        Fetch daily totals for missing days and incomplete known fallback days.

        Args:
            installation_id: Installation ID
            date_from: Start date
            date_to: End date

        Returns:
            ConsumptionData object
        """
        # Try 15-minute data first
        data = await self.api.get_consumption_15min(
            installation_id,
            date_from.strftime("%Y-%m-%d"),
            date_to.strftime("%Y-%m-%d"),
        )

        quarter_hour_values = self.aggregator.merge_tariffs(data.get_import_values())
        available_days = {
            value.date
            for value in quarter_hour_values
            if date_from.date().isoformat() <= value.date < date_to.date().isoformat()
        }
        complete_days = set(self.validator.find_complete_days(quarter_hour_values))
        refresh_days = {
            day
            for day in self.daily_fallback_days - complete_days
            if date_from.date().isoformat() <= day < date_to.date().isoformat()
        }
        requested_days = (date_to.date() - date_from.date()).days
        if len(available_days) >= requested_days and not refresh_days:
            return data

        daily = await self.api.get_consumption_daily(
            installation_id,
            date_from.strftime("%Y-%m-%d"),
            date_to.strftime("%Y-%m-%d"),
        )
        if data.is_empty():
            return daily
        fallback_values = [
            replace(value, origin=ValueOrigin.DAILY_TOTAL)
            for value in self.aggregator.merge_tariffs(daily.get_import_values())
            if date_from.date().isoformat() <= value.date < date_to.date().isoformat()
            and (value.date not in available_days or value.date in refresh_days)
        ]
        fallback_days = {value.date for value in fallback_values}
        return ConsumptionData(
            series_total=SeriesData(
                values=[value for value in quarter_hour_values if value.date not in fallback_days]
                + fallback_values,
                level=data.level,
            ),
            level=data.level,
        )
