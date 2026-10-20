"""Consumption data importer."""

import logging
import zoneinfo
from dataclasses import replace
from datetime import datetime

from ..api.models import ConsumptionData, SeriesData
from .importer import BaseImporter

_LOGGER = logging.getLogger(__name__)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")


class ConsumptionImporter(BaseImporter):
    """Imports electricity consumption data."""

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

        First tries 15-minute data, falls back to daily data for older periods.

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

        available_days = {
            value.date
            for value in data.get_all_values()
            if date_from.date().isoformat() <= value.date < date_to.date().isoformat()
        }
        requested_days = (date_to.date() - date_from.date()).days
        if len(available_days) >= requested_days:
            return data

        daily = await self.api.get_consumption_daily(
            installation_id,
            date_from.strftime("%Y-%m-%d"),
            date_to.strftime("%Y-%m-%d"),
        )
        if data.is_empty():
            return daily
        fallback_values = [
            replace(value, status="DAILY_VALID")
            for value in self.aggregator.merge_tariffs(daily.get_all_values())
            if date_from.date().isoformat() <= value.date < date_to.date().isoformat()
            and value.date not in available_days
        ]
        return ConsumptionData(
            series_total=SeriesData(
                values=self.aggregator.merge_tariffs(data.get_all_values()) + fallback_values,
                level=data.level,
            ),
            level=data.level,
        )
