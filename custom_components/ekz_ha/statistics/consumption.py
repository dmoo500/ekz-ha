"""Consumption data importer."""

import logging
from datetime import datetime, timedelta

import zoneinfo

from ..api.models import ConsumptionData
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

        # If no data and this is an older period (> 30 days ago), try daily data
        if data.is_empty():
            recent_threshold = datetime.now(tz=ZRH) - timedelta(days=30)
            if date_from >= recent_threshold:
                # Recent period with no data likely means session error
                _LOGGER.info(
                    "[Consumption] No 15-min data for recent period %s (likely session error) - skipping daily fallback",
                    date_from.date(),
                )
                return data

            _LOGGER.info(
                "[Consumption] No 15-min data for %s, trying daily data",
                date_from.date(),
            )
            data = await self.api.get_consumption_daily(
                installation_id,
                date_from.strftime("%Y-%m-%d"),
                date_to.strftime("%Y-%m-%d"),
            )

        return data
