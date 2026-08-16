"""Production (solar feed-in) data importer."""

import logging
from datetime import datetime

from ..api.models import ConsumptionData
from .importer import BaseImporter

_LOGGER = logging.getLogger(__name__)


class ProductionImporter(BaseImporter):
    """Imports solar production (feed-in) data."""

    def get_data_type_name(self) -> str:
        """Get the name of this data type for logging."""
        return "Production"

    async def fetch_data(
        self,
        installation_id: str,
        date_from: datetime,
        date_to: datetime,
    ) -> ConsumptionData:
        """
        Fetch production data from the API.

        Production data is typically only available as 15-minute data.

        Args:
            installation_id: Installation ID
            date_from: Start date
            date_to: End date

        Returns:
            ConsumptionData object
        """
        return await self.api.get_production_15min(
            installation_id,
            date_from.strftime("%Y-%m-%d"),
            date_to.strftime("%Y-%m-%d"),
        )
