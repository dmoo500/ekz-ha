"""High-level EKZ API client."""

import logging
from datetime import date

from .models import ConsumptionData, InstallationData, InstallationSelectionData
from .session import Session

_LOGGER = logging.getLogger(__name__)


class EkzApiClient:
    """High-level client for the EKZ API with type-safe responses."""

    def __init__(
        self,
        username: str,
        password: str,
        totp_secret: str | None = None,
        device_name: str | None = None,
    ):
        """Initialize the API client."""
        self.session = Session(username, password, totp_secret, device_name)

    async def close(self):
        """Close the API session."""
        if self.session._session:
            await self.session._session.close()

    async def get_consumption_installations(self) -> InstallationSelectionData:
        """Fetch available consumption installations."""
        data = await self.session.installation_selection_data()
        return InstallationSelectionData.from_dict(data)

    async def get_production_installations(self) -> InstallationSelectionData:
        """Fetch available production (solar) installations."""
        data = await self.session.production_installation_selection_data()
        return InstallationSelectionData.from_dict(data)

    async def get_installation_metadata(self, installation_id: str) -> InstallationData:
        """Fetch metadata for a specific installation."""
        data = await self.session.get_installation_data(installation_id)
        return InstallationData.from_dict(data)

    async def get_consumption_data(
        self,
        installation_id: str,
        data_type: str,
        date_from: date | str,
        date_to: date | str,
    ) -> ConsumptionData:
        """
        Fetch consumption or production data.

        Args:
            installation_id: Installation ID
            data_type: Type of data (e.g., 'PK_VERB_15MIN', 'WIRK_NEG_15MIN')
            date_from: Start date
            date_to: End date

        Returns:
            ConsumptionData with parsed values
        """
        # Convert dates to strings if needed
        if isinstance(date_from, date):
            date_from = date_from.strftime("%Y-%m-%d")
        if isinstance(date_to, date):
            date_to = date_to.strftime("%Y-%m-%d")

        data = await self.session.get_consumption_data(
            installation_id, data_type, date_from, date_to
        )
        return ConsumptionData.from_dict(data)

    async def get_consumption_15min(
        self, installation_id: str, date_from: date | str, date_to: date | str
    ) -> ConsumptionData:
        """Fetch 15-minute consumption data."""
        return await self.get_consumption_data(installation_id, "PK_VERB_15MIN", date_from, date_to)

    async def get_consumption_daily(
        self, installation_id: str, date_from: date | str, date_to: date | str
    ) -> ConsumptionData:
        """Fetch daily consumption data."""
        return await self.get_consumption_data(
            installation_id, "PK_VERB_TAG_METER", date_from, date_to
        )

    async def get_production_15min(
        self, installation_id: str, date_from: date | str, date_to: date | str
    ) -> ConsumptionData:
        """Fetch 15-minute production (solar feed-in) data."""
        return await self.get_consumption_data(
            installation_id, "WIRK_NEG_15MIN", date_from, date_to
        )
