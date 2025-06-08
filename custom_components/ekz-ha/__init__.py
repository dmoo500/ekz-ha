"""Entrypoint."""

from datetime import datetime, timedelta
import logging
import zoneinfo

import voluptuous as vol

from .EkzFetcher import EkzFetcher
from homeassistant import config_entries, core
from homeassistant.components.recorder.statistics import async_import_statistics
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import DEFAULT_SCAN_INTERVAL, DOMAIN

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
_LOGGER = logging.getLogger(__name__)


class EkzConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Ekz config flow."""

    # The schema version of the entries that it creates
    # Home Assistant will call your migrate method if the version changes
    VERSION = 1
    MINOR_VERSION = 1

    async def async_step_user(self, user_input):
        """Configure EKZ login."""
        if user_input is not None:
            return self.async_create_entry(title="ekz", data=user_input)
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required("user"): str,
                    vol.Required("password"): str,
                }
            ),
        )


# TODO consider deleting.
async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the platform.

    @NOTE: `config` is the full dict from `configuration.yaml`.

    :returns: A boolean to indicate that initialization was successful.
    """
    return True


class EkzCoordinator(DataUpdateCoordinator):
    """Coordinates data fetching from EKZ."""

    def __init__(
        self,
        hass: HomeAssistant,
        ekz_fetcher: EkzFetcher,
        update_interval: timedelta,
        config,
    ) -> None:
        """Initialize EKZ§ coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            # Name of the data. For logging purposes.
            name=DOMAIN,
            # Polling interval. Will only be polled if there are subscribers.
            update_interval=update_interval,
            # Set always_update to `False` if the data returned from the
            # api can be compared via `__eq__` to avoid duplicate updates
            # being dispatched to listeners
            always_update=True,
        )
        self.ekz_fetcher = ekz_fetcher
        self.config = config
        self.installations = []

    async def _async_setup(self):
        """Set up the coordinator.

        This is the place to set up your coordinator,
        or to load data, that only needs to be loaded once.

        This method will be called automatically during
        coordinator.async_config_entry_first_refresh.
        """
        self.installations = await self.ekz_fetcher.getInstallations()

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        if self.installations is None or self.installations == []:
            self.installations = await self.ekz_fetcher.getInstallations()
        for key in self.installations:
            last_full_day = self.hass.states.get(
                f"input_text.electricity_consumption_ekz_{key}_last_full_day_update"
            )
            last_update_total = self.hass.states.get(
                f"input_number.electricity_consumption_ekz_{key}_last_update_total"
            )
            if last_full_day is None or last_update_total is None:
                _LOGGER.info(
                    f"Initializing info for EKZ installation {key} from scratch"
                )
                result = await self.ekz_fetcher.fetchEntireHistory(key)
            else:
                _LOGGER.info(f"Incrementally updating info for EKZ installation {key}")
                result = await self.ekz_fetcher.fetchNewInstallationData(
                    key,
                    last_full_day.as_dict()["state"],
                    float(last_update_total.as_dict()["state"]),
                )
            async_import_statistics(
                self.hass,
                {
                    "has_sum": True,
                    "source": "recorder",
                    "statistic_id": f"input_number.electricity_consumption_ekz_{key}",
                    "name": None,
                    "unit_of_measurement": "kWh",
                },
                result["statistics"],
            )
            self.hass.states.async_set(
                f"input_text.electricity_consumption_ekz_{key}_last_full_day_update",
                result["last_full_day"],
            )
            self.hass.states.async_set(
                f"input_number.electricity_consumption_ekz_{key}_last_update_total",
                result["last_full_day_sum"],
            )

        self.hass.async_create_task(
            async_load_platform(self.hass, "sensor", DOMAIN, {}, self.config)
        )


async def async_setup_entry(hass: core.HomeAssistant, config: ConfigEntry) -> bool:
    """Set up integration entry."""
    scan_interval = config.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    ekz_fetcher = EkzFetcher(config.data["user"], config.data["password"])
    coordinator = EkzCoordinator(hass, ekz_fetcher, scan_interval, config)

    hass.data[DOMAIN] = {
        "conf": config,
        "coordinator": coordinator,
    }
    await coordinator.async_refresh()
    return True
