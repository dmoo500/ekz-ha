"""Entrypoint."""

import logging

import voluptuous as vol

from datetime import datetime, timedelta

from config.custom_components.ekz_ha.EkzFetcher import EkzFetcher
from homeassistant import config_entries, core
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    async_import_statistics,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import DEFAULT_SCAN_INTERVAL, DOMAIN

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
            _LOGGER.info(user_input)
            return self.async_create_entry(title="ekz", data=user_input)
        _LOGGER.info("Initial setup")
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required("user"): str,
                    vol.Required("password"): str,
                    # vol.Optional(
                    #     CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL
                    # ): vol.All(cv.time_period, cv.positive_timedelta),
                }
            ),
        )


async def async_setup(hass: core.HomeAssistant, config: dict) -> bool:
    """Set up the platform.

    @NOTE: `config` is the full dict from `configuration.yaml`.

    :returns: A boolean to indicate that initialization was successful.
    """
    _LOGGER.info(f"{config} in setup")
    return True


import pytz

ZRH = pytz.timezone("Europe/Zurich")


class EkzCoordinator(DataUpdateCoordinator):
    def __init__(self, hass, ekz_fetcher, update_interval, config):
        """Initialize my coordinator."""
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

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        # TODO identify date range that needs updating
        self.installations = await self.ekz_fetcher.getInstallations()
        self.hass.async_create_task(
            async_load_platform(self.hass, "sensor", DOMAIN, {}, self.config)
        )
        new_data = await self.ekz_fetcher.fetch()
        for key in new_data:
            values = new_data[key]
            # values = sorted(values, key=lambda v: v["timestamp"], reverse=True)
            # print(values)
            running_sum = 0
            statistics = [
                {
                    "start": datetime.strptime(
                        str(value["timestamp"]), "%Y%m%d%H%M%S"
                    ).astimezone(tz=ZRH),
                    # "last_reset": datetime.strptime(
                    #     str(value["timestamp"]), "%Y%m%d%H%M%S"
                    # ).astimezone(tz=ZRH)
                    # - timedelta(hours=1),
                    "sum": (running_sum := running_sum + value["value"]),
                    "state": value["value"],
                }
                for value in values
            ]
            print(statistics)
            async_import_statistics(
                self.hass,
                {
                    "has_sum": True,
                    "source": "recorder",
                    "statistic_id": f"input_number.electricity_consumption_ekz_{key}",
                    "name": None,
                    "unit_of_measurement": "kWh",
                },
                statistics,
            )
            _LOGGER.info("updated history")


async def async_setup_entry(hass: core.HomeAssistant, config: ConfigEntry) -> bool:
    scan_interval = config.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    ekz_fetcher = EkzFetcher(config.data["user"], config.data["password"])
    coordinator = EkzCoordinator(hass, ekz_fetcher, scan_interval, config)

    hass.data[DOMAIN] = {
        "conf": config,
        "coordinator": coordinator,
    }
    await coordinator.async_refresh()
    return True
