"""Entrypoint."""

from datetime import datetime, timedelta
import logging
import zoneinfo

import voluptuous as vol

from homeassistant import config_entries, core
from homeassistant.components.recorder.statistics import async_import_statistics
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.discovery import async_load_platform
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import DEFAULT_SCAN_INTERVAL, DOMAIN
from .EkzFetcher import EkzFetcher

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


class EkzCoordinator(DataUpdateCoordinator):
    """Coordinates data fetching from EKZ."""

    def __init__(
        self,
        hass: HomeAssistant,
        ekz_fetcher: EkzFetcher,
        update_interval: timedelta,
        config,
    ) -> None:
        """Initialize EKZ coordinator."""
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
        self.consumption_averages = {}

    async def _async_setup(self):
        """Set up the coordinator.

        This is the place to set up your coordinator,
        or to load data, that only needs to be loaded once.

        This method will be called automatically during
        coordinator.async_config_entry_first_refresh.
        """
        self.installations = await self.ekz_fetcher.getInstallations()
        self.hass.async_create_task(
            async_load_platform(self.hass, "sensor", DOMAIN, {}, self.config)
        )

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        if self.installations is None or self.installations == []:
            self.installations = await self.ekz_fetcher.getInstallations()
            self.hass.async_create_task(
                async_load_platform(self.hass, "sensor", DOMAIN, {}, self.config)
            )
        for key in self.installations:
            last_full_day = self.hass.states.get(
                f"sensor.ekz_electricity_consumption_{key}_internal_last_day"
            )
            last_update_total = self.hass.states.get(
                f"sensor.ekz_electricity_consumption_{key}_internal_last_sum"
            )
            last_get_all = self.hass.states.get(
                f"sensor.ekz_electricity_consumption_{key}_last_get_all"
            )
            if last_get_all is not None:
                last_get_all_date = datetime.strptime(
                    last_get_all.as_dict()["state"], "%Y-%m-%d %H:%M:%S"
                )
                # TODO perform the full update more rarely, e.g. once a month
                if last_get_all_date + timedelta(days=1) < datetime.now():
                    # force full update by pretending last_full_day is None
                    last_full_day = None
            else:
                last_full_day = None
            if (
                last_full_day is None
                or last_update_total is None
                or last_full_day.as_dict()["state"] is None
                or last_update_total.as_dict()["state"] is None
                or float(last_update_total.as_dict()["state"]) < 0
            ):
                _LOGGER.info(
                    f"Initializing info for EKZ installation {key} from scratch"
                )
                result = await self.ekz_fetcher.fetchEntireHistory(key)
                self.hass.states.async_set(
                    f"sensor.ekz_electricity_consumption_{key}_last_get_all",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
            else:
                _LOGGER.info(f"Incrementally updating info for EKZ installation {key}")
                result = await self.ekz_fetcher.fetchNewInstallationData(
                    key,
                    last_full_day.as_dict()["state"],
                    float(last_update_total.as_dict()["state"]),
                )
            averages = None
            if "averages" in result:
                if self.consumption_averages is None:
                    self.consumption_averages = {}
                self.consumption_averages[key] = result["averages"]
                averages = result["averages"]
            elif self.consumption_averages is None:
                if key in self.consumption_averages:
                    averages = self.consumption_averages[key]
            if averages is not None and len(result["statistics"]) > 0:
                # For all times for which we have a value in result["statistics"], we now want to set the sum/state to 0 (overriding previous predictions).
                # For all times from the highest timestamp to now, we try to find a prediction.
                predictions = [
                    {"start": x["start"], "sum": 0, "state": 0}
                    for x in result["statistics"]
                ]
                last_actual = result["statistics"][len(result["statistics"]) - 1]
                # Copy
                last_actual = {
                    "start": last_actual["start"],
                    "sum": last_actual["sum"],
                    "state": last_actual["state"],
                }
                last_actual["start"] = last_actual["start"] + timedelta(hours=1)
                running_total = 0
                while last_actual["start"] < datetime.now().astimezone(tz=ZRH):
                    mh_key = (
                        last_actual["start"].month * 100 + last_actual["start"].hour
                    )
                    if mh_key in averages:
                        running_total += averages[mh_key]
                        predictions.append(
                            {
                                "start": last_actual["start"],
                                "sum": running_total,
                                "stage": averages[mh_key],
                            }
                        )
                    else:
                        running_total += last_actual["state"]
                        predictions.append(
                            {
                                "start": last_actual["start"],
                                "sum": running_total,
                                "stage": last_actual["state"],
                            }
                        )
                    last_actual["start"] = last_actual["start"] + timedelta(hours=1)
                async_import_statistics(
                    self.hass,
                    {
                        "has_sum": True,
                        "source": "recorder",
                        "statistic_id": f"sensor.ekz_electricity_consumption_{key}_predictions",
                        "name": None,
                        "unit_of_measurement": "kWh",
                    },
                    predictions,
                )
            async_import_statistics(
                self.hass,
                {
                    "has_sum": True,
                    "source": "recorder",
                    "statistic_id": f"sensor.ekz_electricity_consumption_{key}",
                    "name": None,
                    "unit_of_measurement": "kWh",
                },
                result["statistics"],
            )
            self.hass.states.async_set(
                f"sensor.ekz_electricity_consumption_{key}_internal_last_day",
                result["last_full_day"],
            )
            self.hass.states.async_set(
                f"sensor.ekz_electricity_consumption_{key}_internal_last_sum",
                result["last_full_day_sum"],
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
