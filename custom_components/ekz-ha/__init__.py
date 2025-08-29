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
        """Set up the coordinator (nur Installationen laden)."""
        self.installations = await self.ekz_fetcher.getInstallations()

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        if self.installations is None or self.installations == []:
            self.installations = await self.ekz_fetcher.getInstallations()
        meta_entities = getattr(self, "meta_entities", None)
        if not meta_entities:
            _LOGGER.debug("meta_entities mapping not set or empty during update. Entities may not be initialized yet.")
        for key in self.installations:
            meta_entity = meta_entities.get(key) if meta_entities else None
            if meta_entity is not None:
                _LOGGER.debug(f"Meta entity for {key}: unique_id={getattr(meta_entity, 'unique_id', None)}, last_import={getattr(meta_entity, '_last_import', None)}, contract_start={getattr(meta_entity, '_contract_start', None)}")
            else:
                _LOGGER.debug(f"Meta entity for {key}: None")
            if meta_entity is None:
                continue # if no meta entity, skip it - need it to store last_import etc.
            # Determine contract_start and last_import
            contract_start = meta_entity._contract_start if meta_entity is not None else None
            if contract_start is None:
                contract_start = self.installations[key]["contract_start"]
                if meta_entity is not None and meta_entity._contract_start is None:
                    meta_entity.set_contract_start(
                        datetime.strptime(contract_start, "%Y-%m-%d").date()
                    )
                    _LOGGER.debug(f"Meta entity for {key}: unique_id={meta_entity.unique_id}, last_import={getattr(meta_entity, '_last_import', None)}, contract_start={getattr(meta_entity, '_contract_start', None)}")
            last_import = meta_entity._last_import if meta_entity is not None else None
            if contract_start is not None and last_import is None:
                last_import = datetime.strptime(
                    contract_start, "%Y-%m-%d"
                ).date()

            result = await self.ekz_fetcher.import_full_history_to_statistics(
                self.hass, key, last_import, contract_start, meta_entity
            )
            _LOGGER.debug(f"Result for {key}: {result}")

            # save averages for later use
            averages = None
            if "averages" in result:
                _LOGGER.debug(f"Averages for {key}: {result['averages']}")
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

                _LOGGER.debug(
                    f"Predictions for {key} from {predictions[0]['start']} to {predictions[len(predictions)-1]['start']}: {predictions}"
                )
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
            if len(result["statistics"]) > 0:
                _LOGGER.debug(f"Statistics for {key}: {result['statistics']}")
                statistics = result["statistics"]
                async_import_statistics(
                    self.hass,
                    {
                        "has_sum": True,
                        "source": "recorder",
                        "statistic_id": f"sensor.ekz_electricity_consumption_{key}",
                        "name": None,
                        "unit_of_measurement": "kWh",
                    },
                    statistics
                )
                _LOGGER.debug(f"Meta entity: {meta_entity}")
                if statistics is not None and len(statistics) > 0 and meta_entity is not None:
                    lastDate = statistics[len(statistics)-1]["start"]
                    _LOGGER.debug(f"Setting last_import for {key} to {lastDate}")
                    meta_entity.set_last_import(lastDate)     
                

async def async_setup_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up integration entry."""
    scan_interval = entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    ekz_fetcher = EkzFetcher(entry.data["user"], entry.data["password"])
    coordinator = EkzCoordinator(hass, ekz_fetcher, scan_interval, entry)

    hass.data[DOMAIN] = {
        "conf": entry,
        "coordinator": coordinator,
    }
    await coordinator.async_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    return True

async def async_unload_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if unload_ok:
        hass.data.pop(DOMAIN, None)
    return unload_ok
