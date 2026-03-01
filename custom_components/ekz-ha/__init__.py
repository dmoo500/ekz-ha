"""Entrypoint."""

from datetime import datetime, timedelta
import logging
import zoneinfo

from homeassistant import core
from homeassistant.components.recorder.models import StatisticData, StatisticMeanType, StatisticMetaData
from homeassistant.components.recorder.statistics import async_import_statistics, get_last_statistics
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import CATCHUP_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN
from .EkzFetcher import EkzFetcher

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
_LOGGER = logging.getLogger(__name__)


def _make_stat_meta(statistic_id: str) -> StatisticMetaData:
    """Build StatisticMetaData, adding unit_class='energy' when supported (HA 2024.3+)."""
    kwargs = {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "source": "recorder",
        "statistic_id": statistic_id,
        "name": None,
        "unit_of_measurement": "kWh",
    }
    try:
        return StatisticMetaData(**kwargs, unit_class="energy")
    except TypeError:
        return StatisticMetaData(**kwargs)


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
        self.last_sums: dict[str, float] = {}
        self.last_prediction_sums: dict[str, float] = {}
        self._normal_interval = update_interval  # remember configured interval for later restore

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
            
            # One-time migration: check for old statistic_id and clear it
            old_statistic_id = f"sensor.ekz_electricity_consumption_{key}"
            try:
                from homeassistant.components.recorder.statistics import async_clear_statistics
                last_stats = await get_last_statistics(self.hass, 1, old_statistic_id, True, {"sum"})
                if last_stats and old_statistic_id in last_stats:
                    _LOGGER.info(f"Found old statistics under wrong ID {old_statistic_id}, clearing to restart import with correct ID")
                    await async_clear_statistics(self.hass, [old_statistic_id])
                    # Reset last_import so we restart from beginning
                    if meta_entity is not None:
                        meta_entity.set_last_import(None)
                        meta_entity._last_import = None
                    last_import = None
            except Exception as e:
                _LOGGER.debug(f"Migration check failed for {key}: {e}")
            
            # Check if we have existing statistics to resume from
            last_import = meta_entity._last_import if meta_entity is not None else None
            if last_import is None:
                # Query the statistics database to find the last imported data point AND last sum
                statistic_id = f"sensor.electricity_consumption_ekz_{key}"
                try:
                    last_stats = await get_last_statistics(self.hass, 1, statistic_id, True, {"sum"})
                    if last_stats and statistic_id in last_stats:
                        last_stat_data = last_stats[statistic_id]
                        if last_stat_data:
                            last_import_dt = last_stat_data[0]["start"]
                            last_import = last_import_dt.date()
                            _LOGGER.info(f"Found existing statistics for {key}, resuming from {last_import}")
                            if meta_entity is not None:
                                meta_entity.set_last_import(last_import_dt)
                            # Restore last known sum so the running total continues correctly
                            if key not in self.last_sums and last_stat_data[0].get("sum") is not None:
                                self.last_sums[key] = last_stat_data[0]["sum"]
                                _LOGGER.info(f"Restored last_sum for {key}: {self.last_sums[key]}")
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing statistics for {key}: {e}")
                    
            if contract_start is not None and last_import is None:
                if isinstance(contract_start, str):
                    last_import = datetime.strptime(contract_start, "%Y-%m-%d").date()
                else:
                    last_import = contract_start

            # Import exactly one 30-day chunk per update cycle.
            # The meta entity tracks last_import so the next cycle continues where we left off.
            result = await self.ekz_fetcher.import_full_history_to_statistics(
                self.hass, key, contract_start, meta_entity,
                running_sum_offset=self.last_sums.get(key, 0.0),
            )
            _LOGGER.debug(f"Chunk result for {key}: from={result.get('from_date')} to={result.get('to_date')}, entries={len(result.get('statistics', []))}")
            if result.get("statistics"):
                last_stat = result["statistics"][-1]
                self.last_sums[key] = last_stat["sum"]
                _LOGGER.info(f"Importing chunk of {len(result['statistics'])} statistics for {key}, range {result['statistics'][0]['start']} to {result['statistics'][-1]['start']}")
                try:
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(f"sensor.electricity_consumption_ekz_{key}"),
                        [StatisticData(start=s["start"], sum=s["sum"], state=s["state"]) for s in result["statistics"]],
                    )
                except Exception as e:
                    _LOGGER.error(f"Failed to import statistics chunk for {key}: {e}")
            # Automatically adjust polling interval based on catch-up status
            today = datetime.now(tz=ZRH).date()
            to_date = result.get("to_date")
            still_catching_up = to_date is not None and to_date < today
            if still_catching_up:
                if self.update_interval != CATCHUP_SCAN_INTERVAL:
                    _LOGGER.info(f"Catch-up mode for {key}: imported up to {to_date}, switching poll interval to {CATCHUP_SCAN_INTERVAL}")
                    self.update_interval = CATCHUP_SCAN_INTERVAL
            else:
                if self.update_interval != self._normal_interval:
                    _LOGGER.info(f"Catch-up complete for {key}, restoring poll interval to {self._normal_interval}")
                    self.update_interval = self._normal_interval

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
                                "state": averages[mh_key],
                            }
                        )
                    else:
                        running_total += last_actual["state"]
                        predictions.append(
                            {
                                "start": last_actual["start"],
                                "sum": running_total,
                                "state": last_actual["state"],
                            }
                        )
                    last_actual["start"] = last_actual["start"] + timedelta(hours=1)

                _LOGGER.debug(
                    f"Predictions for {key} from {predictions[0]['start']} to {predictions[len(predictions)-1]['start']}: {predictions}"
                )
                async_import_statistics(
                    self.hass,
                    _make_stat_meta(f"sensor.electricity_consumption_ekz_{key}_predictions"),
                    [StatisticData(start=s["start"], sum=s["sum"], state=s["state"]) for s in predictions],
                )
                if predictions:
                    self.last_prediction_sums[key] = predictions[-1]["sum"]     
                

async def async_setup_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up integration entry."""
    scan_interval = entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    ekz_fetcher = EkzFetcher(entry.data["user"], entry.data["password"], entry.data.get("totp_secret"), entry.data.get("device_name"))
    coordinator = EkzCoordinator(hass, ekz_fetcher, scan_interval, entry)

    hass.data[DOMAIN] = {
        "conf": entry,
        "coordinator": coordinator,
    }
    await coordinator.async_config_entry_first_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    return True

async def async_unload_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if unload_ok:
        hass.data.pop(DOMAIN, None)
    return unload_ok
