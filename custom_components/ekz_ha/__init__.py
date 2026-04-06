"""Entrypoint."""

from datetime import datetime, timedelta
import logging
import zoneinfo

from homeassistant import core
from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import StatisticData, StatisticMeanType, StatisticMetaData
from homeassistant.components.recorder.statistics import async_import_statistics, get_last_statistics
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import CATCHUP_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN, NORMAL_SCAN_INTERVAL
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
        self.production_installations = {}
        self.consumption_averages = {}
        self.last_sums: dict[str, float] = {}
        self.last_production_sums: dict[str, float] = {}
        self.last_prediction_sums: dict[str, float] = {}
        self.catching_up: dict[str, bool] = {}
        self._normal_interval = update_interval  # remember configured interval for later restore

    async def _async_setup(self):
        """Load installations on first start."""
        self.installations = await self.ekz_fetcher.getInstallations()
        self.production_installations = await self.ekz_fetcher.getProductionInstallations()
        _LOGGER.debug(f"Production installations found: {list(self.production_installations.keys())}")

    async def _async_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        if self.installations is None or self.installations == []:
            self.installations = await self.ekz_fetcher.getInstallations()
        if not self.production_installations:
            self.production_installations = await self.ekz_fetcher.getProductionInstallations()
        meta_entities = getattr(self, "meta_entities", None)
        if not meta_entities:
            _LOGGER.debug("meta_entities not yet available during update — entities not initialized yet.")
        for key in self.installations:
            meta_entity = meta_entities.get(key) if meta_entities else None
            if meta_entity is not None:
                _LOGGER.debug(f"Meta entity for {key}: unique_id={getattr(meta_entity, 'unique_id', None)}, last_import={getattr(meta_entity, '_last_import', None)}, contract_start={getattr(meta_entity, '_contract_start', None)}")
            else:
                _LOGGER.debug(f"Meta entity for {key}: None")
            if meta_entity is None:
                continue  # meta entity required for tracking import state
            # Determine contract_start
            contract_start = meta_entity._contract_start if meta_entity is not None else None
            if contract_start is None:
                contract_start = self.installations[key]["contract_start"]
                if meta_entity is not None and meta_entity._contract_start is None:
                    meta_entity.set_contract_start(
                        datetime.strptime(contract_start, "%Y-%m-%d").date()
                    )
                    _LOGGER.debug(f"Meta entity for {key}: unique_id={meta_entity.unique_id}, last_import={getattr(meta_entity, '_last_import', None)}, contract_start={getattr(meta_entity, '_contract_start', None)}")

            # Query DB only on first cycle after (re)start to restore last import date.
            # In-memory state (set by EkzFetcher after each import) is trusted for all subsequent cycles.
            # Querying every cycle risks overwriting with a stale DB result if the recorder is not yet ready.
            statistic_id = f"sensor.electricity_consumption_ekz_{key}"
            if meta_entity._last_import is None:
                try:
                    last_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                    )
                    if last_stats and statistic_id in last_stats:
                        last_stat_data = last_stats[statistic_id]
                        if last_stat_data:
                            raw_start = last_stat_data[0]["start"]
                            # HA returns start as float (Unix timestamp) or datetime depending on version
                            if isinstance(raw_start, (int, float)):
                                import_dt = datetime.fromtimestamp(float(raw_start), tz=ZRH)
                            elif hasattr(raw_start, "tzinfo") and raw_start.tzinfo is not None:
                                import_dt = raw_start.astimezone(ZRH)
                            else:
                                import_dt = raw_start.replace(tzinfo=ZRH)
                            import_date = import_dt.date()
                            _LOGGER.info(f"Restored last import for {key} from DB: {import_date}")
                            meta_entity.set_last_import(import_date)
                            if last_stat_data[0].get("sum") is not None:
                                self.last_sums[key] = last_stat_data[0]["sum"]
                            # Pre-initialise catching_up flag so sensor shows correct value immediately
                            today_date = datetime.now(tz=ZRH).date()
                            if (today_date - import_date).days <= 1:
                                self.catching_up[key] = False
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing statistics for {key}: {e}")

            # One-time migration: clear data stored under the old statistic_id from early integration versions
            old_statistic_id = f"sensor.ekz_electricity_consumption_{key}"
            try:
                old_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, old_statistic_id, True, {"sum"}
                )
                if old_stats and old_statistic_id in old_stats:
                    _LOGGER.info(f"Migrating: clearing old statistics under {old_statistic_id}")
                    try:
                        from homeassistant.components.recorder.statistics import async_clear_statistics
                        await async_clear_statistics(self.hass, [old_statistic_id])
                    except ImportError:
                        _LOGGER.info(
                            f"async_clear_statistics not available in this HA version — "
                            f"old statistics under {old_statistic_id} will remain but won't affect functionality"
                        )
            except Exception as e:
                _LOGGER.debug(f"Migration check failed for {key}: {e}")

            # Fall back to contract_start when no statistics exist yet (first-ever import)
            if meta_entity._last_import is None and contract_start is not None:
                start = contract_start if not isinstance(contract_start, str) else datetime.strptime(contract_start, "%Y-%m-%d")
                meta_entity.set_last_import(start)
                _LOGGER.info(f"No existing statistics for {key}, starting import from contract start {start}")

            # Import exactly one 30-day chunk per update cycle.
            # EkzFetcher updates meta_entity._last_import after each import so the next cycle continues from there.
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
            # Adjust polling interval based on catch-up status
            today = datetime.now(tz=ZRH).date()
            to_date = result.get("to_date")
            still_catching_up = to_date is not None and (today - to_date).days > 1
            self.catching_up[key] = still_catching_up
            if still_catching_up:
                if self.update_interval != CATCHUP_SCAN_INTERVAL:
                    _LOGGER.info(f"Catch-up mode for {key}: imported up to {to_date}, switching poll interval to {CATCHUP_SCAN_INTERVAL}")
                    self.update_interval = CATCHUP_SCAN_INTERVAL
            else:
                if self.update_interval != NORMAL_SCAN_INTERVAL:
                    _LOGGER.info(f"Catch-up complete for {key}, switching to daily poll interval")
                    self.update_interval = NORMAL_SCAN_INTERVAL

            # Save consumption averages for prediction calculation
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
                # Reset prediction statistics for all periods covered by real data,
                # then extrapolate forward using historical averages.
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

        # --- Production (solar feed-in) import loop ---
        production_meta_entities = getattr(self, "production_meta_entities", {})
        for key, info in self.production_installations.items():
            prod_meta = production_meta_entities.get(key) if production_meta_entities else None
            if prod_meta is None:
                continue
            contract_start = info.get("contract_start")
            if prod_meta._contract_start is None and contract_start:
                prod_meta.set_contract_start(datetime.strptime(contract_start, "%Y-%m-%d").date())
            statistic_id = f"sensor.electricity_production_ekz_{key}"
            if prod_meta._last_import is None:
                try:
                    last_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                    )
                    if last_stats and statistic_id in last_stats:
                        last_stat_data = last_stats[statistic_id]
                        if last_stat_data:
                            raw_start = last_stat_data[0]["start"]
                            if isinstance(raw_start, (int, float)):
                                import_dt = datetime.fromtimestamp(float(raw_start), tz=ZRH)
                            elif hasattr(raw_start, "tzinfo") and raw_start.tzinfo is not None:
                                import_dt = raw_start.astimezone(ZRH)
                            else:
                                import_dt = raw_start.replace(tzinfo=ZRH)
                            prod_meta.set_last_import(import_dt.date())
                            if last_stat_data[0].get("sum") is not None:
                                self.last_production_sums[key] = last_stat_data[0]["sum"]
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing production statistics for {key}: {e}")
            if prod_meta._last_import is None and contract_start:
                prod_meta.set_last_import(datetime.strptime(contract_start, "%Y-%m-%d"))
            result = await self.ekz_fetcher.import_production_history_to_statistics(
                self.hass, key, contract_start, prod_meta,
                running_sum_offset=self.last_production_sums.get(key, 0.0),
            )
            if result.get("statistics"):
                self.last_production_sums[key] = result["statistics"][-1]["sum"]
                _LOGGER.info(f"Importing {len(result['statistics'])} production statistics for {key}")
                try:
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(statistic_id),
                        [StatisticData(start=s["start"], sum=s["sum"], state=s["state"]) for s in result["statistics"]],
                    )
                except Exception as e:
                    _LOGGER.error(f"Failed to import production statistics for {key}: {e}")


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

    async def handle_reset_statistics(call):
        """Delete all EKZ statistics from the DB and reset in-memory state so a full re-import starts."""
        import inspect

        async def _clear_statistics(statistic_ids: list[str]) -> bool:
            """Try to clear statistics, handling different HA versions robustly."""
            # Approach 1: module-level function (most HA versions)
            try:
                from homeassistant.components.recorder.statistics import async_clear_statistics
                result = async_clear_statistics(hass, statistic_ids)
                if inspect.isawaitable(result):
                    await result
                return True
            except (ImportError, Exception) as err:
                _LOGGER.debug("Module-level async_clear_statistics failed: %s", err)

            # Approach 2: method on recorder instance (some HA versions)
            try:
                recorder = get_recorder_instance(hass)
                clear_fn = getattr(recorder, "async_clear_statistics", None)
                if clear_fn is not None:
                    result = clear_fn(statistic_ids)
                    if inspect.isawaitable(result):
                        await result
                    return True
            except Exception as err:
                _LOGGER.debug("Recorder instance async_clear_statistics failed: %s", err)

            return False

        statistic_ids = []
        for key in coordinator.installations:
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}")
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}_predictions")
        for key in coordinator.production_installations:
            statistic_ids.append(f"sensor.electricity_production_ekz_{key}")

        _LOGGER.info("Resetting EKZ statistics for: %s", statistic_ids)
        if not await _clear_statistics(statistic_ids):
            _LOGGER.error("Cannot clear statistics: no compatible API found in this HA version")
            return

        # Reset in-memory tracking so the next poll starts from contract_start
        coordinator.last_sums = {}
        coordinator.last_production_sums = {}
        coordinator.last_prediction_sums = {}
        coordinator.catching_up = {}
        for meta in (getattr(coordinator, "meta_entities", None) or {}).values():
            meta.set_last_import(None)
        for meta in (getattr(coordinator, "production_meta_entities", None) or {}).values():
            meta.set_last_import(None)

        _LOGGER.info("EKZ statistics reset complete — re-import will start on next poll")
        await coordinator.async_request_refresh()

    hass.services.async_register(DOMAIN, "reset_statistics", handle_reset_statistics)
    return True

async def async_unload_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if unload_ok:
        hass.data.pop(DOMAIN, None)
        hass.services.async_remove(DOMAIN, "reset_statistics")
    return unload_ok
