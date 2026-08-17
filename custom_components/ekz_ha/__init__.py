"""Entrypoint."""

import asyncio
import logging
import zoneinfo
from datetime import datetime, timedelta

from homeassistant import core
from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
)

try:
    from homeassistant.components.recorder.models import StatisticMeanType

    MEAN_TYPE_NONE = StatisticMeanType.NONE
except ImportError:
    # HA 2024.2+ removed StatisticMeanType enum, use None directly
    MEAN_TYPE_NONE = None  # type: ignore[assignment]
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .api.client import EkzApiClient
from .const import CATCHUP_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN, NORMAL_SCAN_INTERVAL
from .statistics.consumption import ConsumptionImporter
from .statistics.prediction import PredictionService
from .statistics.production import ProductionImporter

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")
_LOGGER = logging.getLogger(__name__)


def _is_contract_active(contract) -> bool:
    """Check if a contract is currently active (not expired).

    Args:
        contract: InstallationContract with optional auszdat (contract end date)

    Returns:
        True if contract is active (no end date or end date in future/today)
    """
    if not contract.auszdat:
        return True  # No end date means active contract

    try:
        end_date = datetime.strptime(contract.auszdat, "%Y-%m-%d").date()
        today = datetime.now(tz=ZRH).date()
        return end_date >= today  # Active if end date is today or in future
    except (ValueError, AttributeError):
        _LOGGER.warning(
            "Invalid contract end date format for installation %s: %s",
            contract.anlage,
            contract.auszdat
        )
        return True  # If parsing fails, assume active to avoid data loss


def _make_stat_meta(statistic_id: str) -> StatisticMetaData:
    """Build StatisticMetaData, adding unit_class='energy' when supported (HA 2024.3+)."""
    kwargs = {
        "has_sum": True,
        "mean_type": MEAN_TYPE_NONE,
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
        api_client: EkzApiClient,
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
        self.api_client = api_client
        self.config = config
        self.installations = []
        self.production_installations = {}
        self.consumption_averages = {}
        self.last_sums: dict[str, float] = {}
        self.last_production_sums: dict[str, float] = {}
        self.last_prediction_sums: dict[str, float] = {}
        self.catching_up: dict[str, bool] = {}
        self._normal_interval = update_interval  # remember configured interval for later restore
        self._reset_lock = asyncio.Lock()
        self.consumption_averages_raw: dict[str, dict] = {}  # accumulated slot sums for prediction
        self.prediction_services: dict[
            str, PredictionService
        ] = {}  # prediction service per installation
        self.next_update_time: datetime | None = None

    async def _async_setup(self):
        """Load installations on first start."""
        installations_data = await self.api_client.get_consumption_installations()

        # Filter for active contracts only
        active_contracts = [
            c for c in installations_data.contracts if c.anlage and _is_contract_active(c)
        ]
        inactive_count = len(
            [c for c in installations_data.contracts if c.anlage and not _is_contract_active(c)]
        )

        if inactive_count > 0:
            _LOGGER.info("Skipping %d inactive/expired contract(s)", inactive_count)

        self.installations = {
            contract.anlage: {"contract_start": contract.einzdat}
            for contract in active_contracts
        }

        if not self.installations:
            _LOGGER.warning("No active installations found in EKZ account")
            return

        _LOGGER.info(
            "Found %d active installation(s): %s",
            len(self.installations),
            list(self.installations.keys()),
        )

        # Production installations: Check each consumption installation for production data
        self.production_installations = {}
        for inst_id in self.installations:
            try:
                # Try to fetch production data to see if this installation has solar
                test_data = await self.api_client.get_production_15min(
                    inst_id, datetime.now(tz=ZRH) - timedelta(days=7), datetime.now(tz=ZRH)
                )
                if not test_data.is_empty():
                    self.production_installations[inst_id] = {
                        "contract_start": self.installations[inst_id]["contract_start"]
                    }
                    _LOGGER.info(f"Production data available for installation {inst_id}")
            except Exception as e:
                _LOGGER.debug(f"No production data for installation {inst_id}: {e}")

        _LOGGER.debug(
            f"Production installations found: {list(self.production_installations.keys())}"
        )

    async def _async_update_data(self):
        """Acquire reset lock then delegate to _do_update_data."""
        async with self._reset_lock:
            await self._do_update_data()

    async def _do_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        if self.installations is None or self.installations == []:
            installations_data = await self.api_client.get_consumption_installations()

            # Filter for active contracts only
            active_contracts = [
                c
                for c in installations_data.contracts
                if c.anlage and _is_contract_active(c)
            ]

            self.installations = {
                contract.anlage: {"contract_start": contract.einzdat}
                for contract in active_contracts
            }

        if not self.production_installations:
            # Check for production installations on first update
            for inst_id in self.installations:
                try:
                    test_data = await self.api_client.get_production_15min(
                        inst_id, datetime.now(tz=ZRH) - timedelta(days=7), datetime.now(tz=ZRH)
                    )
                    if not test_data.is_empty():
                        self.production_installations[inst_id] = {
                            "contract_start": self.installations[inst_id]["contract_start"]
                        }
                        _LOGGER.info(f"Production data available for installation {inst_id}")
                except Exception as e:
                    _LOGGER.debug(f"No production data for installation {inst_id}: {e}")

        meta_entities = getattr(self, "meta_entities", None)
        if not meta_entities:
            _LOGGER.debug(
                "meta_entities not yet available during update — entities not initialized yet."
            )
            return

        # --- Consumption import loop ---
        for key in self.installations:
            meta_entity = meta_entities.get(key) if meta_entities else None
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

            statistic_id = f"sensor.electricity_consumption_ekz_{key}"

            # Query DB only on first cycle after (re)start to restore last import date
            if meta_entity._last_import is None:
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
                            # Go back 1 day from the last DB entry
                            import_date = import_dt.date() - timedelta(days=1)
                            _LOGGER.info(
                                f"Restored last import for {key} from DB: {import_dt.date()} → rewinding to {import_date}"
                            )
                            meta_entity.set_last_import(import_date - timedelta(days=1))

                            # Pre-initialize catching_up flag
                            today_date = datetime.now(tz=ZRH).date()
                            if (today_date - import_date).days <= 1:
                                self.catching_up[key] = False
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing statistics for {key}: {e}")

            # One-time migration: clear old statistic_id
            old_statistic_id = f"sensor.ekz_electricity_consumption_{key}"
            try:
                old_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, old_statistic_id, True, {"sum"}
                )
                if old_stats and old_statistic_id in old_stats:
                    _LOGGER.info(f"Migrating: clearing old statistics under {old_statistic_id}")
                    try:
                        from homeassistant.components.recorder.statistics import (
                            async_clear_statistics,
                        )

                        await async_clear_statistics(self.hass, [old_statistic_id])
                    except ImportError:
                        _LOGGER.info(
                            f"async_clear_statistics not available — old statistics under {old_statistic_id} will remain"
                        )
            except Exception as e:
                _LOGGER.debug(f"Migration check failed for {key}: {e}")

            # Fall back to contract_start when no statistics exist yet
            if meta_entity._last_import is None and contract_start is not None:
                start = (
                    contract_start
                    if not isinstance(contract_start, str)
                    else datetime.strptime(contract_start, "%Y-%m-%d")
                )
                meta_entity.set_last_import(start)
                _LOGGER.info(
                    f"No existing statistics for {key}, starting import from contract start {start}"
                )

            # Query DB for running offset
            _last_import_date = meta_entity._last_import
            if _last_import_date is not None:
                if isinstance(_last_import_date, datetime):
                    _last_import_date = _last_import_date.date()
                offset_boundary = (
                    datetime.combine(_last_import_date + timedelta(days=1), datetime.min.time())
                    .replace(tzinfo=ZRH)
                    .astimezone(UTC)
                )
                try:
                    pre_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        statistics_during_period,
                        self.hass,
                        offset_boundary - timedelta(hours=26),
                        offset_boundary,
                        {statistic_id},
                        "hour",
                        None,
                        {"sum"},
                    )
                    if pre_stats and statistic_id in pre_stats and pre_stats[statistic_id]:
                        running_sum = pre_stats[statistic_id][-1]["sum"]
                        _LOGGER.info(
                            f"DB offset for {key}: {running_sum:.3f} kWh (boundary {offset_boundary})"
                        )
                    else:
                        running_sum = 0.0
                        _LOGGER.debug(
                            f"No DB stats found before {offset_boundary} for {key}, using 0"
                        )
                except Exception as e_offset:
                    _LOGGER.debug(f"Could not query DB offset for {key}: {e_offset}")
                    running_sum = self.last_sums.get(key, 0.0)
            else:
                running_sum = 0.0

            # Import using new architecture
            importer = ConsumptionImporter(self.api_client)
            contract_start_dt = (
                datetime.strptime(contract_start, "%Y-%m-%d")
                if isinstance(contract_start, str)
                else datetime.combine(contract_start, datetime.min.time())
            )
            result = await importer.import_statistics(
                self.hass,
                key,
                contract_start_dt,
                meta_entity,
                running_sum_offset=running_sum,
            )

            _LOGGER.debug(
                f"Chunk result for {key}: from={result.get('from_date')} to={result.get('to_date')}, entries={len(result.get('statistics', []))}"
            )

            if result.get("statistics"):
                # Update running sum
                self.last_sums[key] = result["statistics"][-1]["sum"]

                _LOGGER.info(
                    f"Importing chunk of {len(result['statistics'])} statistics for {key}, range {result['statistics'][0]['start']} to {result['statistics'][-1]['start']}"
                )
                try:
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(f"sensor.electricity_consumption_ekz_{key}"),
                        [
                            StatisticData(start=s["start"], sum=s["sum"], state=s["state"])
                            for s in result["statistics"]
                        ],
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
                    _LOGGER.info(
                        f"Catch-up mode for {key}: imported up to {to_date}, switching poll interval to {CATCHUP_SCAN_INTERVAL}"
                    )
                    self.update_interval = CATCHUP_SCAN_INTERVAL
            else:
                if self.update_interval != NORMAL_SCAN_INTERVAL:
                    _LOGGER.info(f"Catch-up complete for {key}, switching to daily poll interval")
                    self.update_interval = NORMAL_SCAN_INTERVAL

            # --- Prediction: Accumulate and generate predictions ---
            # Initialize prediction service if needed
            if key not in self.prediction_services:
                self.prediction_services[key] = PredictionService()

            prediction_service = self.prediction_services[key]

            # Accumulate raw values from this import chunk
            if result.get("raw_values"):
                prediction_service.accumulate_values(result["raw_values"])
                prediction_service.calculate_averages()
                averages = prediction_service.get_averages()
                _LOGGER.debug(
                    f"Updated prediction averages for {key}: {len(averages)} month-hour buckets"
                )

            # Generate predictions only when caught up (gap fills the recent EKZ delay)
            if result.get("statistics") and not still_catching_up:
                predictions = []
                averages = prediction_service.get_averages()

                if averages:
                    # Zero out predictions for periods already covered by real data
                    predictions = [
                        {"start": s["start"], "sum": 0, "state": 0} for s in result["statistics"]
                    ]

                    # Generate forward predictions from last real data to now
                    last_actual_start = result["statistics"][-1]["start"]
                    now_utc = datetime.now(tz=UTC)

                    forward_predictions = prediction_service.generate_predictions(
                        last_actual_start, now_utc
                    )
                    predictions.extend(forward_predictions)

                if len(predictions) > 1:
                    _LOGGER.info(
                        f"Predictions for {key}: {len(predictions)} entries, "
                        f"gap coverage {result['statistics'][-1]['start'].date()} → {datetime.now(tz=UTC).date()}"
                    )
                    try:
                        async_import_statistics(
                            self.hass,
                            _make_stat_meta(f"sensor.electricity_consumption_ekz_{key}_prediction"),
                            [
                                StatisticData(start=p["start"], sum=p["sum"], state=p["state"])
                                for p in predictions
                            ],
                        )
                    except Exception as e:
                        _LOGGER.error(f"Failed to import prediction statistics for {key}: {e}")

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
                            import_date = import_dt.date() - timedelta(days=1)
                            _LOGGER.info(
                                f"Restored last import for production {key} from DB: {import_dt.date()} → rewinding to {import_date}"
                            )
                            prod_meta.set_last_import(import_date - timedelta(days=1))
                            if last_stat_data[0].get("sum") is not None:
                                self.last_production_sums[key] = last_stat_data[0]["sum"]
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing production statistics for {key}: {e}")

            if prod_meta._last_import is None and contract_start:
                prod_meta.set_last_import(datetime.strptime(contract_start, "%Y-%m-%d"))

            # Import using new architecture
            importer = ProductionImporter(self.api_client)
            contract_start_dt = (
                datetime.strptime(contract_start, "%Y-%m-%d")
                if isinstance(contract_start, str)
                else datetime.combine(contract_start, datetime.min.time())
            )
            result = await importer.import_statistics(
                self.hass,
                key,
                contract_start_dt,
                prod_meta,
                running_sum_offset=self.last_production_sums.get(key, 0.0),
            )

            if result.get("statistics"):
                self.last_production_sums[key] = result["statistics"][-1]["sum"]
                _LOGGER.info(
                    f"Importing {len(result['statistics'])} production statistics for {key}"
                )
                try:
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(statistic_id),
                        [
                            StatisticData(start=s["start"], sum=s["sum"], state=s["state"])
                            for s in result["statistics"]
                        ],
                    )
                except Exception as e:
                    _LOGGER.error(f"Failed to import production statistics for {key}: {e}")

        # Track when the next update is scheduled so the next-sync sensor can display it
        self.next_update_time = datetime.now(tz=UTC) + self.update_interval


async def async_setup_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up integration entry."""
    scan_interval = entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)

    # Create API client using new architecture
    api_client = EkzApiClient(
        entry.data["user"],
        entry.data["password"],
        entry.data.get("totp_secret"),
        entry.data.get("device_name"),
    )

    coordinator = EkzCoordinator(hass, api_client, scan_interval, entry)

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
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}_prediction")
        for key in coordinator.production_installations:
            statistic_ids.append(f"sensor.electricity_production_ekz_{key}")

        _LOGGER.info("Resetting EKZ statistics for: %s", statistic_ids)

        # Hold the reset lock so any in-progress _async_update_data finishes first,
        # and new updates are blocked until the state is fully cleared.
        async with coordinator._reset_lock:
            if not await _clear_statistics(statistic_ids):
                _LOGGER.error("Cannot clear statistics: no compatible API found in this HA version")
                return

            # Reset in-memory tracking so the next poll starts from contract_start
            coordinator.last_sums = {}
            coordinator.last_production_sums = {}
            coordinator.last_prediction_sums = {}
            coordinator.catching_up = {}
            coordinator.consumption_averages_raw = {}
            # Reset all prediction services
            for prediction_service in coordinator.prediction_services.values():
                prediction_service.reset()
            coordinator.prediction_services = {}
            for meta in (getattr(coordinator, "meta_entities", None) or {}).values():
                meta.set_last_import(None)
            for meta in (getattr(coordinator, "production_meta_entities", None) or {}).values():
                meta.set_last_import(None)

        # Lock released — now it's safe to schedule the re-import
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
