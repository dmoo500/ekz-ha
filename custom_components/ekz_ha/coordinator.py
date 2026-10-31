"""Coordinate installation discovery, imports and adaptive polling."""

import asyncio
import logging
import zoneinfo
from datetime import date, datetime, timedelta
from typing import Any

from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.statistics import (
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api.client import EkzApiClient
from .api.session import EkzAuthenticationError
from .const import CATCHUP_SCAN_INTERVAL, DOMAIN, NORMAL_SCAN_INTERVAL
from .import_status import get_import_status, publish_import_status
from .statistics.checkpoint import (
    _async_import_chunk,
    _async_replay_import_checkpoint,
    _persist_import_checkpoint,
    _restore_energy_import_checkpoints,
    _restore_import_checkpoint,
)
from .statistics.consumption import ConsumptionImporter
from .statistics.importer import get_import_start
from .statistics.migration import _async_prepare_energy_statistic
from .statistics.prediction import PredictionService
from .statistics.prediction_manager import async_extend_predictions, async_update_predictions
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
            contract.auszdat,
        )
        return True  # If parsing fails, assume active to avoid data loss


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
        self._sum_offsets: dict[str, dict[date, float]] = {}
        self._daily_statistics: dict[str, list[str]] = {}
        self._source_resolution_store = Store[dict[str, list[str]]](
            hass, 1, f"{DOMAIN}.{config.entry_id}.source_resolution"
        )
        self._import_checkpoints: dict[str, dict[str, Any]] = {}
        self._statistic_ids: dict[str, str] = {}
        self._external_migrations: dict[str, dict[str, Any]] = {}
        self._external_migration_store = Store[dict[str, dict[str, Any]]](
            hass, 1, f"{DOMAIN}.{config.entry_id}.external_statistics"
        )
        self._statistic_id_store = Store[dict[str, str]](
            hass, 1, f"{DOMAIN}.{config.entry_id}.statistic_ids"
        )
        self._import_checkpoint_store = Store[dict[str, dict[str, Any]]](
            hass, 1, f"{DOMAIN}.{config.entry_id}.import_checkpoint"
        )
        self._installation_store = Store[dict[str, dict[str, Any]]](
            hass, 1, f"{DOMAIN}.{config.entry_id}.installations"
        )
        self._retry_setup = False
        self.last_prediction_sums: dict[str, float] = {}
        self.catching_up: dict[str, bool] = {}
        self._normal_interval = update_interval  # remember configured interval for later restore
        self._reset_lock = asyncio.Lock()
        self.consumption_averages_raw: dict[str, dict] = {}  # accumulated slot sums for prediction
        self.prediction_services: dict[
            str, PredictionService
        ] = {}  # prediction service per installation
        self.next_update_time: datetime | None = None
        self._prediction_store = Store[dict[str, dict[str, Any]]](
            hass, 1, f"{DOMAIN}.{config.entry_id}.prediction"
        )
        self._prediction_states: dict[str, dict[str, Any]] | None = None
        self._prediction_initialized: set[str] = set()

    async def async_extend_predictions(self, now: datetime) -> None:
        """Extend predictions using the same lock as imports and resets."""
        await async_extend_predictions(self, now)

    async def _async_setup(self):
        """Load installations on first start."""
        self._statistic_ids = await self._statistic_id_store.async_load() or {}
        self._external_migrations = await self._external_migration_store.async_load() or {}
        store = getattr(self, "_source_resolution_store", None)
        if store is not None:
            self._daily_statistics = await store.async_load() or {}
        checkpoint_store = getattr(self, "_import_checkpoint_store", None)
        if checkpoint_store is not None:
            self._import_checkpoints = await checkpoint_store.async_load() or {}
        try:
            await self._async_discover_installations()
        except EkzAuthenticationError as err:
            raise ConfigEntryAuthFailed("EKZ authentication failed") from err

    async def _async_discover_installations(self):
        """Discover and persist the currently active installations."""
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
            contract.anlage: {"contract_start": contract.einzdat} for contract in active_contracts
        }
        consumption_vstellen = {
            contract.anlage: contract.vstelle for contract in active_contracts if contract.anlage
        }

        _LOGGER.info(
            "Found %d active installation(s): %s",
            len(self.installations),
            list(self.installations.keys()),
        )

        # Production installations have their own contract list and may use a
        # different meter than the consumption contract.
        production_data = await self.api_client.get_production_installations()
        production_contracts = [
            contract
            for contract in production_data.contracts
            if contract.anlage and _is_contract_active(contract)
        ]
        self.production_installations = {
            contract.anlage: {"contract_start": contract.einzdat}
            for contract in production_contracts
        }
        production_vstellen = {
            contract.vstelle for contract in production_contracts if contract.vstelle
        }
        # Some accounts expose production on the consumption installation
        # instead of returning it in the separate production contract list.
        for inst_id, info in self.installations.items():
            if inst_id in self.production_installations:
                continue
            vstelle = consumption_vstellen.get(inst_id)
            if vstelle and vstelle in production_vstellen:
                _LOGGER.debug(
                    "Skipping production probe for %s: supply point %s has a production contract",
                    inst_id,
                    vstelle,
                )
                continue
            try:
                test_data = await self.api_client.get_production_15min(
                    inst_id, datetime.now(tz=ZRH) - timedelta(days=7), datetime.now(tz=ZRH)
                )
                if not test_data.is_empty():
                    self.production_installations[inst_id] = info.copy()
                    _LOGGER.info(f"Production data available for installation {inst_id}")
            except EkzAuthenticationError:
                raise
            except Exception as e:
                _LOGGER.debug(f"No production data for installation {inst_id}: {e}")

        _LOGGER.debug(
            f"Production installations found: {list(self.production_installations.keys())}"
        )
        if not self.installations and not self.production_installations:
            raise UpdateFailed("No active consumption or production installations found")
        installation_store = getattr(self, "_installation_store", None)
        if installation_store is not None:
            await installation_store.async_save(
                {
                    "consumption": self.installations,
                    "production": self.production_installations,
                }
            )
        self._retry_setup = False

    async def _async_update_data(self):
        """Acquire reset lock then delegate to _do_update_data."""
        async with self._reset_lock:
            try:
                if self._retry_setup:
                    await self._async_setup()
                    registered_ids = (
                        set(getattr(self, "meta_entities", None) or {}),
                        set(getattr(self, "production_meta_entities", None) or {}),
                    )
                    current_ids = (set(self.installations), set(self.production_installations))
                    if registered_ids != current_ids:
                        self.hass.async_create_task(
                            self.hass.config_entries.async_reload(self.config.entry_id)
                        )
                        return
                await self._do_update_data()
            except EkzAuthenticationError as err:
                raise ConfigEntryAuthFailed("EKZ authentication failed") from err

    async def _get_running_sum_before(
        self, statistic_id: str, import_date: date, fallback: float = 0.0
    ) -> float:
        cached = getattr(self, "_sum_offsets", {}).get(statistic_id, {})
        if import_date in cached:
            return float(cached[import_date])
        boundary = (
            datetime.combine(import_date, datetime.min.time()).replace(tzinfo=ZRH).astimezone(UTC)
        )
        try:
            for lookback in (timedelta(hours=26), timedelta(days=30), timedelta(days=3660)):
                pre_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                    statistics_during_period,
                    self.hass,
                    boundary - lookback,
                    boundary,
                    {statistic_id},
                    "hour",
                    None,
                    {"sum"},
                )
                if pre_stats and statistic_id in pre_stats and pre_stats[statistic_id]:
                    return float(pre_stats[statistic_id][-1]["sum"])
        except Exception as err:
            _LOGGER.debug("Could not query DB offset for %s: %s", statistic_id, err)
            return fallback
        return fallback

    async def _do_update_data(self):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        _restore_energy_import_checkpoints(self)

        if self.installations is None or self.installations == []:
            installations_data = await self.api_client.get_consumption_installations()

            # Filter for active contracts only
            active_contracts = [
                c for c in installations_data.contracts if c.anlage and _is_contract_active(c)
            ]

            self.installations = {
                contract.anlage: {"contract_start": contract.einzdat}
                for contract in active_contracts
            }

        if not self.production_installations:
            production_data = await self.api_client.get_production_installations()
            production_contracts = [
                contract
                for contract in production_data.contracts
                if contract.anlage and _is_contract_active(contract)
            ]
            discovered_production = {
                contract.anlage: {"contract_start": contract.einzdat}
                for contract in production_contracts
            }
            production_vstellen = {
                contract.vstelle for contract in production_contracts if contract.vstelle
            }
            consumption_vstellen = {}
            if production_vstellen and self.installations:
                consumption_data = await self.api_client.get_consumption_installations()
                consumption_vstellen = {
                    contract.anlage: contract.vstelle
                    for contract in consumption_data.contracts
                    if contract.anlage and _is_contract_active(contract)
                }
            for inst_id, info in self.installations.items():
                if inst_id in discovered_production:
                    continue
                vstelle = consumption_vstellen.get(inst_id)
                if vstelle and vstelle in production_vstellen:
                    continue
                try:
                    test_data = await self.api_client.get_production_15min(
                        inst_id, datetime.now(tz=ZRH) - timedelta(days=7), datetime.now(tz=ZRH)
                    )
                    if not test_data.is_empty():
                        discovered_production[inst_id] = info.copy()
                        _LOGGER.info(f"Production data available for installation {inst_id}")
                except EkzAuthenticationError:
                    raise
                except Exception as e:
                    _LOGGER.debug(f"No production data for installation {inst_id}: {e}")
            self.production_installations = discovered_production

        meta_entities = getattr(self, "meta_entities", None)
        if not meta_entities and not getattr(self, "production_meta_entities", None):
            _LOGGER.debug(
                "meta_entities not yet available during update — entities not initialized yet."
            )
            return

        successful_imports = 0
        production_errors: list[UpdateFailed] = []

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

            try:
                statistic_id = await _async_prepare_energy_statistic(self, key, "consumption")
            except UpdateFailed as err:
                status = get_import_status(meta_entity)
                status.state = "failed"
                status.last_result = "failed"
                status.error_type = type(err.__cause__ or err).__name__
                publish_import_status(meta_entity)
                _LOGGER.error("Consumption statistic migration failed for %s: %s", key, err)
                raise
            _restore_import_checkpoint(self, statistic_id, meta_entity)
            await _async_replay_import_checkpoint(self, statistic_id, meta_entity)

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
                            meta_entity.set_last_import(import_date)
                            meta_entity._last_successful_import = import_dt

                            # Pre-initialize catching_up flag
                            today_date = datetime.now(tz=ZRH).date()
                            if (today_date - import_date).days <= 1:
                                self.catching_up[key] = False
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing statistics for {key}: {e}")

            # Rebuild the running sum before the day being re-imported.
            pending_from = getattr(meta_entity, "_pending_from", None)
            _last_import_date = get_import_start(meta_entity)
            if _last_import_date is not None:
                if isinstance(_last_import_date, datetime):
                    _last_import_date = _last_import_date.date()
                running_sum = await self._get_running_sum_before(
                    statistic_id,
                    _last_import_date,
                    (
                        meta_entity._pending_sum_offset
                        if pending_from is not None
                        else self.last_sums.get(key, 0.0)
                    ),
                )
                _LOGGER.debug(
                    "DB offset for %s before %s: %.3f kWh",
                    key,
                    _last_import_date,
                    running_sum,
                )
            else:
                running_sum = 0.0

            # Import using new architecture
            meta_entity._daily_statistics = set(
                getattr(self, "_daily_statistics", {}).get(statistic_id, [])
            )
            importer = ConsumptionImporter(
                self.api_client, daily_fallback_days=meta_entity._daily_statistics
            )
            contract_start_dt = (
                datetime.strptime(contract_start, "%Y-%m-%d")
                if isinstance(contract_start, str)
                else datetime.combine(contract_start, datetime.min.time())
            )
            result = await _async_import_chunk(
                self,
                importer,
                key,
                contract_start_dt,
                meta_entity,
                statistic_id,
                running_sum,
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

            await async_update_predictions(self, key, result, still_catching_up)

            successful_imports += 1

        # --- Production (solar feed-in) import loop ---
        production_meta_entities = getattr(self, "production_meta_entities", {})
        for key, info in self.production_installations.items():
            prod_meta = production_meta_entities.get(key) if production_meta_entities else None
            if prod_meta is None:
                continue

            contract_start = info.get("contract_start")
            if prod_meta._contract_start is None and contract_start:
                prod_meta.set_contract_start(datetime.strptime(contract_start, "%Y-%m-%d").date())

            try:
                statistic_id = await _async_prepare_energy_statistic(self, key, "production")
            except EkzAuthenticationError as err:
                raise ConfigEntryAuthFailed("EKZ authentication failed") from err
            except ConfigEntryAuthFailed:
                raise
            except Exception as err:
                status = get_import_status(prod_meta)
                status.state = "failed"
                status.last_result = "failed"
                status.error_type = type(err.__cause__ or err).__name__
                publish_import_status(prod_meta)
                _LOGGER.error("Production statistic migration failed for %s: %s", key, err)
                failure = err
                if not isinstance(failure, UpdateFailed):
                    failure = UpdateFailed(
                        f"Production statistic migration failed for {key}: {err}"
                    )
                    failure.__cause__ = err
                production_errors.append(failure)
                continue
            try:
                _restore_import_checkpoint(self, statistic_id, prod_meta)
                await _async_replay_import_checkpoint(self, statistic_id, prod_meta)
            except UpdateFailed as err:
                status = get_import_status(prod_meta)
                status.state = "failed"
                status.last_result = "failed"
                status.error_type = type(err.__cause__ or err).__name__
                try:
                    await _persist_import_checkpoint(self, statistic_id, prod_meta, [])
                except Exception as save_err:
                    _LOGGER.error(
                        "Could not persist failure diagnostics for %s: %s",
                        statistic_id,
                        save_err,
                    )
                publish_import_status(prod_meta)
                _LOGGER.error("Production checkpoint replay failed for %s: %s", key, err)
                production_errors.append(err)
                continue
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
                            prod_meta.set_last_import(import_date)  # Already subtracted 1 day above
                            prod_meta._last_successful_import = import_dt
                            if last_stat_data[0].get("sum") is not None:
                                self.last_production_sums[key] = last_stat_data[0]["sum"]
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing production statistics for {key}: {e}")

            pending_from = getattr(prod_meta, "_pending_from", None)
            production_import_date = get_import_start(prod_meta)
            if isinstance(production_import_date, datetime):
                production_import_date = production_import_date.date()
            production_offset = (
                await self._get_running_sum_before(
                    statistic_id,
                    production_import_date,
                    (
                        prod_meta._pending_sum_offset
                        if pending_from is not None
                        else self.last_production_sums.get(key, 0.0)
                    ),
                )
                if production_import_date is not None
                else 0.0
            )

            # Import using new architecture
            prod_meta._daily_statistics = set(
                getattr(self, "_daily_statistics", {}).get(statistic_id, [])
            )
            importer = ProductionImporter(self.api_client)
            contract_start_dt = (
                datetime.strptime(contract_start, "%Y-%m-%d")
                if isinstance(contract_start, str)
                else datetime.combine(contract_start, datetime.min.time())
            )
            try:
                result = await _async_import_chunk(
                    self,
                    importer,
                    key,
                    contract_start_dt,
                    prod_meta,
                    statistic_id,
                    production_offset,
                )
            except ConfigEntryAuthFailed:
                raise
            except UpdateFailed as err:
                _LOGGER.error("Production import failed for %s: %s", key, err)
                production_errors.append(err)
                continue
            successful_imports += 1

            if result.get("statistics"):
                self.last_production_sums[key] = result["statistics"][-1]["sum"]
                _LOGGER.info(
                    f"Importing {len(result['statistics'])} production statistics for {key}"
                )

        if production_errors and not successful_imports:
            raise production_errors[0]

        # Track when the next update is scheduled so the next-sync sensor can display it
        self.next_update_time = datetime.now(tz=UTC) + self.update_interval
