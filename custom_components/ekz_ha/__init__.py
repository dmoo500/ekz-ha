"""Entrypoint."""

import asyncio
import inspect
import logging
import zoneinfo
from copy import deepcopy
from datetime import date, datetime, timedelta
from typing import Any, cast

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
    MEAN_TYPE_NONE = None  # type: ignore[assignment]
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api.client import EkzApiClient
from .api.session import EkzAuthenticationError
from .const import CATCHUP_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN, NORMAL_SCAN_INTERVAL
from .data.validator import day_slots
from .import_status import ImportStatus, get_import_status
from .statistics.consumption import ConsumptionImporter
from .statistics.importer import get_import_start
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
            contract.auszdat,
        )
        return True  # If parsing fails, assume active to avoid data loss


def _make_stat_meta(statistic_id: str) -> StatisticMetaData:
    """Build StatisticMetaData, adding unit_class='energy' when supported (HA 2024.3+)."""
    kwargs = {
        "has_sum": True,
        "source": "recorder",
        "statistic_id": statistic_id,
        "name": None,
        "unit_of_measurement": "kWh",
    }
    if MEAN_TYPE_NONE is None:
        kwargs["has_mean"] = False
    else:
        kwargs["mean_type"] = MEAN_TYPE_NONE
    if "unit_class" in StatisticMetaData.__annotations__:
        kwargs["unit_class"] = "energy"
    return StatisticMetaData(**kwargs)


def _prediction_statistic_id(hass: HomeAssistant, installation_id: str) -> str | None:
    entity_registry = er.async_get(hass)
    unique_id = f"ekz_electricity_consumption_{installation_id}_prediction"
    return cast(str | None, entity_registry.async_get_entity_id("sensor", DOMAIN, unique_id))


def _cache_sum_offsets(
    coordinator: "EkzCoordinator",
    statistic_id: str,
    statistics: list[dict[str, Any]],
    next_import_date: date | None = None,
    running_sum_offset: float = 0.0,
) -> None:
    offsets: dict[date, float] = {}
    for statistic in statistics:
        local_day = statistic["start"].astimezone(ZRH).date()
        offsets.setdefault(local_day, statistic["sum"] - statistic["state"])
    if next_import_date is not None:
        boundary = datetime.combine(next_import_date, datetime.min.time(), tzinfo=ZRH)
        preceding = [statistic for statistic in statistics if statistic["start"] < boundary]
        offsets[next_import_date] = float(preceding[-1]["sum"]) if preceding else running_sum_offset
    if not hasattr(coordinator, "_sum_offsets"):
        coordinator._sum_offsets = {}
    coordinator._sum_offsets[statistic_id] = offsets


async def _async_clear_statistics(hass: HomeAssistant, statistic_ids: list[str]) -> bool:
    try:
        from homeassistant.components.recorder import statistics as recorder_statistics

        clear_fn = getattr(recorder_statistics, "async_clear_statistics", None)
        if clear_fn is not None:
            result = clear_fn(hass, statistic_ids)
            if inspect.isawaitable(result):
                await result
            return True
    except Exception as err:
        _LOGGER.debug("Module-level async_clear_statistics failed: %s", err)

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


async def _persist_source_resolution(
    coordinator: "EkzCoordinator", statistic_id: str, statistics: list[dict[str, Any]]
) -> None:
    sources = dict(getattr(coordinator, "_daily_statistics", {}))
    daily_dates = set(sources.get(statistic_id, []))
    by_day: dict[str, list[dict[str, Any]]] = {}
    for statistic in statistics:
        day = statistic["start"].astimezone(ZRH).date().isoformat()
        by_day.setdefault(day, []).append(statistic)
    for day, rows in by_day.items():
        if any(row.get("source_resolution") == "day" for row in rows):
            daily_dates.add(day)
        else:
            if {row["start"] for row in rows} == set(day_slots(day, minutes=60)):
                daily_dates.discard(day)
    if sorted(daily_dates) == sources.get(statistic_id, []):
        return
    sources[statistic_id] = sorted(daily_dates)
    store = getattr(coordinator, "_source_resolution_store", None)
    if store is not None:
        await store.async_save(sources)
    coordinator._daily_statistics = sources


def _restore_import_checkpoint(coordinator: "EkzCoordinator", statistic_id: str, meta: Any) -> None:
    if getattr(meta, "_checkpoint_restored", False):
        return
    state = getattr(coordinator, "_import_checkpoints", {}).get(statistic_id, {})
    meta._hourly_slots = state.get("slots", {})
    meta._recent_statistics = [
        {
            **row,
            "start": datetime.fromisoformat(row["start"]),
            "end": datetime.fromisoformat(row["end"]),
        }
        for row in state.get("statistics", [])
    ]
    history_cursor = state.get("history_cursor")
    meta._history_cursor = date.fromisoformat(history_cursor) if history_cursor else None
    meta._empty_search_days = state.get("empty_search_days")
    if meta._recent_statistics:
        first = meta._recent_statistics[0]
        _cache_sum_offsets(
            coordinator,
            statistic_id,
            meta._recent_statistics,
            get_import_start(meta),
            float(first["sum"] - first["state"]),
        )
    if state.get("import_status"):
        meta._import_status = ImportStatus.restore(state["import_status"])
    if state.get("last_successful_import"):
        meta._last_successful_import = datetime.fromisoformat(state["last_successful_import"])
    elif meta._recent_statistics:
        meta._last_successful_import = max(row["start"] for row in meta._recent_statistics)
    if state.get("pending_from"):
        pending_date = date.fromisoformat(state["pending_from"])
        pending_offset = float(state["pending_offset"])
        meta.set_pending(pending_date, pending_offset)
        if not hasattr(coordinator, "_sum_offsets"):
            coordinator._sum_offsets = {}
        coordinator._sum_offsets.setdefault(statistic_id, {})[pending_date] = pending_offset
    meta._checkpoint_restored = True
    meta._checkpoint_needs_replay = bool(meta._recent_statistics)


async def _async_replay_import_checkpoint(
    coordinator: "EkzCoordinator", statistic_id: str, meta: Any
) -> None:
    if not getattr(meta, "_checkpoint_needs_replay", False):
        return
    try:
        async_import_statistics(
            coordinator.hass,
            _make_stat_meta(statistic_id),
            [
                StatisticData(start=row["start"], sum=row["sum"], state=row["state"])
                for row in meta._recent_statistics
            ],
        )
        await get_recorder_instance(coordinator.hass).async_block_till_done()
    except Exception as err:
        raise UpdateFailed(f"Checkpoint replay failed for {statistic_id}: {err}") from err
    meta._checkpoint_needs_replay = False


async def _persist_import_checkpoint(
    coordinator: "EkzCoordinator", statistic_id: str, meta: Any, statistics: list[dict[str, Any]]
) -> None:
    slots = dict(getattr(meta, "_hourly_slots", {}))
    daily_dates = {
        row["start"].astimezone(ZRH).date()
        for row in statistics
        if row.get("source_resolution") == "day"
    }
    slots = {
        hour: values
        for hour, values in slots.items()
        if datetime.strptime(hour, "%Y%m%d%H%M%S").replace(tzinfo=UTC).astimezone(ZRH).date()
        not in daily_dates
    }
    for row in statistics:
        hour = row["start"].astimezone(UTC).strftime("%Y%m%d%H%M%S")
        if row.get("slots") and len(row["slots"]) < 4:
            slots[hour] = dict(row["slots"])
        else:
            slots.pop(hour, None)
    pending = getattr(meta, "_pending_from", None)
    state = {
        "slots": slots,
        "import_status": {
            "state": get_import_status(meta).state,
            **get_import_status(meta).as_attributes(),
        },
        "last_successful_import": (
            meta._last_successful_import.isoformat()
            if getattr(meta, "_last_successful_import", None) is not None
            else None
        ),
        "history_cursor": meta._history_cursor.isoformat() if meta._history_cursor else None,
        "empty_search_days": getattr(meta, "_empty_search_days", None),
        "pending_from": pending.isoformat() if pending else None,
        "pending_offset": float(getattr(meta, "_pending_sum_offset", 0.0)),
        "statistics": [
            {
                **row,
                "start": row["start"].isoformat(),
                "end": row.get("end", row["start"] + timedelta(hours=1)).isoformat(),
            }
            for row in (statistics or getattr(meta, "_recent_statistics", []))
        ],
    }
    checkpoints = dict(getattr(coordinator, "_import_checkpoints", {}))
    if checkpoints.get(statistic_id) != state:
        checkpoints[statistic_id] = state
        store = getattr(coordinator, "_import_checkpoint_store", None)
        if store is not None:
            await store.async_save(checkpoints)
        coordinator._import_checkpoints = checkpoints
    meta._hourly_slots = slots


async def _async_import_chunk(
    coordinator: "EkzCoordinator",
    importer: ConsumptionImporter | ProductionImporter,
    installation_id: str,
    contract_start: datetime,
    meta: Any,
    statistic_id: str,
    running_sum_offset: float,
) -> dict[str, Any]:
    tracked_fields = (
        "_last_import",
        "_last_successful_import",
        "_history_cursor",
        "_empty_search_days",
        "_pending_from",
        "_pending_sum_offset",
        "_last_run_date",
        "_hourly_slots",
        "_recent_statistics",
    )
    previous_meta = {
        field: deepcopy(getattr(meta, field)) for field in tracked_fields if hasattr(meta, field)
    }
    previous_sources = deepcopy(getattr(coordinator, "_daily_statistics", {}))
    previous_checkpoints = deepcopy(getattr(coordinator, "_import_checkpoints", {}))
    try:
        result: dict[str, Any] = await importer.import_statistics(
            coordinator.hass,
            installation_id,
            contract_start,
            meta,
            running_sum_offset=running_sum_offset,
        )
        statistics = result.get("statistics", [])
        # The previous checkpoint must reach the recorder before it is replaced.
        await get_recorder_instance(coordinator.hass).async_block_till_done()
        status = get_import_status(meta)
        status.last_result = "imported" if result["fetched_data"] else "no_data"
        requested_to = status.requested_to
        status.state = (
            "searching"
            if getattr(meta, "_empty_search_days", None) is not None
            else (
                "importing"
                if requested_to is None or requested_to <= datetime.now(tz=ZRH).date()
                else "current"
            )
        )
        if statistics:
            timestamps = [row["start"] for row in statistics]
            previous_success = getattr(meta, "_last_successful_import", None)
            if previous_success is not None:
                timestamps.append(previous_success)
            meta._last_successful_import = max(timestamps)
        if statistics:
            await _persist_source_resolution(coordinator, statistic_id, statistics)
        await _persist_import_checkpoint(coordinator, statistic_id, meta, statistics)
        if statistics:
            async_import_statistics(
                coordinator.hass,
                _make_stat_meta(statistic_id),
                [
                    StatisticData(start=row["start"], sum=row["sum"], state=row["state"])
                    for row in statistics
                ],
            )
            meta._recent_statistics = statistics
        _cache_sum_offsets(
            coordinator, statistic_id, statistics, get_import_start(meta), running_sum_offset
        )
        return result
    except Exception as err:
        for attribute, previous, store_name in (
            ("_daily_statistics", previous_sources, "_source_resolution_store"),
            ("_import_checkpoints", previous_checkpoints, "_import_checkpoint_store"),
        ):
            if getattr(coordinator, attribute, {}) != previous:
                store = getattr(coordinator, store_name, None)
                if store is not None:
                    try:
                        await store.async_save(previous)
                    except Exception as restore_err:
                        _LOGGER.error(
                            "Could not restore import metadata for %s: %s",
                            statistic_id,
                            restore_err,
                        )
            setattr(coordinator, attribute, previous)
        for field in tracked_fields:
            if field in previous_meta:
                setattr(meta, field, previous_meta[field])
            elif hasattr(meta, field):
                delattr(meta, field)
        status = get_import_status(meta)
        status.state = "failed"
        status.last_result = "failed"
        status.error_type = type(err).__name__
        checkpoints = deepcopy(previous_checkpoints)
        state = checkpoints.setdefault(statistic_id, {})
        state["import_status"] = {"state": status.state, **status.as_attributes()}
        previous_success = getattr(meta, "_last_successful_import", None)
        state["last_successful_import"] = (
            previous_success.isoformat() if previous_success is not None else None
        )
        try:
            store = getattr(coordinator, "_import_checkpoint_store", None)
            if store is not None:
                await store.async_save(checkpoints)
            coordinator._import_checkpoints = checkpoints
        except Exception as save_err:
            _LOGGER.error(
                "Could not persist failure diagnostics for %s: %s", statistic_id, save_err
            )
        if isinstance(err, EkzAuthenticationError):
            raise ConfigEntryAuthFailed("EKZ authentication failed") from err
        raise UpdateFailed(f"Import failed for {statistic_id}: {err}") from err


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
        self.migrated_prediction_statistics: set[str] = set()
        self.next_update_time: datetime | None = None

    async def _async_setup(self):
        """Load installations on first start."""
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
        # Some accounts expose production on the consumption installation
        # instead of returning it in the separate production contract list.
        for inst_id, info in self.installations.items():
            if inst_id in self.production_installations:
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
        for kind, entities in (
            ("consumption", getattr(self, "meta_entities", None)),
            ("production", getattr(self, "production_meta_entities", None)),
        ):
            for key, meta in (entities or {}).items():
                _restore_import_checkpoint(self, f"sensor.electricity_{kind}_ekz_{key}", meta)

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
            self.production_installations = {
                contract.anlage: {"contract_start": contract.einzdat}
                for contract in production_contracts
            }
            for inst_id, info in self.installations.items():
                if inst_id in self.production_installations:
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

        meta_entities = getattr(self, "meta_entities", None)
        if not meta_entities and not getattr(self, "production_meta_entities", None):
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

            # One-time migration: clear old statistic_id
            old_statistic_id = f"sensor.ekz_electricity_consumption_{key}"
            try:
                old_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, old_statistic_id, True, {"sum"}
                )
                if old_stats and old_statistic_id in old_stats:
                    _LOGGER.info(f"Migrating: clearing old statistics under {old_statistic_id}")
                    if not await _async_clear_statistics(self.hass, [old_statistic_id]):
                        _LOGGER.info(
                            f"async_clear_statistics not available — old statistics under {old_statistic_id} will remain"
                        )
            except Exception as e:
                _LOGGER.debug(f"Migration check failed for {key}: {e}")

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

            prediction_statistic_id = _prediction_statistic_id(self.hass, key)
            legacy_prediction_statistic_id = f"sensor.electricity_consumption_ekz_{key}_prediction"
            if (
                prediction_statistic_id is not None
                and prediction_statistic_id != legacy_prediction_statistic_id
                and key not in self.migrated_prediction_statistics
            ):
                try:
                    old_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        get_last_statistics,
                        self.hass,
                        1,
                        legacy_prediction_statistic_id,
                        True,
                        {"sum"},
                    )
                    if old_stats and legacy_prediction_statistic_id in old_stats:
                        if not await _async_clear_statistics(
                            self.hass, [legacy_prediction_statistic_id]
                        ):
                            raise RuntimeError("No compatible statistics clearing API found")
                        _LOGGER.info(
                            "Cleared outdated prediction statistics for %s (%s)",
                            key,
                            legacy_prediction_statistic_id,
                        )
                    self.migrated_prediction_statistics.add(key)
                except Exception as e:
                    _LOGGER.warning(
                        "Could not migrate outdated prediction statistics for %s: %s",
                        key,
                        e,
                    )

            # Generate predictions only when caught up (gap fills the recent EKZ delay)
            if result.get("statistics") and not still_catching_up:
                predictions = []
                for statistic in result["statistics"]:
                    covered_hour = statistic["start"]
                    coverage_end = statistic.get("end", covered_hour + timedelta(hours=1))
                    while covered_hour < coverage_end:
                        predictions.append({"start": covered_hour, "sum": 0, "state": 0})
                        covered_hour += timedelta(hours=1)

                last_actual_start = max(
                    statistic.get("end", statistic["start"] + timedelta(hours=1))
                    for statistic in result["statistics"]
                ) - timedelta(hours=1)
                last_actual_statistic = next(
                    (
                        statistic
                        for statistic in result["statistics"]
                        if statistic["start"] == last_actual_start
                        and statistic.get("source_resolution") == "hour"
                    ),
                    None,
                )
                delivered_slots = (
                    len(last_actual_statistic.get("slots", {}))
                    if last_actual_statistic is not None
                    else 4
                )
                partial_hour_prediction = prediction_service.estimate_missing_slots(
                    last_actual_start, delivered_slots
                )
                if partial_hour_prediction > 0:
                    predictions = [
                        prediction
                        for prediction in predictions
                        if prediction["start"] != last_actual_start
                    ]
                    predictions.append(
                        {
                            "start": last_actual_start,
                            "sum": partial_hour_prediction,
                            "state": partial_hour_prediction,
                        }
                    )
                forward_predictions = prediction_service.generate_predictions(
                    last_actual_start,
                    datetime.now(tz=UTC),
                    initial_total=partial_hour_prediction,
                )
                predictions.extend(forward_predictions)

                if predictions:
                    if prediction_statistic_id is None:
                        _LOGGER.warning(
                            "Prediction entity for %s is not registered; skipping statistics import",
                            key,
                        )
                    else:
                        _LOGGER.info(
                            f"Predictions for {key}: {len(predictions)} entries, "
                            f"gap coverage {result['statistics'][-1]['start'].date()} → {datetime.now(tz=UTC).date()}"
                        )
                        try:
                            async_import_statistics(
                                self.hass,
                                _make_stat_meta(prediction_statistic_id),
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
            await _async_replay_import_checkpoint(self, statistic_id, prod_meta)
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
            result = await _async_import_chunk(
                self,
                importer,
                key,
                contract_start_dt,
                prod_meta,
                statistic_id,
                production_offset,
            )

            if result.get("statistics"):
                self.last_production_sums[key] = result["statistics"][-1]["sum"]
                _LOGGER.info(
                    f"Importing {len(result['statistics'])} production statistics for {key}"
                )

        # Track when the next update is scheduled so the next-sync sensor can display it
        self.next_update_time = datetime.now(tz=UTC) + self.update_interval


async def async_setup_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up integration entry."""
    scan_interval = entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)

    # Create API client using new architecture
    try:
        api_client = EkzApiClient(
            entry.data["user"],
            entry.data["password"],
            entry.data.get("totp_secret"),
            entry.data.get("device_name"),
        )
    except EkzAuthenticationError as err:
        raise ConfigEntryAuthFailed("Invalid EKZ authentication configuration") from err

    coordinator = EkzCoordinator(hass, api_client, scan_interval, entry)

    hass.data[DOMAIN] = {
        "conf": entry,
        "coordinator": coordinator,
    }
    known_installations = await coordinator._installation_store.async_load()
    try:
        await coordinator.async_config_entry_first_refresh()
    except ConfigEntryNotReady:
        if not known_installations or not any(known_installations.values()):
            await api_client.close()
            raise
        coordinator.installations = known_installations["consumption"]
        coordinator.production_installations = known_installations["production"]
        coordinator._retry_setup = True
        _LOGGER.warning(
            "EKZ startup refresh failed; restoring known installations for diagnostics. "
            "Installation discovery will be retried before imports resume."
        )
    except ConfigEntryAuthFailed:
        await api_client.close()
        raise
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])

    async def handle_reset_statistics(call):
        """Delete all EKZ statistics from the DB and reset in-memory state so a full re-import starts."""
        statistic_ids = []
        for key in coordinator.installations:
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}")
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}_prediction")
            prediction_statistic_id = _prediction_statistic_id(hass, key)
            if prediction_statistic_id is not None:
                statistic_ids.append(prediction_statistic_id)
        for key in coordinator.production_installations:
            statistic_ids.append(f"sensor.electricity_production_ekz_{key}")

        _LOGGER.info("Resetting EKZ statistics for: %s", statistic_ids)

        # Hold the reset lock so any in-progress _async_update_data finishes first,
        # and new updates are blocked until the state is fully cleared.
        async with coordinator._reset_lock:
            if not await _async_clear_statistics(hass, statistic_ids):
                _LOGGER.error("Cannot clear statistics: no compatible API found in this HA version")
                return

            # Reset in-memory tracking so the next poll starts from contract_start
            coordinator.last_sums = {}
            coordinator.last_production_sums = {}
            coordinator.last_prediction_sums = {}
            coordinator._sum_offsets = {}
            coordinator._daily_statistics = {}
            await coordinator._source_resolution_store.async_save({})
            coordinator._import_checkpoints = {}
            await coordinator._import_checkpoint_store.async_save({})
            coordinator.catching_up = {}
            coordinator.consumption_averages_raw = {}
            # Reset all prediction services
            for prediction_service in coordinator.prediction_services.values():
                prediction_service.reset()
            coordinator.prediction_services = {}
            for meta in (getattr(coordinator, "meta_entities", None) or {}).values():
                meta.set_last_import(None)
                meta._last_successful_import = None
                meta._import_status = ImportStatus()
                meta.set_history_cursor(None)
                meta._empty_search_days = None
                meta._recent_statistics = []
                meta._daily_statistics = set()
                meta._hourly_slots = {}
                meta._checkpoint_restored = False
                meta.set_pending(None)
            for meta in (getattr(coordinator, "production_meta_entities", None) or {}).values():
                meta.set_last_import(None)
                meta._last_successful_import = None
                meta._import_status = ImportStatus()
                meta.set_history_cursor(None)
                meta._empty_search_days = None
                meta._recent_statistics = []
                meta._daily_statistics = set()
                meta._hourly_slots = {}
                meta._checkpoint_restored = False
                meta.set_pending(None)

        # Lock released — now it's safe to schedule the re-import
        _LOGGER.info("EKZ statistics reset complete — re-import will start on next poll")
        await coordinator.async_request_refresh()

    hass.services.async_register(DOMAIN, "reset_statistics", handle_reset_statistics)
    entry.async_create_background_task(
        hass,
        coordinator.async_request_refresh(),
        "EKZ initial import",
    )
    return True


async def async_unload_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if unload_ok:
        data = hass.data.pop(DOMAIN, None)
        if data is not None:
            await data["coordinator"].api_client.close()
        hass.services.async_remove(DOMAIN, "reset_statistics")
    return unload_ok
