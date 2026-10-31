"""Integration setup, service registration and teardown."""

import logging
from typing import Literal

from homeassistant import core
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.start import async_at_started

from .api.client import EkzApiClient
from .api.session import EkzAuthenticationError
from .const import DEFAULT_SCAN_INTERVAL, DOMAIN
from .coordinator import EkzCoordinator
from .import_status import ImportStatus
from .statistics.migration import async_initialize_migration
from .statistics.recorder import (
    _async_clear_statistics,
    _energy_statistic_id,
    _legacy_energy_statistic_id,
    _legacy_prediction_statistic_id,
    _prediction_statistic_id,
    _validate_legacy_statistic_ids,
)

_LOGGER = logging.getLogger(__name__)


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
    try:
        await async_initialize_migration(coordinator)
    except Exception as err:
        await api_client.close()
        _LOGGER.error("Cannot prepare external statistics migration: %s", err)
        raise ConfigEntryNotReady("Cannot persist external statistics migration") from err
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])

    async def handle_reset_statistics(call):
        """Delete all EKZ statistics from the DB and reset in-memory state so a full re-import starts."""
        # Hold the reset lock so any in-progress _async_update_data finishes first,
        # and new updates are blocked until the state is fully cleared.
        async with coordinator._reset_lock:
            candidates = {
                target: set(migration["sources"])
                for target, migration in coordinator._external_migrations.items()
            }
            kinds: tuple[Literal["consumption", "prediction", "production"], ...] = (
                "consumption",
                "prediction",
                "production",
            )
            for kind in kinds:
                installations = (
                    coordinator.production_installations
                    if kind == "production"
                    else coordinator.installations
                )
                for key in installations:
                    if kind == "prediction":
                        target = _prediction_statistic_id(hass, key)
                        registered = _legacy_prediction_statistic_id(hass, key)
                        fixed = f"sensor.electricity_consumption_ekz_{key}_prediction"
                    else:
                        target = _energy_statistic_id(hass, key, kind)
                        registered = _legacy_energy_statistic_id(hass, key, kind)
                        fixed = f"sensor.electricity_{kind}_ekz_{key}"
                    sources = candidates.setdefault(target, set())
                    sources.add(fixed)
                    if registered:
                        sources.add(registered)
            for saved_key, source in coordinator._statistic_ids.items():
                saved_kind, key = saved_key.split(":", 1)
                target = (
                    _prediction_statistic_id(hass, key)
                    if saved_kind == "prediction"
                    else f"{DOMAIN}:electricity_{saved_kind}_{key}"
                )
                candidates.setdefault(target, set()).add(source)
            for target, sources in candidates.items():
                _validate_legacy_statistic_ids(hass, target, sources, entry.entry_id)
            statistic_ids = sorted(
                {
                    statistic_id
                    for target, sources in candidates.items()
                    for statistic_id in sources | {target}
                }
            )
            _LOGGER.info("Resetting EKZ statistics for: %s", statistic_ids)
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
            coordinator._external_migrations = {
                target: {**state, "complete": True, "tail": None}
                for target, state in coordinator._external_migrations.items()
            }
            await coordinator._external_migration_store.async_save(coordinator._external_migrations)
            coordinator.catching_up = {}
            coordinator.consumption_averages_raw = {}
            # Reset all prediction services
            for prediction_service in coordinator.prediction_services.values():
                prediction_service.reset()
            coordinator.prediction_services = {}
            coordinator._prediction_states = {}
            coordinator._prediction_initialized.clear()
            await coordinator._prediction_store.async_save({})
            for meta in (getattr(coordinator, "meta_entities", None) or {}).values():
                meta.set_last_import(None)
                meta._last_successful_import = None
                meta._import_status = ImportStatus()
                meta.set_history_cursor(None)
                meta._empty_search_days = None
                meta._empty_search_from = None
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
                meta._empty_search_from = None
                meta._recent_statistics = []
                meta._daily_statistics = set()
                meta._hourly_slots = {}
                meta._checkpoint_restored = False
                meta.set_pending(None)

        # Lock released — now it's safe to schedule the re-import
        _LOGGER.info("EKZ statistics deletion verified — requesting re-import from contract start")
        await coordinator.async_request_refresh()

    hass.services.async_register(DOMAIN, "reset_statistics", handle_reset_statistics)

    entry.async_on_unload(
        async_track_time_change(hass, coordinator.async_extend_predictions, minute=1, second=0)
    )

    async def start_initial_import(_hass: core.HomeAssistant) -> None:
        entry.async_create_background_task(
            hass, coordinator.async_request_refresh(), "EKZ initial import"
        )

    entry.async_on_unload(async_at_started(hass, start_initial_import))
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
