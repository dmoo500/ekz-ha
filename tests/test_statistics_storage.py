"""Disk-backed checks for Home Assistant's suppressed storage errors."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import UpdateFailed
from homeassistant.util.file import WriteError

from custom_components.ekz_ha.coordinator import EkzCoordinator
from custom_components.ekz_ha.statistics.storage import (
    _read_saved_document,
    async_load_verified,
    async_save_verified,
)

REAL_WRITE_DATA = Store._async_write_data


async def test_corrupt_state_is_not_renamed_or_replaced(hass, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.storage._read_saved_document", _read_saved_document
    )
    monkeypatch.setattr(hass.config, "config_dir", str(tmp_path))
    store = Store(hass, 1, "corrupt-progress")
    path = tmp_path / ".storage" / store.key
    path.parent.mkdir()
    path.write_text("{broken progress", encoding="utf-8")
    load = AsyncMock()
    monkeypatch.setattr(store, "async_load", load)
    with pytest.raises(UpdateFailed, match="Could not read saved statistics state"):
        await async_load_verified(hass, store)
    load.assert_not_awaited()
    assert path.read_text(encoding="utf-8") == "{broken progress"


@pytest.mark.parametrize("payload", [None, [], "invalid"])
async def test_invalid_saved_payload_cannot_become_empty_migration_state(
    hass, hass_storage, payload
):
    store = Store(hass, 1, "invalid-progress")
    hass_storage[store.key] = {"version": 1, "data": payload}
    with pytest.raises(UpdateFailed, match="Invalid saved statistics state"):
        await async_load_verified(hass, store)


@pytest.mark.parametrize("existing", [False, True])
async def test_verified_save_detects_actual_disk_write_failure(
    hass, tmp_path, monkeypatch, existing
):
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.storage._read_saved_document", _read_saved_document
    )
    monkeypatch.setattr(hass.config, "config_dir", str(tmp_path))
    store = Store(hass, 1, "disk-verification")
    monkeypatch.setattr(store, "_async_write_data", REAL_WRITE_DATA.__get__(store, Store))
    if existing:
        await async_save_verified(hass, store, {"reset_pending": False})
    with monkeypatch.context() as failing:
        failing.setattr(store, "_write_data", Mock(side_effect=WriteError("disk unavailable")))
        if hasattr(store, "_write_prepared_data"):
            failing.setattr(
                store, "_write_prepared_data", Mock(side_effect=WriteError("disk unavailable"))
            )
        with pytest.raises(UpdateFailed, match="verify saved|not saved correctly"):
            await async_save_verified(hass, store, {"reset_pending": True})
    if existing:
        assert _read_saved_document(store.path)["data"] == {"reset_pending": False}
    await async_save_verified(hass, store, {"reset_pending": True})
    assert _read_saved_document(store.path)["data"] == {"reset_pending": True}


async def test_saved_state_load_failure_does_not_use_offline_setup_fallback(
    hass, monkeypatch, hass_storage
):
    from custom_components.ekz_ha import async_setup_entry

    entry = Mock(entry_id="load-failure", data={"user": "test", "password": "test"})
    hass_storage["ekz_ha.load-failure.installations"] = {
        "version": 1,
        "data": {"consumption": {"test": {"contract_start": "2024-03-10"}}, "production": {}},
    }
    saved = {
        "ekz_ha:electricity_consumption_test": {
            "sources": ["sensor.renamed_energy"],
            "reset_pending": True,
            "complete": False,
        }
    }
    hass_storage["ekz_ha.load-failure.external_statistics"] = {"version": 1, "data": saved}
    api = Mock(close=AsyncMock())
    monkeypatch.setattr("custom_components.ekz_ha.EkzApiClient", Mock(return_value=api))

    async def first_refresh(coordinator):
        coordinator._external_migration_store.async_load = AsyncMock(
            side_effect=OSError("cannot read migration file")
        )
        try:
            await coordinator._async_setup()
        except OSError as err:
            raise ConfigEntryNotReady from err

    monkeypatch.setattr(EkzCoordinator, "async_config_entry_first_refresh", first_refresh)
    initialize = AsyncMock()
    monkeypatch.setattr("custom_components.ekz_ha.async_initialize_migration", initialize)
    with patch.object(
        hass.config_entries, "async_forward_entry_setups", new=AsyncMock()
    ) as forward:
        with pytest.raises(ConfigEntryNotReady):
            await async_setup_entry(hass, entry)
    initialize.assert_not_awaited()
    forward.assert_not_awaited()
    api.close.assert_awaited_once()
    assert hass_storage["ekz_ha.load-failure.external_statistics"]["data"] == saved
