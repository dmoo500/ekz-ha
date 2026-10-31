from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers import entity_registry as er

from custom_components.ekz_ha.const import DOMAIN
from custom_components.ekz_ha.sensor import _migrate_production_meta_entities
from custom_components.ekz_ha.statistics.recorder import (
    _async_clear_statistics,
    _legacy_prediction_statistic_id,
)


def test_prediction_statistic_id_uses_registered_entity_id(monkeypatch):
    registered_id = "sensor.electricity_consumption_prediction_ekz_825821"
    registry = Mock()
    registry.async_get_entity_id.return_value = registered_id
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.er.async_get", lambda hass: registry
    )

    result = _legacy_prediction_statistic_id(object(), "825821")

    assert result == registered_id
    registry.async_get_entity_id.assert_called_once_with(
        "sensor", DOMAIN, "ekz_electricity_consumption_825821_prediction"
    )


async def test_clear_statistics_uses_recorder_instance_fallback(monkeypatch):
    recorder = Mock(async_clear_statistics=AsyncMock())
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.get_recorder_instance", lambda hass: recorder
    )
    statistic_ids = ["sensor.electricity_consumption_ekz_825821_prediction"]

    assert await _async_clear_statistics(object(), statistic_ids)

    recorder.async_clear_statistics.assert_awaited_once_with(statistic_ids)


@pytest.mark.parametrize("shared_meter", [False, True])
@pytest.mark.parametrize("new_entry_exists", [False, True])
async def test_production_metadata_migration_preserves_entity_id(
    hass, shared_meter, new_entry_exists
):
    registry = er.async_get(hass)
    old_unique_id = "ekz_electricity_consumption_solar_meta"
    new_unique_id = "ekz_electricity_production_solar_meta"
    old = registry.async_get_or_create(
        "sensor", DOMAIN, old_unique_id, suggested_object_id="solar_last_import"
    )
    new = (
        registry.async_get_or_create(
            "sensor", DOMAIN, new_unique_id, suggested_object_id="new_solar_last_import"
        )
        if new_entry_exists
        else None
    )
    coordinator = SimpleNamespace(
        installations={"solar": {}} if shared_meter else {},
        production_installations={"solar": {}},
    )

    _migrate_production_meta_entities(hass, coordinator)

    updated = registry.async_get(old.entity_id)
    assert updated is not None
    if shared_meter or new_entry_exists:
        assert updated.unique_id == old_unique_id
    else:
        assert updated.unique_id == new_unique_id
        assert registry.async_get_entity_id("sensor", DOMAIN, new_unique_id) == old.entity_id
    if new is not None:
        assert registry.async_get(new.entity_id).unique_id == new_unique_id
