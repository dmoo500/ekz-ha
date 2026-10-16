from unittest.mock import AsyncMock, Mock

from custom_components.ekz_ha import _async_clear_statistics, _prediction_statistic_id
from custom_components.ekz_ha.const import DOMAIN


def test_prediction_statistic_id_uses_registered_entity_id(monkeypatch):
    registered_id = "sensor.electricity_consumption_prediction_ekz_825821"
    registry = Mock()
    registry.async_get_entity_id.return_value = registered_id
    monkeypatch.setattr("custom_components.ekz_ha.er.async_get", lambda hass: registry)

    result = _prediction_statistic_id(object(), "825821")

    assert result == registered_id
    registry.async_get_entity_id.assert_called_once_with(
        "sensor", DOMAIN, "ekz_electricity_consumption_825821_prediction"
    )


async def test_clear_statistics_uses_recorder_instance_fallback(monkeypatch):
    recorder = Mock(async_clear_statistics=AsyncMock())
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    statistic_ids = ["sensor.electricity_consumption_ekz_825821_prediction"]

    assert await _async_clear_statistics(object(), statistic_ids)

    recorder.async_clear_statistics.assert_awaited_once_with(statistic_ids)
