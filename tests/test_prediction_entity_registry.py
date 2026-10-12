from unittest.mock import Mock

from custom_components.ekz_ha import _prediction_statistic_id
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
