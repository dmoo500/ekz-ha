"""Home Assistant recorder metadata, entity IDs and clearing compatibility."""

import inspect
import logging
from typing import cast

from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import StatisticMetaData
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from ..const import DOMAIN

try:
    from homeassistant.components.recorder.models import StatisticMeanType

    MEAN_TYPE_NONE = StatisticMeanType.NONE
except ImportError:
    MEAN_TYPE_NONE = None  # type: ignore[assignment]

_LOGGER = logging.getLogger(__name__)


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
