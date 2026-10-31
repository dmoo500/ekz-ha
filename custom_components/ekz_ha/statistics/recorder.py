"""Home Assistant recorder metadata, statistic IDs, writes and synchronization."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Literal, cast

from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    async_import_statistics,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.recorder import get_instance as get_recorder_instance

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
        "source": DOMAIN if statistic_id.startswith(f"{DOMAIN}:") else "recorder",
        "statistic_id": statistic_id,
        "name": None,
        "unit_of_measurement": "kWh",
    }
    if statistic_id.startswith(f"{DOMAIN}:"):
        object_id = statistic_id.split(":", 1)[1]
        label, installation_id = object_id.rsplit("_", 1)
        if installation_id == "prediction":
            label, installation_id = label.rsplit("_", 1)
            label = "Electricity consumption prediction"
        else:
            label = label.replace("_", " ").capitalize()
        kwargs["name"] = f"EKZ {installation_id} {label}"
    if MEAN_TYPE_NONE is None:
        kwargs["has_mean"] = False
    else:
        kwargs["mean_type"] = MEAN_TYPE_NONE
    if "unit_class" in StatisticMetaData.__annotations__:
        kwargs["unit_class"] = "energy"
    return StatisticMetaData(**kwargs)


def _legacy_prediction_statistic_id(hass: HomeAssistant, installation_id: str) -> str | None:
    entity_registry = er.async_get(hass)
    unique_id = f"ekz_electricity_consumption_{installation_id}_prediction"
    return cast(str | None, entity_registry.async_get_entity_id("sensor", DOMAIN, unique_id))


def _legacy_energy_statistic_id(
    hass: HomeAssistant, installation_id: str, kind: Literal["consumption", "production"]
) -> str | None:
    registry = er.async_get(hass)
    return cast(
        str | None,
        registry.async_get_entity_id("sensor", DOMAIN, f"ekz_electricity_{kind}_{installation_id}"),
    )


def _prediction_statistic_id(hass: HomeAssistant, installation_id: str) -> str:
    return f"{DOMAIN}:electricity_consumption_{installation_id}_prediction"


def _energy_statistic_id(
    hass: HomeAssistant, installation_id: str, kind: Literal["consumption", "production"]
) -> str:
    return f"{DOMAIN}:electricity_{kind}_{installation_id}"


def _async_write_statistics(
    hass: HomeAssistant, metadata: StatisticMetaData, rows: list[StatisticData]
) -> None:
    if metadata["source"] == DOMAIN:
        async_add_external_statistics(hass, metadata, rows)
    else:
        async_import_statistics(hass, metadata, rows)


async def _async_wait_for_statistics(hass: HomeAssistant, statistic_id: str) -> None:
    """Wait for queued imports via the recorder's public completion callback."""
    future = hass.loop.create_future()

    def complete() -> None:
        if not future.done():
            future.set_result(None)

    def on_done() -> None:
        hass.loop.call_soon_threadsafe(complete)

    get_recorder_instance(hass).async_update_statistics_metadata(statistic_id, on_done=on_done)
    await asyncio.wait_for(asyncio.shield(future), timeout=60)


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
