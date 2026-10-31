"""Home Assistant recorder metadata, entity IDs and clearing compatibility."""

from __future__ import annotations

import asyncio
import inspect
import logging
from functools import partial
from typing import TYPE_CHECKING, Literal, cast

from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import StatisticMetaData
from homeassistant.components.recorder.statistics import get_metadata
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import UpdateFailed

from ..const import DOMAIN

if TYPE_CHECKING:
    from ..coordinator import EkzCoordinator

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


def _energy_statistic_id(
    hass: HomeAssistant, installation_id: str, kind: Literal["consumption", "production"]
) -> str | None:
    registry = er.async_get(hass)
    return registry.async_get_entity_id(
        "sensor", DOMAIN, f"ekz_electricity_{kind}_{installation_id}"
    )


async def _async_prepare_energy_statistic(
    coordinator: EkzCoordinator,
    installation_id: str,
    kind: Literal["consumption", "production"],
) -> str:
    """Resolve the entity and move existing history before resuming imports."""
    statistic_id = _energy_statistic_id(coordinator.hass, installation_id, kind)
    if statistic_id is None:
        raise UpdateFailed(f"EKZ {kind} entity is not registered for {installation_id}")

    key = f"{kind}:{installation_id}"
    previous_id = coordinator._statistic_ids.get(
        key, f"sensor.electricity_{kind}_ekz_{installation_id}"
    )
    if previous_id == statistic_id and coordinator._statistic_ids.get(key) == statistic_id:
        return statistic_id

    recorder = get_recorder_instance(coordinator.hass)
    if previous_id != statistic_id:
        for attribute in ("_import_checkpoints", "_daily_statistics"):
            states = getattr(coordinator, attribute)
            if (
                previous_id in states
                and statistic_id in states
                and states[statistic_id] != states[previous_id]
            ):
                raise UpdateFailed(
                    f"Conflicting EKZ {attribute} for {previous_id} and {statistic_id}"
                )
        await recorder.async_block_till_done()
        metadata = await recorder.async_add_executor_job(
            partial(get_metadata, coordinator.hass, statistic_ids={previous_id, statistic_id})
        )
        if previous_id in metadata:
            if statistic_id in metadata:
                raise UpdateFailed(
                    f"Cannot migrate EKZ statistics from {previous_id} to {statistic_id}: "
                    "both IDs already have statistics metadata; history was left unchanged"
                )
            completed = coordinator.hass.loop.create_future()

            def migration_done() -> None:
                coordinator.hass.loop.call_soon_threadsafe(completed.set_result, None)

            recorder.async_update_statistics_metadata(
                previous_id,
                new_statistic_id=statistic_id,
                on_done=migration_done,
            )
            try:
                async with asyncio.timeout(60):
                    await asyncio.shield(completed)
            except TimeoutError as err:
                raise UpdateFailed(
                    f"Timed out migrating EKZ statistics from {previous_id} to {statistic_id}"
                ) from err
            migrated = await recorder.async_add_executor_job(
                partial(get_metadata, coordinator.hass, statistic_ids={previous_id, statistic_id})
            )
            if previous_id in migrated or statistic_id not in migrated:
                raise UpdateFailed(
                    f"EKZ statistics migration from {previous_id} to {statistic_id} failed"
                )
            _LOGGER.info("Moved EKZ statistics from %s to %s", previous_id, statistic_id)

        for attribute, store in (
            ("_import_checkpoints", coordinator._import_checkpoint_store),
            ("_daily_statistics", coordinator._source_resolution_store),
        ):
            states = getattr(coordinator, attribute)
            if previous_id in states:
                updated = {**states, statistic_id: states[previous_id]}
                del updated[previous_id]
                try:
                    await store.async_save(updated)
                except OSError as err:
                    raise UpdateFailed(
                        f"Could not save EKZ {attribute} migration to {statistic_id}: {err}"
                    ) from err
                setattr(coordinator, attribute, updated)
        if previous_id in coordinator._sum_offsets:
            coordinator._sum_offsets[statistic_id] = coordinator._sum_offsets.pop(previous_id)

    updated_ids = {**coordinator._statistic_ids, key: statistic_id}
    try:
        await coordinator._statistic_id_store.async_save(updated_ids)
    except OSError as err:
        raise UpdateFailed(f"Could not save EKZ statistic ID {statistic_id}: {err}") from err
    coordinator._statistic_ids = updated_ids
    return statistic_id


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
