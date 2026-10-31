"""Resumable migration of entity statistics to external EKZ statistics."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import (
    get_last_statistics,
    get_metadata,
    statistics_during_period,
)
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.recorder import get_instance as get_recorder_instance
from homeassistant.helpers.update_coordinator import UpdateFailed

from ..api.session import EkzAuthenticationError
from ..const import DOMAIN
from .recorder import (
    _async_wait_for_statistics,
    _async_write_statistics,
    _energy_statistic_id,
    _legacy_energy_statistic_id,
    _legacy_prediction_statistic_id,
    _make_stat_meta,
    _prediction_statistic_id,
)

if TYPE_CHECKING:
    from ..coordinator import EkzCoordinator

_LOGGER = logging.getLogger(__name__)


def _statistics_by_start(
    statistic_id: str, rows: Iterable[Mapping[str, Any]]
) -> dict[float, tuple[float | None, float]]:
    result = {}
    for row in rows:
        start, state, total = row.get("start"), row.get("state"), row.get("sum")
        if (
            not isinstance(start, (int, float))
            or not isinstance(total, (int, float))
            or (state is not None and not isinstance(state, (int, float)))
        ):
            raise UpdateFailed(f"Invalid hourly statistics in {statistic_id}: {row}")
        result[float(start)] = (state, total)
    return result


async def async_initialize_migration(coordinator: EkzCoordinator) -> None:
    """Persist legacy IDs before the sensor platform removes obsolete entities."""
    states = dict(coordinator._external_migrations)
    legacy_entities = []
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
        for installation_id in installations:
            if kind == "prediction":
                target = _prediction_statistic_id(coordinator.hass, installation_id)
                registered = _legacy_prediction_statistic_id(coordinator.hass, installation_id)
                fixed = f"sensor.electricity_consumption_ekz_{installation_id}_prediction"
            else:
                target = _energy_statistic_id(coordinator.hass, installation_id, kind)
                registered = _legacy_energy_statistic_id(coordinator.hass, installation_id, kind)
                fixed = f"sensor.electricity_{kind}_ekz_{installation_id}"
            sources = {
                fixed,
                coordinator._statistic_ids.get(f"{kind}:{installation_id}", fixed),
            }
            if kind == "consumption":
                sources.add(f"sensor.ekz_electricity_consumption_{installation_id}")
            if registered:
                sources.add(registered)
                legacy_entities.append(registered)
            previous = states.get(target)
            if previous is None:
                states[target] = {
                    "sources": sorted(sources - {target}),
                    "cursor": "1970-01-01T00:00:00+00:00",
                    "end": datetime.now(UTC).isoformat(),
                    "complete": False,
                }
    await coordinator._external_migration_store.async_save(states)
    coordinator._external_migrations = states
    if legacy_entities:
        ir.async_create_issue(
            coordinator.hass,
            DOMAIN,
            f"external_statistics_{coordinator.config.entry_id}",
            is_fixable=False,
            is_persistent=True,
            severity=ir.IssueSeverity.WARNING,
            translation_key="external_statistics",
            translation_placeholders={
                "statistics": ", ".join(sorted(states)),
            },
        )


async def _async_prepare_energy_statistic(
    coordinator: EkzCoordinator,
    installation_id: str,
    kind: Literal["consumption", "production"],
) -> str:
    """Finish historical migration before importing measurements or predictions."""
    await async_migrate_statistics(coordinator, installation_id, kind)
    if kind == "consumption":
        await async_migrate_statistics(coordinator, installation_id, "prediction")
    return _energy_statistic_id(coordinator.hass, installation_id, kind)


async def async_migrate_statistics(
    coordinator: EkzCoordinator,
    installation_id: str,
    kind: Literal["consumption", "production", "prediction"],
) -> None:
    """Copy bounded periods, checking overlaps and persisting progress after writes."""
    target = (
        _prediction_statistic_id(coordinator.hass, installation_id)
        if kind == "prediction"
        else _energy_statistic_id(coordinator.hass, installation_id, kind)
    )
    state = coordinator._external_migrations.get(target)
    if state is None:
        await async_initialize_migration(coordinator)
        state = coordinator._external_migrations[target]
    if state["complete"]:
        return
    recorder = get_recorder_instance(coordinator.hass)
    start = datetime.fromisoformat(state["cursor"])
    end = datetime.fromisoformat(state["end"])
    sources = state["sources"]
    try:
        _LOGGER.info("Migrating retained EKZ history to %s from %s", target, sources)
        await _async_wait_for_statistics(coordinator.hass, target)
        metadata = await recorder.async_add_executor_job(
            partial(get_metadata, coordinator.hass, statistic_ids=set(sources) | {target})
        )
        for statistic_id, (_, meta) in metadata.items():
            if (
                meta["source"] != (DOMAIN if statistic_id == target else "recorder")
                or meta["unit_of_measurement"] != "kWh"
                or not meta["has_sum"]
            ):
                raise UpdateFailed(f"Incompatible statistics metadata for {statistic_id}")
            if statistic_id in sources:
                latest = await recorder.async_add_executor_job(
                    get_last_statistics, coordinator.hass, 1, statistic_id, True, {"sum"}
                )
                timestamps = _statistics_by_start(statistic_id, latest.get(statistic_id, []))
                if timestamps:
                    end = max(
                        end, datetime.fromtimestamp(max(timestamps), UTC) + timedelta(hours=1)
                    )
        state = {**state, "end": end.isoformat()}
        while start < end:
            stop = min(start + timedelta(days=366), end)
            rows_by_id = await recorder.async_add_executor_job(
                statistics_during_period,
                coordinator.hass,
                start,
                stop,
                set(sources) | {target},
                "hour",
                None,
                {"state", "sum"},
            )
            merged: dict[float, tuple[float | None, float]] = {}
            for source in sources:
                for timestamp, values in _statistics_by_start(
                    source, rows_by_id.get(source, [])
                ).items():
                    if timestamp in merged and merged[timestamp] != values:
                        raise UpdateFailed(
                            f"Conflicting legacy history for {target} at {timestamp}"
                        )
                    merged[timestamp] = values
            existing = _statistics_by_start(target, rows_by_id.get(target, []))
            for timestamp, values in merged.items():
                if timestamp in existing and existing[timestamp] != values:
                    raise UpdateFailed(f"Conflicting external history for {target} at {timestamp}")
            missing: list[StatisticData] = []
            for timestamp, (hourly_state, total) in sorted(merged.items()):
                if timestamp in existing:
                    continue
                row = StatisticData(start=datetime.fromtimestamp(timestamp, UTC), sum=total)
                if hourly_state is not None:
                    row["state"] = hourly_state
                missing.append(row)
            if missing:
                _LOGGER.debug(
                    "Copying %s hourly rows to %s for %s to %s", len(missing), target, start, stop
                )
                _async_write_statistics(coordinator.hass, _make_stat_meta(target), missing)
                await _async_wait_for_statistics(coordinator.hass, target)
                verified = await recorder.async_add_executor_job(
                    statistics_during_period,
                    coordinator.hass,
                    start,
                    stop,
                    {target},
                    "hour",
                    None,
                    {"state", "sum"},
                )
                actual = _statistics_by_start(target, verified.get(target, []))
                if any(actual.get(timestamp) != values for timestamp, values in merged.items()):
                    raise UpdateFailed(f"Could not verify migrated history for {target}")
            state = {**state, "cursor": stop.isoformat()}
            updated = {**coordinator._external_migrations, target: state}
            await coordinator._external_migration_store.async_save(updated)
            coordinator._external_migrations = updated
            _LOGGER.info(
                "External statistics migration completed for %s; legacy history retained", target
            )
            start = stop

        for attribute, store in (
            ("_import_checkpoints", coordinator._import_checkpoint_store),
            ("_daily_statistics", coordinator._source_resolution_store),
        ):
            values = getattr(coordinator, attribute)
            candidates = [values[source] for source in sources if source in values]
            if target in values:
                candidates.append(values[target])
            if candidates:
                if any(candidate != candidates[0] for candidate in candidates[1:]):
                    raise UpdateFailed(f"Conflicting {attribute} during migration to {target}")
                updated = {key: value for key, value in values.items() if key not in sources}
                updated[target] = candidates[0]
                await store.async_save(updated)
                setattr(coordinator, attribute, updated)
        offsets = dict(coordinator._sum_offsets.get(target, {}))
        for source in sources:
            for day, offset in coordinator._sum_offsets.get(source, {}).items():
                if day in offsets and offsets[day] != offset:
                    raise UpdateFailed(f"Conflicting sum offsets during migration to {target}")
                offsets[day] = offset
        if offsets:
            coordinator._sum_offsets[target] = offsets
        for source in sources:
            coordinator._sum_offsets.pop(source, None)
        updated = {**coordinator._external_migrations, target: {**state, "complete": True}}
        await coordinator._external_migration_store.async_save(updated)
        coordinator._external_migrations = updated
    except (UpdateFailed, EkzAuthenticationError):
        raise
    except Exception as err:
        raise UpdateFailed(f"External statistics migration failed for {target}: {err}") from err
