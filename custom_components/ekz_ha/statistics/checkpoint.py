"""Durable import checkpoints, recorder replay and failure rollback."""

from __future__ import annotations

import logging
import zoneinfo
from copy import deepcopy
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import async_import_statistics
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from ..api.session import EkzAuthenticationError
from ..data.validator import day_slots
from ..import_status import ImportStatus, get_import_status, publish_import_status
from .consumption import ConsumptionImporter
from .importer import get_import_start
from .production import ProductionImporter
from .recorder import _make_stat_meta

if TYPE_CHECKING:
    from ..coordinator import EkzCoordinator

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")
_LOGGER = logging.getLogger(__name__)


def _cache_sum_offsets(
    coordinator: EkzCoordinator,
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
        if preceding:
            offsets[next_import_date] = float(preceding[-1]["sum"])
        else:
            offsets.setdefault(next_import_date, running_sum_offset)
    if not hasattr(coordinator, "_sum_offsets"):
        coordinator._sum_offsets = {}
    coordinator._sum_offsets[statistic_id] = offsets


async def _persist_source_resolution(
    coordinator: EkzCoordinator, statistic_id: str, statistics: list[dict[str, Any]]
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


def _restore_import_checkpoint(coordinator: EkzCoordinator, statistic_id: str, meta: Any) -> None:
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
    empty_search_from = state.get("empty_search_from")
    meta._empty_search_from = date.fromisoformat(empty_search_from) if empty_search_from else None
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
    coordinator: EkzCoordinator, statistic_id: str, meta: Any
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
    coordinator: EkzCoordinator, statistic_id: str, meta: Any, statistics: list[dict[str, Any]]
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
        "empty_search_from": (
            meta._empty_search_from.isoformat()
            if getattr(meta, "_empty_search_from", None) is not None
            else None
        ),
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
    coordinator: EkzCoordinator,
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
        "_empty_search_from",
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
    finally:
        publish_import_status(meta)
