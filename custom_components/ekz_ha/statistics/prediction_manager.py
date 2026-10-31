"""Prediction recovery, persistence and recorder updates."""

from __future__ import annotations

import logging
import zoneinfo
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import (
    get_last_statistics,
    statistics_during_period,
)

from .prediction import PredictionService
from .recorder import (
    _async_write_statistics,
    _energy_statistic_id,
    _make_stat_meta,
    _prediction_statistic_id,
)

if TYPE_CHECKING:
    from ..coordinator import EkzCoordinator

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")
_LOGGER = logging.getLogger(__name__)
PREDICTION_MAX_GAP = timedelta(days=14)
PREDICTION_LOOKBACK = timedelta(days=400)


def _statistics_datetime(value: datetime | float) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


async def _async_save_prediction_state(coordinator: EkzCoordinator) -> None:
    store = getattr(coordinator, "_prediction_store", None)
    if store is None:
        return
    if coordinator._prediction_states is None:
        try:
            coordinator._prediction_states = await store.async_load() or {}
        except Exception as err:
            _LOGGER.warning("Could not load prediction learning state before saving: %s", err)
            return
    states = dict(coordinator._prediction_states or {})
    states.update(
        {
            key: service.to_dict()
            for key, service in coordinator.prediction_services.items()
            if key in coordinator._prediction_initialized
        }
    )
    try:
        await store.async_save(states)
    except Exception as err:
        _LOGGER.warning("Could not save prediction learning state: %s", err)
    else:
        coordinator._prediction_states = states


async def _async_get_prediction_service(coordinator: EkzCoordinator, key: str) -> PredictionService:
    service = coordinator.prediction_services.setdefault(key, PredictionService())
    store = getattr(coordinator, "_prediction_store", None)
    if store is None or key in coordinator._prediction_initialized:
        return service
    if coordinator._prediction_states is None:
        try:
            coordinator._prediction_states = await store.async_load() or {}
        except Exception as err:
            _LOGGER.warning("Could not load prediction learning state: %s", err)
            return service
    states = coordinator._prediction_states
    assert states is not None
    stored = states.get(key)
    if stored:
        try:
            service.load_dict(stored)
        except (KeyError, TypeError, ValueError, OverflowError) as err:
            _LOGGER.warning("Invalid prediction learning state for %s: %s", key, err)
        else:
            coordinator._prediction_initialized.add(key)
            return service

    statistic_id = _energy_statistic_id(coordinator.hass, key, "consumption")
    if statistic_id is None:
        _LOGGER.warning("Cannot rebuild prediction history: consumption entity %s is missing", key)
        return service
    now = datetime.now(tz=UTC)
    try:
        rows_by_id = await get_recorder_instance(coordinator.hass).async_add_executor_job(
            statistics_during_period,
            coordinator.hass,
            now - PREDICTION_LOOKBACK,
            now,
            {statistic_id},
            "hour",
            None,
            {"sum"},
        )
    except Exception as err:
        _LOGGER.warning("Could not rebuild prediction learning state for %s: %s", key, err)
        return service
    daily_days = set(getattr(coordinator, "_daily_statistics", {}).get(statistic_id, []))
    rows: list[tuple[datetime, float | None]] = []
    for row in (rows_by_id or {}).get(statistic_id, []):
        raw_start = row.get("start")
        if raw_start is None:
            _LOGGER.warning("Prediction history for %s contains a row without a start", key)
            continue
        rows.append((_statistics_datetime(raw_start), row.get("sum")))
    rows.sort(key=lambda row: row[0])
    values = []
    for (previous_start, previous_sum), (start, total) in zip(rows, rows[1:], strict=False):
        if (
            start - previous_start == timedelta(hours=1)
            and previous_sum is not None
            and total is not None
            and start.astimezone(ZRH).date().isoformat() not in daily_days
            and previous_start.astimezone(ZRH).date().isoformat() not in daily_days
        ):
            values.append((start, total - previous_sum))
    service.seed_hourly_values(values)
    coordinator._prediction_initialized.add(key)
    _LOGGER.info(
        "Rebuilt prediction learning state for %s: %d month-hour buckets",
        key,
        len(service.get_averages()),
    )
    await _async_save_prediction_state(coordinator)
    return service


async def async_extend_predictions(coordinator: EkzCoordinator, now: datetime) -> None:
    """Advance estimates locally, serialized with imports and statistics reset."""
    if coordinator._reset_lock.locked():
        return
    async with coordinator._reset_lock:
        if any(state.get("reset_pending") for state in coordinator._external_migrations.values()):
            _LOGGER.error(
                "Prediction extension blocked: statistics reset is incomplete. "
                "Resolve the recorder/storage error and retry ekz_ha.reset_statistics."
            )
            return
        now = now.astimezone(UTC)
        for key in coordinator.installations or {}:
            if coordinator.catching_up.get(key, True):
                continue
            meta = (getattr(coordinator, "meta_entities", None) or {}).get(key)
            last_real = getattr(meta, "_last_successful_import", None)
            if last_real is None or now - last_real > PREDICTION_MAX_GAP or last_real > now:
                continue
            statistic_id = _prediction_statistic_id(coordinator.hass, key)
            if statistic_id is None:
                continue
            try:
                service = await _async_get_prediction_service(coordinator, key)
                if not service.get_averages():
                    continue
                recorder = get_recorder_instance(coordinator.hass)
                await recorder.async_block_till_done()
                last = await recorder.async_add_executor_job(
                    get_last_statistics, coordinator.hass, 1, statistic_id, True, {"sum"}
                )
                rows = (last or {}).get(statistic_id, [])
                if not rows:
                    continue
                raw_start = rows[0].get("start")
                last_sum = rows[0].get("sum")
                if raw_start is None or last_sum is None:
                    _LOGGER.warning("Prediction tail for %s has no start or cumulative sum", key)
                    continue
                last_start = _statistics_datetime(raw_start)
                if last_start < last_real:
                    continue
                predictions = service.generate_predictions(last_start, now, initial_total=last_sum)
                if predictions:
                    _async_write_statistics(
                        coordinator.hass,
                        _make_stat_meta(statistic_id),
                        [
                            StatisticData(start=p["start"], state=p["state"], sum=p["sum"])
                            for p in predictions
                        ],
                    )
            except Exception as err:
                _LOGGER.warning("Could not extend predictions for %s: %s", key, err)


async def async_update_predictions(
    coordinator: EkzCoordinator, key: str, result: dict[str, Any], still_catching_up: bool
) -> None:
    """Learn from an import and replace estimates covered by real readings."""
    prediction_service = await _async_get_prediction_service(coordinator, key)

    # Accumulate raw values from this import chunk
    prediction_ready = (
        getattr(coordinator, "_prediction_store", None) is None
        or key in coordinator._prediction_initialized
    )
    if result.get("raw_values") and prediction_ready:
        prediction_service.accumulate_values(result["raw_values"])
        prediction_service.calculate_averages()
        await _async_save_prediction_state(coordinator)
        averages = prediction_service.get_averages()
        _LOGGER.debug(f"Updated prediction averages for {key}: {len(averages)} month-hour buckets")

    prediction_statistic_id = _prediction_statistic_id(coordinator.hass, key)
    # Generate predictions only when caught up (gap fills the recent EKZ delay)
    if result.get("statistics") and not still_catching_up:
        predictions = []
        for statistic in result["statistics"]:
            covered_hour = statistic["start"]
            coverage_end = statistic.get("end", covered_hour + timedelta(hours=1))
            while covered_hour < coverage_end:
                predictions.append({"start": covered_hour, "sum": 0, "state": 0})
                covered_hour += timedelta(hours=1)

        last_actual_start = max(
            statistic.get("end", statistic["start"] + timedelta(hours=1))
            for statistic in result["statistics"]
        ) - timedelta(hours=1)
        last_actual_statistic = next(
            (
                statistic
                for statistic in result["statistics"]
                if statistic["start"] == last_actual_start
                and statistic.get("source_resolution") == "hour"
            ),
            None,
        )
        delivered_slots = (
            len(last_actual_statistic.get("slots", {})) if last_actual_statistic is not None else 4
        )
        partial_hour_prediction = prediction_service.estimate_missing_slots(
            last_actual_start, delivered_slots
        )
        if partial_hour_prediction > 0:
            predictions = [
                prediction for prediction in predictions if prediction["start"] != last_actual_start
            ]
            predictions.append(
                {
                    "start": last_actual_start,
                    "sum": partial_hour_prediction,
                    "state": partial_hour_prediction,
                }
            )
        now = datetime.now(tz=UTC)
        forward_predictions = (
            prediction_service.generate_predictions(
                last_actual_start, now, initial_total=partial_hour_prediction
            )
            if now - last_actual_start <= PREDICTION_MAX_GAP
            else []
        )
        predictions.extend(forward_predictions)

        if predictions:
            if prediction_statistic_id is None:
                _LOGGER.warning(
                    "Prediction entity for %s is not registered; skipping statistics import",
                    key,
                )
            else:
                _LOGGER.info(
                    f"Predictions for {key}: {len(predictions)} entries, "
                    f"gap coverage {result['statistics'][-1]['start'].date()} → {datetime.now(tz=UTC).date()}"
                )
                try:
                    _async_write_statistics(
                        coordinator.hass,
                        _make_stat_meta(prediction_statistic_id),
                        [
                            StatisticData(start=p["start"], sum=p["sum"], state=p["state"])
                            for p in predictions
                        ],
                    )
                except Exception as e:
                    _LOGGER.error(f"Failed to import prediction statistics for {key}: {e}")
