"""Prediction learning persistence, recorder recovery and local hourly extension."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant.helpers import entity_registry as er

from custom_components.ekz_ha import async_setup_entry
from custom_components.ekz_ha.api.models import ApiValue
from custom_components.ekz_ha.coordinator import EkzCoordinator
from custom_components.ekz_ha.sensor import EkzMetaEntity
from custom_components.ekz_ha.statistics.prediction import PredictionService
from custom_components.ekz_ha.statistics.prediction_manager import (
    _async_get_prediction_service,
    _async_save_prediction_state,
)


def slots(start, energy=0.25):
    return [
        ApiValue.from_dict(
            {"timestamp": (start + timedelta(minutes=15 * i)).isoformat(), "value": energy}
        )
        for i in range(4)
    ]


@pytest.fixture
def prediction(hass, monkeypatch, freezer):
    freezer.move_to("2026-10-28T12:01:00+00:00")
    coordinator = EkzCoordinator(
        hass, Mock(), timedelta(hours=4), Mock(entry_id="prediction-recovery")
    )
    er.async_get(hass).async_get_or_create(
        "sensor",
        "ekz_ha",
        "ekz_electricity_consumption_test",
        suggested_object_id="electricity_consumption_ekz_test",
    )
    coordinator.installations = {"test": {"contract_start": "2026-01-01"}}
    coordinator.catching_up = {"test": False}
    meta = EkzMetaEntity(coordinator, "test")
    meta._last_successful_import = datetime(2026, 10, 20, tzinfo=UTC)
    coordinator.meta_entities = {"test": meta}
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={}),
        async_block_till_done=AsyncMock(),
    )
    for module in (
        "coordinator",
        "statistics.checkpoint",
        "statistics.prediction_manager",
        "statistics.recorder",
    ):
        monkeypatch.setattr(
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda _hass: recorder
        )
    enqueue = Mock()
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint.async_import_statistics", enqueue
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager.async_import_statistics", enqueue
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda *_args: "sensor.prediction",
    )
    return coordinator, recorder, enqueue


def test_learning_round_trip_preserves_slot_corrections_and_seeded_hours():
    service = PredictionService()
    start = datetime(2026, 10, 20, tzinfo=UTC)
    service.seed_hourly_values([(start, 1.0), (start + timedelta(hours=1), 2.0)])
    service.accumulate_values(slots(start, 0.5))
    service.calculate_averages()
    restored = PredictionService()
    restored.load_dict(service.to_dict())
    assert restored.get_raw_accumulator() == service.get_raw_accumulator()
    assert restored.get_averages() == service.get_averages()
    restored.accumulate_values(slots(start, 0.5))
    assert restored.get_raw_accumulator() == service.get_raw_accumulator()
    restored.accumulate_values(slots(start + timedelta(hours=1), 0.75))
    assert restored.calculate_averages()[1001] == pytest.approx(3.0)
    assert restored.get_raw_accumulator()[1001] == (3.0, 4)
    restored.reset()
    assert restored.to_dict() == {
        "hourly_raw": {},
        "slot_values": {},
        "seeded_hours": {},
        "slot_cutoff": None,
    }


def test_seeded_history_is_not_counted_twice_or_replayed_after_cutoff():
    service = PredictionService()
    old = datetime(2025, 10, 1, tzinfo=UTC)
    recent = datetime(2026, 10, 20, tzinfo=UTC)
    service.seed_hourly_values([(old, 1.0), (recent, 1.0)])
    before = service.get_raw_accumulator()
    service.accumulate_values(slots(old, 99.0))
    assert service.get_raw_accumulator() == before
    service.accumulate_values(slots(recent, 0.5))
    assert service.get_raw_accumulator()[1000] == pytest.approx((3.0, 8))
    service.seed_hourly_values([(recent, 99.0)])
    assert service.get_raw_accumulator()[1000] == pytest.approx((3.0, 8))


def test_older_raw_correction_does_not_move_seeded_history_cutoff_backwards():
    service = PredictionService()
    old = datetime(2026, 8, 20, tzinfo=UTC)
    latest = datetime(2026, 10, 20, tzinfo=UTC)
    service.seed_hourly_values([(old, 1.0), (latest, 1.0)])
    cutoff = service.to_dict()["slot_cutoff"]
    service.accumulate_values(slots(latest - timedelta(days=30)))
    assert service.to_dict()["slot_cutoff"] == cutoff
    before = service.get_raw_accumulator()
    service.accumulate_values(slots(old, 99.0))
    assert service.get_raw_accumulator() == before


def test_invalid_saved_state_is_rejected():
    service = PredictionService()
    data = service.to_dict()
    data["hourly_raw"] = {"1024": [1.0, 4]}
    with pytest.raises(ValueError):
        service.load_dict(data)


async def test_learning_survives_restart_without_recorder_rebuild(prediction):
    coordinator, recorder, _ = prediction
    service = PredictionService()
    service.accumulate_values(slots(datetime(2026, 10, 20, tzinfo=UTC)))
    service.calculate_averages()
    coordinator.prediction_services["test"] = service
    coordinator._prediction_initialized.add("test")
    await _async_save_prediction_state(coordinator)
    coordinator.prediction_services = {}
    coordinator._prediction_initialized.clear()
    coordinator._prediction_states = None
    restored = await _async_get_prediction_service(coordinator, "test")
    assert restored.to_dict() == service.to_dict()
    recorder.async_add_executor_job.assert_not_awaited()
    assert "test" in coordinator._prediction_initialized


@pytest.mark.parametrize("recovery_failure", [None, "load", "recorder"])
@pytest.mark.parametrize("age_days", [8, 15])
async def test_successful_import_persists_learning_for_next_coordinator(
    prediction, monkeypatch, age_days, recovery_failure
):
    from custom_components.ekz_ha.api.models import InstallationSelectionData
    from custom_components.ekz_ha.import_status import get_import_status
    from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter

    coordinator, recorder, enqueue = prediction
    coordinator.api_client.get_production_installations = AsyncMock(
        return_value=InstallationSelectionData()
    )
    coordinator.api_client.get_production_15min = AsyncMock(
        side_effect=RuntimeError("no production")
    )
    meta = coordinator.meta_entities["test"]
    meta._checkpoint_restored = True
    meta.set_contract_start(datetime(2026, 1, 1).date())
    start = datetime(2026, 10, 28, tzinfo=UTC) - timedelta(days=age_days)
    meta.set_last_import(start.date())

    async def imported(_importer, _hass, _key, _start, metadata, **_kwargs):
        get_import_status(metadata).begin(start, datetime(2026, 10, 29, tzinfo=UTC), False)
        return {
            "fetched_data": True,
            "statistics": [{"start": start, "sum": 1.0, "state": 1.0}],
            "raw_values": slots(start),
            "to_date": datetime(2026, 10, 29).date(),
        }

    monkeypatch.setattr(ConsumptionImporter, "import_statistics", imported)
    if recovery_failure is not None:
        old = PredictionService()
        old.seed_hourly_values([(datetime(2026, 9, 1, tzinfo=UTC), 3.0)])
        old_state = old.to_dict()
        if recovery_failure == "load":
            await coordinator._prediction_store.async_save({"test": old_state})
            original_load = coordinator._prediction_store.async_load
            monkeypatch.setattr(
                coordinator._prediction_store,
                "async_load",
                AsyncMock(side_effect=OSError("temporary load failure")),
            )
        else:
            original_query = recorder.async_add_executor_job

            async def fail_rebuild(function, *args):
                from homeassistant.components.recorder.statistics import statistics_during_period

                if function is statistics_during_period:
                    raise OSError("temporary recorder failure")
                return await original_query(function, *args)

            recorder.async_add_executor_job = AsyncMock(side_effect=fail_rebuild)
        await coordinator._do_update_data()
        assert "test" not in coordinator._prediction_initialized
        assert coordinator.prediction_services["test"].get_raw_accumulator() == {}
        if recovery_failure == "load":
            monkeypatch.setattr(coordinator._prediction_store, "async_load", original_load)
            # Saving another initialized installation must not replace this profile.
            coordinator.prediction_services["other"] = PredictionService()
            coordinator._prediction_initialized.add("other")
            await _async_save_prediction_state(coordinator)
            assert (await original_load())["test"] == old_state
        else:
            recorder.async_add_executor_job = original_query
            statistic_id = "sensor.electricity_consumption_ekz_test"
            original_query.return_value = {
                statistic_id: [
                    {"start": datetime(2026, 8, 31, 23, tzinfo=UTC), "sum": 5.0},
                    {"start": datetime(2026, 9, 1, tzinfo=UTC), "sum": 8.0},
                ]
            }
        enqueue.reset_mock()
    await coordinator._do_update_data()
    predictions = enqueue.call_args.args[2]
    assert any(row["start"] > start for row in predictions) == (age_days <= 14)
    state = await coordinator._prediction_store.async_load()
    assert state["test"]["hourly_raw"]["1000"] == [1.0, 4]
    if recovery_failure is not None:
        assert state["test"]["hourly_raw"]["900"] == [3.0, 4]
        assert "test" in coordinator._prediction_initialized
    coordinator.prediction_services.clear()
    coordinator._prediction_initialized.clear()
    coordinator._prediction_states = None
    recorder.async_add_executor_job.reset_mock()
    restored = await _async_get_prediction_service(coordinator, "test")
    assert restored.get_averages()[1000] == 1.0
    recorder.async_add_executor_job.assert_not_awaited()


async def test_saving_one_installation_preserves_unloaded_other_installations(prediction):
    coordinator, _, _ = prediction
    state = PredictionService().to_dict()
    coordinator._prediction_states = {"other": state}
    coordinator.prediction_services["test"] = PredictionService()
    coordinator._prediction_initialized.add("test")
    await _async_save_prediction_state(coordinator)
    assert (await coordinator._prediction_store.async_load())["other"] == state


async def test_recorder_rebuild_excludes_daily_days_gaps_and_negative_deltas(prediction):
    coordinator, recorder, _ = prediction
    statistic_id = "sensor.electricity_consumption_ekz_test"
    coordinator._daily_statistics = {statistic_id: ["2026-10-19"]}
    start = datetime(2026, 10, 19, 20, tzinfo=UTC)
    recorder.async_add_executor_job.return_value = {
        statistic_id: [
            {"start": start.timestamp(), "sum": 10.0},
            {"start": (start + timedelta(hours=1)).timestamp(), "sum": 110.0},
            # Local midnight: a preceding daily-total row must not seed this hour.
            {"start": (start + timedelta(hours=2)).timestamp(), "sum": 210.0},
            {"start": (start + timedelta(hours=3)).timestamp(), "sum": 211.0},
            {"start": (start + timedelta(hours=4)).timestamp(), "sum": 209.0},
            {"start": (start + timedelta(hours=6)).timestamp(), "sum": 250.0},
        ]
    }
    service = await _async_get_prediction_service(coordinator, "test")
    assert service.get_raw_accumulator() == {1023: (1.0, 4)}
    assert service.get_averages() == {1023: 1.0}
    assert await coordinator._prediction_store.async_load()


async def test_recorder_rebuild_overlapping_raw_import_does_not_double_count(prediction):
    coordinator, recorder, _ = prediction
    statistic_id = "sensor.electricity_consumption_ekz_test"
    start = datetime(2026, 10, 20, tzinfo=UTC)
    recorder.async_add_executor_job.return_value = {
        statistic_id: [
            {"start": start - timedelta(hours=1), "sum": 10.0},
            {"start": start, "sum": 11.0},
        ]
    }
    service = await _async_get_prediction_service(coordinator, "test")
    service.accumulate_values(slots(start, 0.5))
    assert service.calculate_averages() == {1000: 2.0}
    assert service.get_raw_accumulator() == {1000: (2.0, 4)}


async def test_corrupt_state_logs_and_rebuilds(prediction, caplog):
    coordinator, recorder, _ = prediction
    coordinator._prediction_states = {"test": {"invalid": True}}
    await _async_get_prediction_service(coordinator, "test")
    assert "Invalid prediction learning state" in caplog.text
    recorder.async_add_executor_job.assert_awaited_once()


async def test_storage_and_recorder_errors_are_logged_and_retried(prediction, caplog):
    coordinator, recorder, _ = prediction
    coordinator._prediction_store = Mock(
        async_load=AsyncMock(side_effect=OSError("read")),
        async_save=AsyncMock(side_effect=OSError("write")),
    )
    await _async_get_prediction_service(coordinator, "test")
    assert "Could not load prediction learning state" in caplog.text
    assert "test" not in coordinator._prediction_initialized
    coordinator._prediction_store.async_load.side_effect = None
    coordinator._prediction_store.async_load.return_value = {}
    recorder.async_add_executor_job.side_effect = OSError("recorder")
    await _async_get_prediction_service(coordinator, "test")
    assert "Could not rebuild prediction learning state" in caplog.text
    assert "test" not in coordinator._prediction_initialized
    await _async_save_prediction_state(coordinator)
    assert "Could not save prediction learning state" in caplog.text


async def test_hourly_extension_continues_running_sum_without_api_calls(prediction):
    coordinator, recorder, enqueue = prediction
    service = PredictionService()
    service.seed_hourly_values([(datetime(2026, 10, 20, 12, tzinfo=UTC), 1.5)])
    coordinator.prediction_services["test"] = service
    coordinator._prediction_initialized.add("test")
    recorder.async_add_executor_job.return_value = {
        "sensor.prediction": [
            {"start": datetime(2026, 10, 28, 11, tzinfo=UTC).timestamp(), "sum": 7.0}
        ]
    }
    await coordinator.async_extend_predictions(datetime(2026, 10, 28, 12, 1, tzinfo=UTC))
    rows = enqueue.call_args.args[2]
    assert rows == [{"start": datetime(2026, 10, 28, 12, tzinfo=UTC), "state": 1.5, "sum": 8.5}]
    recorder.async_block_till_done.assert_awaited_once()
    assert coordinator.api_client.mock_calls == []
    recorder.async_add_executor_job.return_value = {
        "sensor.prediction": [{"start": rows[-1]["start"], "sum": rows[-1]["sum"]}]
    }
    await coordinator.async_extend_predictions(datetime(2026, 10, 28, 12, 30, tzinfo=UTC))
    assert enqueue.call_count == 1


@pytest.mark.parametrize("age_days,expected", [(10, True), (14, True), (15, False)])
async def test_prediction_horizon_uses_real_measurements_not_fresh_prediction(
    prediction, age_days, expected
):
    coordinator, recorder, enqueue = prediction
    now = datetime(2026, 10, 28, 12, 1, tzinfo=UTC)
    coordinator.meta_entities["test"]._last_successful_import = now - timedelta(days=age_days)
    service = PredictionService()
    service.seed_hourly_values([(now.replace(minute=0), 1.0)])
    coordinator.prediction_services["test"] = service
    coordinator._prediction_initialized.add("test")
    recorder.async_add_executor_job.return_value = {
        "sensor.prediction": [{"start": now.replace(hour=11, minute=0), "sum": 2.0}]
    }
    await coordinator.async_extend_predictions(now)
    assert enqueue.called == expected


async def test_timer_event_fires_hourly_and_can_be_cancelled(prediction, hass):
    from homeassistant.helpers.event import async_track_time_change
    from pytest_homeassistant_custom_component.common import async_fire_time_changed

    coordinator, _, _ = prediction
    callback = AsyncMock(wraps=coordinator.async_extend_predictions)
    cancel = async_track_time_change(hass, callback, minute=1, second=0)
    now = datetime(2026, 10, 28, 13, 1, tzinfo=UTC)
    try:
        async_fire_time_changed(hass, now)
        await hass.async_block_till_done()
        callback.assert_awaited_once()
    finally:
        cancel()
    async_fire_time_changed(hass, now + timedelta(hours=1))
    await hass.async_block_till_done()
    callback.assert_awaited_once()


@pytest.mark.parametrize(
    "reason", ["catchup", "uninitialized", "reset", "stale", "no_actual", "future"]
)
async def test_hourly_extension_stops_for_unsafe_conditions(prediction, reason):
    coordinator, recorder, enqueue = prediction
    if reason == "catchup":
        coordinator.catching_up["test"] = True
    elif reason == "uninitialized":
        coordinator.catching_up.clear()
    elif reason == "reset":
        await coordinator._reset_lock.acquire()
    elif reason == "stale":
        # A fresh prediction must not conceal stale real measurements.
        coordinator.meta_entities["test"]._last_successful_import = datetime(
            2026, 10, 1, tzinfo=UTC
        )
    elif reason == "no_actual":
        coordinator.meta_entities["test"]._last_successful_import = None
    else:
        coordinator.meta_entities["test"]._last_successful_import = datetime(
            2026, 10, 29, tzinfo=UTC
        )
    try:
        await coordinator.async_extend_predictions(datetime(2026, 10, 28, 12, 1, tzinfo=UTC))
    finally:
        if coordinator._reset_lock.locked():
            coordinator._reset_lock.release()
    recorder.async_add_executor_job.assert_not_awaited()
    enqueue.assert_not_called()


async def test_prediction_timer_registration_and_unload(hass, monkeypatch):
    from custom_components.ekz_ha.sensor import async_setup_entry as setup_sensors

    entry = Mock(entry_id="prediction-timer", data={"user": "test", "password": "test"})
    entry.async_create_background_task.side_effect = lambda _hass, coro, _name: coro.close()
    cancel = Mock()
    timer = Mock(return_value=cancel)
    monkeypatch.setattr("custom_components.ekz_ha.async_track_time_change", timer)
    monkeypatch.setattr("custom_components.ekz_ha.EkzApiClient", Mock())

    async def discover(coordinator):
        coordinator.installations = {"test": {"contract_start": "2026-01-01"}}

    async def forward(_entry, _platforms):
        await setup_sensors(hass, entry, Mock())

    monkeypatch.setattr(EkzCoordinator, "async_config_entry_first_refresh", discover)
    with patch.object(hass.config_entries, "async_forward_entry_setups", side_effect=forward):
        assert await async_setup_entry(hass, entry)
    assert timer.call_args.kwargs == {"minute": 1, "second": 0}
    entry.async_on_unload.assert_called_once_with(cancel)
    entry.async_on_unload.call_args.args[0]()
    cancel.assert_called_once()
