"""Recorder history and checkpoint continuity for registered EKZ entity IDs."""

from datetime import UTC, date, datetime, timedelta
from functools import partial
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_last_statistics,
    get_metadata,
    statistics_during_period,
)
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.ekz_ha.const import DOMAIN
from custom_components.ekz_ha.coordinator import EkzCoordinator
from custom_components.ekz_ha.statistics.prediction_manager import _async_get_prediction_service
from custom_components.ekz_ha.statistics.recorder import (
    _async_prepare_energy_statistic,
    _energy_statistic_id,
    _make_stat_meta,
)


@pytest.fixture(params=["consumption", "production"])
def energy(hass, request):
    kind = request.param
    registry = er.async_get(hass)
    entity = registry.async_get_or_create(
        "sensor",
        DOMAIN,
        f"ekz_electricity_{kind}_170579",
        suggested_object_id=f"ekz_170579_electricity_{kind}_ekz_170579",
    )
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id="entity-ids"))
    return coordinator, kind, entity.entity_id, f"sensor.electricity_{kind}_ekz_170579"


async def test_registered_id_without_history_is_used_and_persisted(energy, monkeypatch):
    coordinator, kind, entity_id, legacy_id = energy
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={}),
        async_block_till_done=AsyncMock(),
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.get_recorder_instance",
        lambda _hass: recorder,
    )
    assert _energy_statistic_id(coordinator.hass, "170579", kind) == entity_id
    assert await _async_prepare_energy_statistic(coordinator, "170579", kind) == entity_id
    assert await coordinator._statistic_id_store.async_load() == {f"{kind}:170579": entity_id}
    recorder.async_add_executor_job.assert_awaited_once()
    assert entity_id != legacy_id


async def test_missing_entity_stops_import_instead_of_using_an_unrelated_id(energy):
    coordinator, kind, entity_id, _ = energy
    er.async_get(coordinator.hass).async_remove(entity_id)
    with pytest.raises(UpdateFailed, match="entity is not registered"):
        await _async_prepare_energy_statistic(coordinator, "170579", kind)
    assert coordinator._statistic_ids == {}


async def test_migration_preserves_recorder_history_checkpoints_and_later_renames(
    recorder_mock, energy
):
    coordinator, kind, entity_id, legacy_id = energy
    start = datetime(2026, 10, 20, tzinfo=UTC)
    rows = [
        StatisticData(start=start, state=2.0, sum=12.0),
        StatisticData(start=start + timedelta(hours=1), state=3.0, sum=15.0),
    ]
    async_import_statistics(coordinator.hass, _make_stat_meta(legacy_id), rows)
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    checkpoint = {"history_cursor": "2026-10-19", "pending_from": "2026-10-20"}
    coordinator._import_checkpoints = {legacy_id: checkpoint}
    coordinator._daily_statistics = {legacy_id: ["2026-10-19"]}
    coordinator._sum_offsets = {legacy_id: {date(2026, 10, 20): 10.0}}

    for previous_id, target_id in (
        (legacy_id, entity_id),
        (entity_id, f"sensor.my_renamed_{kind}"),
    ):
        if previous_id == entity_id:
            er.async_get(coordinator.hass).async_update_entity(previous_id, new_entity_id=target_id)
        assert await _async_prepare_energy_statistic(coordinator, "170579", kind) == target_id
        last = await recorder_mock.async_add_executor_job(
            get_last_statistics, coordinator.hass, 1, target_id, True, {"state", "sum"}
        )
        assert last[target_id][0]["sum"] == 15.0
        assert last[target_id][0]["state"] == 3.0
        history = await recorder_mock.async_add_executor_job(
            statistics_during_period,
            coordinator.hass,
            start,
            start + timedelta(hours=2),
            {target_id},
            "hour",
            None,
            {"state", "sum"},
        )
        assert [row["sum"] for row in history[target_id]] == [12.0, 15.0]
        metadata = await recorder_mock.async_add_executor_job(
            partial(get_metadata, coordinator.hass, statistic_ids={previous_id, target_id})
        )
        assert previous_id not in metadata
        assert target_id in metadata
        assert coordinator._import_checkpoints == {target_id: checkpoint}
        assert await coordinator._import_checkpoint_store.async_load() == {target_id: checkpoint}
        assert await coordinator._source_resolution_store.async_load() == {
            target_id: ["2026-10-19"]
        }
        assert coordinator._sum_offsets == {target_id: {date(2026, 10, 20): 10.0}}
        assert await coordinator._statistic_id_store.async_load() == {f"{kind}:170579": target_id}
        # A repeat must not attempt a second rename or reset tracking.
        assert await _async_prepare_energy_statistic(coordinator, "170579", kind) == target_id


async def test_conflicting_histories_are_not_overwritten_or_deleted(recorder_mock, energy):
    coordinator, kind, entity_id, legacy_id = energy
    start = datetime(2026, 10, 20, tzinfo=UTC)
    for statistic_id, total in ((legacy_id, 12.0), (entity_id, 25.0)):
        async_import_statistics(
            coordinator.hass,
            _make_stat_meta(statistic_id),
            [StatisticData(start=start, state=2.0, sum=total)],
        )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    with pytest.raises(UpdateFailed, match="both IDs already have statistics metadata"):
        await _async_prepare_energy_statistic(coordinator, "170579", kind)
    for statistic_id, total in ((legacy_id, 12.0), (entity_id, 25.0)):
        last = await recorder_mock.async_add_executor_job(
            get_last_statistics, coordinator.hass, 1, statistic_id, True, {"sum"}
        )
        assert last[statistic_id][0]["sum"] == total
    assert coordinator._statistic_ids == {}


async def test_checkpoint_save_failure_can_retry_after_recorder_rename(
    recorder_mock, energy, monkeypatch
):
    coordinator, kind, entity_id, legacy_id = energy
    async_import_statistics(
        coordinator.hass,
        _make_stat_meta(legacy_id),
        [StatisticData(start=datetime(2026, 10, 20, tzinfo=UTC), state=2.0, sum=12.0)],
    )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    checkpoint = {"history_cursor": "2026-10-19"}
    coordinator._import_checkpoints = {legacy_id: checkpoint}
    await coordinator._import_checkpoint_store.async_save(coordinator._import_checkpoints)
    with monkeypatch.context() as patch:
        patch.setattr(
            coordinator._import_checkpoint_store,
            "async_save",
            AsyncMock(side_effect=OSError("disk unavailable")),
        )
        with pytest.raises(UpdateFailed, match="disk unavailable"):
            await _async_prepare_energy_statistic(coordinator, "170579", kind)
    assert coordinator._statistic_ids == {}
    assert coordinator._import_checkpoints == {legacy_id: checkpoint}
    restarted = EkzCoordinator(
        coordinator.hass, Mock(), timedelta(hours=4), Mock(entry_id="entity-ids")
    )
    monkeypatch.setattr(restarted, "_async_discover_installations", AsyncMock())
    await restarted._async_setup()
    assert await _async_prepare_energy_statistic(restarted, "170579", kind) == entity_id
    assert restarted._import_checkpoints == {entity_id: checkpoint}


async def test_prediction_rebuild_reads_actual_consumption_entity(hass, monkeypatch):
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id="prediction-id"))
    entity_id = (
        er.async_get(hass)
        .async_get_or_create(
            "sensor",
            DOMAIN,
            "ekz_electricity_consumption_170579",
            suggested_object_id="my_renamed_consumption",
        )
        .entity_id
    )
    start = datetime(2026, 10, 20, tzinfo=UTC)
    recorder = Mock(
        async_add_executor_job=AsyncMock(
            return_value={
                entity_id: [
                    {"start": start.timestamp(), "sum": 12.0},
                    {"start": (start + timedelta(hours=1)).timestamp(), "sum": 15.0},
                ]
            }
        )
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager.get_recorder_instance",
        lambda _hass: recorder,
    )
    service = await _async_get_prediction_service(coordinator, "170579")
    assert recorder.async_add_executor_job.await_args.args[4] == {entity_id}
    assert service.get_raw_accumulator()
