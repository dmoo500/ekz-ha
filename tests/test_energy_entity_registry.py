"""External statistics migration with real recorder history."""

from datetime import UTC, date, datetime, timedelta
from functools import partial
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import get_metadata, statistics_during_period
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.ekz_ha.const import DOMAIN
from custom_components.ekz_ha.coordinator import EkzCoordinator
from custom_components.ekz_ha.sensor import EkzMetaEntity
from custom_components.ekz_ha.sensor import async_setup_entry as setup_sensors
from custom_components.ekz_ha.statistics.checkpoint import (
    _restore_energy_import_checkpoints,
    _restore_import_checkpoint,
)
from custom_components.ekz_ha.statistics.migration import (
    _async_prepare_energy_statistic,
    async_initialize_migration,
    async_migrate_statistics,
)
from custom_components.ekz_ha.statistics.prediction_manager import _async_get_prediction_service
from custom_components.ekz_ha.statistics.recorder import (
    _async_write_statistics,
    _energy_statistic_id,
    _make_stat_meta,
    _prediction_statistic_id,
)

START = datetime(2026, 10, 20, tzinfo=UTC)
END = START + timedelta(hours=3)


@pytest.fixture(params=["consumption", "production", "prediction"])
async def energy(hass, request):
    kind = request.param
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id="external"))
    coordinator.installations = {"170579": {}} if kind != "production" else {}
    coordinator.production_installations = {"170579": {}} if kind == "production" else {}
    suffix = "consumption_170579_prediction" if kind == "prediction" else f"{kind}_170579"
    entity = er.async_get(hass).async_get_or_create(
        "sensor", DOMAIN, f"ekz_electricity_{suffix}", suggested_object_id=f"renamed_{kind}"
    )
    target = (
        _prediction_statistic_id(hass, "170579")
        if kind == "prediction"
        else _energy_statistic_id(hass, "170579", kind)
    )
    await async_initialize_migration(coordinator)
    coordinator._external_migrations[target].update(cursor=START.isoformat(), end=END.isoformat())
    await coordinator._external_migration_store.async_save(coordinator._external_migrations)
    return coordinator, kind, entity.entity_id, target


async def history(recorder, hass, statistic_id, start=START, end=END):
    result = await recorder.async_add_executor_job(
        statistics_during_period, hass, START, end, {statistic_id}, "hour", None, {"state", "sum"}
    )
    return [(row["start"], row["state"], row["sum"]) for row in result.get(statistic_id, [])]


async def seed(recorder, hass, statistic_id, total=12.0, count=2):
    _async_write_statistics(
        hass,
        _make_stat_meta(statistic_id),
        [
            StatisticData(
                start=START + timedelta(hours=index), state=2.0 + index, sum=total + 3 * index
            )
            for index in range(count)
        ],
    )
    await recorder.async_add_executor_job(recorder.block_till_done)


@pytest.mark.parametrize("source_kind", ["renamed", "fixed", "stored", "old_consumption"])
async def test_migration_preserves_exact_history_and_restart_state(
    recorder_mock, energy, source_kind, monkeypatch
):
    coordinator, kind, entity_id, target = energy
    sources = coordinator._external_migrations[target]["sources"]
    if source_kind == "old_consumption" and kind != "consumption":
        pytest.skip("The oldest alias applies only to consumption.")
    if source_kind == "renamed":
        source = entity_id
    elif source_kind == "stored":
        source = f"sensor.previous_{kind}_name"
        coordinator._statistic_ids[f"{kind}:170579"] = source
        coordinator._external_migrations.pop(target)
        await async_initialize_migration(coordinator)
        coordinator._external_migrations[target].update(
            cursor=START.isoformat(), end=END.isoformat()
        )
    elif source_kind == "old_consumption":
        source = "sensor.ekz_electricity_consumption_170579"
    else:
        source = next(value for value in sources if value.startswith("sensor.electricity_"))
    await coordinator._external_migration_store.async_save(coordinator._external_migrations)
    await seed(recorder_mock, coordinator.hass, source)
    before = await history(recorder_mock, coordinator.hass, source)
    checkpoint = {"history_cursor": "2026-10-19"}
    coordinator._import_checkpoints = {source: checkpoint}
    coordinator._daily_statistics = {source: ["2026-10-19"]}
    coordinator._sum_offsets = {source: {date(2026, 10, 20): 10.0}}
    await async_migrate_statistics(coordinator, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == before
    assert await history(recorder_mock, coordinator.hass, source) == before
    metadata = await recorder_mock.async_add_executor_job(
        partial(get_metadata, coordinator.hass, statistic_ids={source, target})
    )
    assert metadata[target][1]["source"] == DOMAIN
    assert metadata[source][1]["source"] == "recorder"
    assert coordinator._import_checkpoints == {target: checkpoint}
    assert await coordinator._import_checkpoint_store.async_load() == {target: checkpoint}
    assert await coordinator._source_resolution_store.async_load() == {target: ["2026-10-19"]}
    assert coordinator._sum_offsets == {target: {date(2026, 10, 20): 10.0}}
    assert coordinator._external_migrations[target]["complete"]
    restarted = EkzCoordinator(
        coordinator.hass, Mock(), timedelta(hours=4), Mock(entry_id="external")
    )
    monkeypatch.setattr(restarted, "_async_discover_installations", AsyncMock())
    await restarted._async_setup()
    await async_migrate_statistics(restarted, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == before
    assert restarted._import_checkpoints == {target: checkpoint}


async def test_partial_copy_is_resumable(recorder_mock, energy):
    coordinator, kind, source, target = energy
    await seed(recorder_mock, coordinator.hass, source)
    await seed(recorder_mock, coordinator.hass, target, count=1)
    await async_migrate_statistics(coordinator, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == await history(
        recorder_mock, coordinator.hass, source
    )


async def test_conflicting_history_stops_without_deleting_data(recorder_mock, energy):
    coordinator, kind, source, target = energy
    await seed(recorder_mock, coordinator.hass, source)
    await seed(recorder_mock, coordinator.hass, target, total=25.0)
    before = await history(recorder_mock, coordinator.hass, target)
    with pytest.raises(UpdateFailed, match="Conflicting external history"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == before
    assert not coordinator._external_migrations[target]["complete"]


async def test_checkpoint_save_failure_retries_after_restart(recorder_mock, energy, monkeypatch):
    coordinator, kind, source, target = energy
    await seed(recorder_mock, coordinator.hass, source)
    coordinator._import_checkpoints = {source: {"history_cursor": "2026-10-19"}}
    await coordinator._import_checkpoint_store.async_save(coordinator._import_checkpoints)
    with monkeypatch.context() as patch:
        patch.setattr(
            coordinator._import_checkpoint_store,
            "async_save",
            AsyncMock(side_effect=OSError("disk unavailable")),
        )
        with pytest.raises(UpdateFailed, match="disk unavailable"):
            await async_migrate_statistics(coordinator, "170579", kind)
    assert not coordinator._external_migrations[target]["complete"]
    restarted = EkzCoordinator(
        coordinator.hass, Mock(), timedelta(hours=4), Mock(entry_id="external")
    )
    monkeypatch.setattr(restarted, "_async_discover_installations", AsyncMock())
    await restarted._async_setup()
    await async_migrate_statistics(restarted, "170579", kind)
    assert restarted._external_migrations[target]["complete"]
    assert restarted._import_checkpoints == {target: {"history_cursor": "2026-10-19"}}


async def test_unverified_write_does_not_advance_progress(recorder_mock, energy, monkeypatch):
    coordinator, kind, source, target = energy
    await seed(recorder_mock, coordinator.hass, source)
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.migration._async_write_statistics", Mock()
    )
    with pytest.raises(UpdateFailed, match="Could not verify migrated history"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert coordinator._external_migrations[target]["cursor"] == START.isoformat()
    assert not coordinator._external_migrations[target]["complete"]


async def test_multiple_batches_resume_at_the_verified_boundary(recorder_mock, energy, monkeypatch):
    coordinator, kind, source, target = energy
    future = START + timedelta(days=367)
    _async_write_statistics(
        coordinator.hass,
        _make_stat_meta(source),
        [
            StatisticData(start=START, sum=12.0, state=2.0),
            StatisticData(start=future, sum=15.0, state=3.0),
        ],
    )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    writes = 0

    def interrupted_write(hass, metadata, rows):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("interrupted second batch")
        _async_write_statistics(hass, metadata, rows)

    with monkeypatch.context() as patch:
        patch.setattr(
            "custom_components.ekz_ha.statistics.migration._async_write_statistics",
            interrupted_write,
        )
        with pytest.raises(UpdateFailed, match="interrupted second batch"):
            await async_migrate_statistics(coordinator, "170579", kind)
    assert (
        coordinator._external_migrations[target]["cursor"]
        == (START + timedelta(days=366)).isoformat()
    )
    assert not coordinator._external_migrations[target]["complete"]
    restarted = EkzCoordinator(
        coordinator.hass, Mock(), timedelta(hours=4), Mock(entry_id="external")
    )
    monkeypatch.setattr(restarted, "_async_discover_installations", AsyncMock())
    await restarted._async_setup()
    await async_migrate_statistics(restarted, "170579", kind)
    assert restarted._external_migrations[target]["complete"]
    assert await history(
        recorder_mock, coordinator.hass, target, end=future + timedelta(hours=1)
    ) == await history(recorder_mock, coordinator.hass, source, end=future + timedelta(hours=1))


async def test_missing_hourly_state_is_preserved_as_null(recorder_mock, energy):
    coordinator, kind, source, target = energy
    _async_write_statistics(
        coordinator.hass, _make_stat_meta(source), [StatisticData(start=START, sum=12.0)]
    )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    await async_migrate_statistics(coordinator, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == [
        (START.timestamp(), None, 12.0)
    ]


async def test_incompatible_metadata_does_not_import_as_kwh(recorder_mock, energy):
    coordinator, kind, source, target = energy
    metadata = _make_stat_meta(source)
    metadata["unit_of_measurement"] = "Wh"
    _async_write_statistics(
        coordinator.hass, metadata, [StatisticData(start=START, state=2.0, sum=12.0)]
    )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    with pytest.raises(UpdateFailed, match="Incompatible statistics metadata"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == []
    assert not coordinator._external_migrations[target]["complete"]


async def test_fresh_installation_does_not_require_energy_entities(recorder_mock, energy):
    coordinator, kind, source, target = energy
    er.async_get(coordinator.hass).async_remove(source)
    await async_migrate_statistics(coordinator, "170579", kind)
    assert coordinator._external_migrations[target]["complete"]
    await seed(recorder_mock, coordinator.hass, target)
    assert len(await history(recorder_mock, coordinator.hass, target)) == 2


async def test_energy_dashboard_does_not_require_a_current_sensor_state(recorder_mock, energy):
    from homeassistant.components.energy import validate

    validate_stat = getattr(
        validate,
        "_async_validate_stat_common",
        getattr(validate, "_async_validate_usage_stat", None),
    )
    assert validate_stat is not None, "Home Assistant energy statistic validator was not found"

    coordinator, kind, source, target = energy
    await seed(recorder_mock, coordinator.hass, source)
    await async_migrate_statistics(coordinator, "170579", kind)
    metadata = await recorder_mock.async_add_executor_job(
        partial(get_metadata, coordinator.hass, statistic_ids={target})
    )
    issues = Mock()
    assert coordinator.hass.states.get(target) is None
    validate_stat(
        coordinator.hass,
        metadata,
        target,
        ["energy"],
        {"energy": ["kWh"]},
        "entity_unexpected_unit_energy",
        issues,
    )
    issues.add_issue.assert_not_called()


async def test_sensor_setup_removes_only_saved_energy_entities_and_shows_repair(energy):
    coordinator, _, entity_id, target = energy
    registry = er.async_get(coordinator.hass)
    diagnostic = registry.async_get_or_create(
        "sensor",
        DOMAIN,
        "ekz_electricity_consumption_170579_meta",
        suggested_object_id="last_import",
    )
    coordinator.hass.data[DOMAIN] = {"coordinator": coordinator}
    add = Mock()
    await setup_sensors(coordinator.hass, coordinator.config, add)
    assert registry.async_get(entity_id) is None
    assert registry.async_get(diagnostic.entity_id) is not None
    assert all(entity.device_class != "energy" for entity in add.call_args.args[0])
    issue = ir.async_get(coordinator.hass).async_get_issue(DOMAIN, "external_statistics_external")
    assert issue is not None
    assert target in issue.translation_placeholders["statistics"]
    assert entity_id in coordinator._external_migrations[target]["sources"]


async def test_prediction_rebuild_reads_external_consumption(hass, monkeypatch):
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id="prediction"))
    target = _energy_statistic_id(hass, "170579", "consumption")
    recorder = Mock(
        async_add_executor_job=AsyncMock(
            return_value={
                target: [
                    {"start": START.timestamp(), "sum": 12.0},
                    {"start": (START + timedelta(hours=1)).timestamp(), "sum": 15.0},
                ]
            }
        )
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager.get_recorder_instance",
        lambda _hass: recorder,
    )
    service = await _async_get_prediction_service(coordinator, "170579")
    assert recorder.async_add_executor_job.await_args.args[4] == {target}
    assert service.get_raw_accumulator()


async def test_prepare_migrates_predictions_before_measurement_imports(recorder_mock, hass):
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id="prepare"))
    coordinator.installations = {"170579": {}}
    await async_initialize_migration(coordinator)
    consumption = _energy_statistic_id(hass, "170579", "consumption")
    prediction = _prediction_statistic_id(hass, "170579")
    for target in (consumption, prediction):
        coordinator._external_migrations[target].update(
            cursor=START.isoformat(), end=END.isoformat()
        )
        source = next(
            source
            for source in coordinator._external_migrations[target]["sources"]
            if source.startswith("sensor.electricity_")
        )
        await seed(recorder_mock, hass, source)
    assert (
        await _async_prepare_energy_statistic(coordinator, "170579", "consumption") == consumption
    )
    for target in (consumption, prediction):
        assert coordinator._external_migrations[target]["complete"]
        assert len(await history(recorder_mock, hass, target)) == 2


async def test_renamed_checkpoint_is_restored_before_and_after_migration(recorder_mock, energy):
    coordinator, kind, source, target = energy
    if kind == "prediction":
        pytest.skip("Prediction statistics have no import checkpoint entity.")
    meta = EkzMetaEntity(
        coordinator, "170579", model="Solar Meter" if kind == "production" else "Electricity Meter"
    )
    coordinator.meta_entities = {"170579": meta} if kind == "consumption" else {}
    coordinator.production_meta_entities = {"170579": meta} if kind == "production" else {}
    coordinator._import_checkpoints = {
        source: {
            "history_cursor": "2026-10-19",
            "last_successful_import": START.isoformat(),
            "pending_from": "2026-10-20",
            "pending_offset": 10.0,
        }
    }
    _restore_energy_import_checkpoints(coordinator)
    assert meta._last_successful_import == START
    assert meta._pending_from == date(2026, 10, 20)
    await async_migrate_statistics(coordinator, "170579", kind)
    _restore_import_checkpoint(coordinator, target, meta)
    assert meta._last_successful_import == START
    assert coordinator._sum_offsets[target][date(2026, 10, 20)] == 10.0


async def test_checkpoint_moved_after_empty_restore_is_not_skipped(recorder_mock, energy):
    coordinator, kind, source, target = energy
    if kind == "prediction":
        pytest.skip("Prediction statistics have no import checkpoint entity.")
    meta = EkzMetaEntity(
        coordinator, "170579", model="Solar Meter" if kind == "production" else "Electricity Meter"
    )
    coordinator.meta_entities = {"170579": meta} if kind == "consumption" else {}
    coordinator.production_meta_entities = {"170579": meta} if kind == "production" else {}
    _restore_energy_import_checkpoints(coordinator)
    coordinator._import_checkpoints[source] = {
        "history_cursor": "2026-10-19",
        "last_successful_import": START.isoformat(),
    }
    await async_migrate_statistics(coordinator, "170579", kind)
    _restore_import_checkpoint(coordinator, target, meta)
    assert meta._last_successful_import == START


async def test_setup_storage_failure_preserves_entities_and_closes_api(hass, monkeypatch):
    from homeassistant.exceptions import ConfigEntryNotReady

    from custom_components.ekz_ha import async_setup_entry

    entity = er.async_get(hass).async_get_or_create(
        "sensor", DOMAIN, "ekz_electricity_consumption_170579", suggested_object_id="keep_history"
    )
    api = Mock(close=AsyncMock())
    monkeypatch.setattr("custom_components.ekz_ha.EkzApiClient", Mock(return_value=api))

    async def discover(coordinator):
        coordinator.installations = {"170579": {}}
        coordinator._external_migration_store.async_save = AsyncMock(
            side_effect=OSError("disk unavailable")
        )

    monkeypatch.setattr(EkzCoordinator, "async_config_entry_first_refresh", discover)
    entry = Mock(entry_id="storage-failure", data={"user": "test", "password": "test"})
    with pytest.raises(ConfigEntryNotReady, match="Cannot persist external statistics migration"):
        await async_setup_entry(hass, entry)
    api.close.assert_awaited_once()
    assert er.async_get(hass).async_get(entity.entity_id) is not None
