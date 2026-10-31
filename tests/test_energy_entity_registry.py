"""External statistics migration with real recorder history."""

import asyncio
from datetime import UTC, date, datetime, timedelta
from functools import partial
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import (
    get_last_statistics,
    get_metadata,
    statistics_during_period,
)
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ekz_ha.api.models import ConsumptionData
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
    _validate_history_joins,
    async_initialize_migration,
    async_migrate_statistics,
)
from custom_components.ekz_ha.statistics.prediction_manager import _async_get_prediction_service
from custom_components.ekz_ha.statistics.recorder import (
    _async_clear_statistics,
    _async_wait_for_statistics,
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
        statistics_during_period, hass, start, end, {statistic_id}, "hour", None, {"state", "sum"}
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


async def test_migration_rejects_legacy_id_reused_by_unrelated_entity(recorder_mock, energy):
    coordinator, kind, _, target = energy
    alias = next(
        value
        for value in coordinator._external_migrations[target]["sources"]
        if value.startswith("sensor.electricity_")
    )
    er.async_get(coordinator.hass).async_get_or_create(
        "sensor", "other_integration", "unrelated_energy", suggested_object_id=alias[7:]
    )
    await seed(recorder_mock, coordinator.hass, alias)
    with pytest.raises(UpdateFailed, match="owned by another entity"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert await history(recorder_mock, coordinator.hass, target) == []
    assert len(await history(recorder_mock, coordinator.hass, alias)) == 2


@pytest.mark.parametrize("ownership", ["unique_id", "config_entry"])
async def test_initialization_rejects_reused_legacy_id(energy, ownership):
    coordinator, _, _, target = energy
    other_entry = None
    if ownership == "config_entry":
        other_entry = MockConfigEntry(domain=DOMAIN, entry_id="different-entry")
        other_entry.add_to_hass(coordinator.hass)
    alias = next(
        value
        for value in coordinator._external_migrations[target]["sources"]
        if value.startswith("sensor.electricity_")
    )
    er.async_get(coordinator.hass).async_get_or_create(
        "sensor",
        DOMAIN,
        "different_ekz_installation"
        if ownership == "unique_id"
        else f"ekz_{target.partition(':')[2]}",
        suggested_object_id=alias[7:],
        config_entry=other_entry,
    )
    coordinator._external_migrations.pop(target)
    with pytest.raises(UpdateFailed, match="owned by another entity"):
        await async_initialize_migration(coordinator)
    assert target not in coordinator._external_migrations


@pytest.mark.parametrize("complete", [False, True])
async def test_migration_retry_refuses_silently_omitting_new_registered_source(energy, complete):
    coordinator, _, source, target = energy
    registry = er.async_get(coordinator.hass)
    registry.async_update_entity(source, new_entity_id="sensor.renamed_again")
    coordinator._external_migrations[target]["complete"] = complete
    with pytest.raises(UpdateFailed, match="Legacy entity ID changed"):
        await async_initialize_migration(coordinator)
    assert "sensor.renamed_again" not in coordinator._external_migrations[target]["sources"]


@pytest.mark.parametrize("compatible", [False, True])
async def test_completed_beta1_migration_validates_legacy_joins_without_overwriting_imports(
    recorder_mock, energy, compatible
):
    coordinator, kind, source, target = energy
    state = coordinator._external_migrations[target]
    other = next(value for value in state["sources"] if value != source)
    state.pop("tail")
    state.update(complete=True, cursor=END.isoformat())
    first = StatisticData(start=START, state=2.0, sum=100.0)
    second = StatisticData(
        start=START + timedelta(hours=1), state=3.0, sum=103.0 if compatible else 3.0
    )
    for statistic_id, rows in ((source, [first]), (other, [second])):
        _async_write_statistics(coordinator.hass, _make_stat_meta(statistic_id), rows)
    # A later normal import may correct old rows and extend the target beyond migration.
    await seed(recorder_mock, coordinator.hass, target, total=200.0, count=4)
    before = await history(recorder_mock, coordinator.hass, target, end=END + timedelta(hours=2))
    if compatible:
        await async_migrate_statistics(coordinator, "170579", kind)
        assert "tail" in coordinator._external_migrations[target]
    else:
        with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
            await async_migrate_statistics(coordinator, "170579", kind)
        assert "tail" not in coordinator._external_migrations[target]
    assert (
        await history(recorder_mock, coordinator.hass, target, end=END + timedelta(hours=2))
        == before
    )


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


async def test_completed_beta1_validation_resumes_across_batch_boundary(
    recorder_mock, energy, monkeypatch
):
    coordinator, kind, source, target = energy
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    boundary = epoch + timedelta(days=366)
    state = coordinator._external_migrations[target]
    other = next(value for value in state["sources"] if value != source)
    state.pop("tail")
    state.update(complete=True, cursor=END.isoformat())
    for statistic_id, row in (
        (source, StatisticData(start=boundary - timedelta(hours=1), state=2.0, sum=100.0)),
        (other, StatisticData(start=boundary, state=3.0, sum=3.0)),
    ):
        _async_write_statistics(coordinator.hass, _make_stat_meta(statistic_id), [row])
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)

    def interrupted(hass, start, *args):
        if start == boundary:
            raise RuntimeError("validation interrupted")
        return statistics_during_period(hass, start, *args)

    with monkeypatch.context() as patch:
        patch.setattr(
            "custom_components.ekz_ha.statistics.migration.statistics_during_period",
            interrupted,
        )
        with pytest.raises(UpdateFailed, match="validation interrupted"):
            await async_migrate_statistics(coordinator, "170579", kind)
    saved = await coordinator._external_migration_store.async_load()
    assert saved[target]["validation_cursor"] == boundary.isoformat()
    assert saved[target]["validation_tail"]["sum"] == 100.0
    coordinator._external_migrations = saved
    with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert "tail" not in coordinator._external_migrations[target]


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


@pytest.mark.parametrize("next_sum", [3.0, 103.0, 203.0])
@pytest.mark.parametrize("cross_batch", [False, True])
async def test_disjoint_history_joins_validate_cumulative_baselines(
    recorder_mock, energy, next_sum, cross_batch
):
    coordinator, kind, source, target = energy
    other = next(
        value for value in coordinator._external_migrations[target]["sources"] if value != source
    )
    boundary = START + (timedelta(days=366) if cross_batch else timedelta(hours=1))
    coordinator._external_migrations[target]["end"] = (boundary + timedelta(hours=1)).isoformat()
    _async_write_statistics(
        coordinator.hass,
        _make_stat_meta(source),
        [StatisticData(start=boundary - timedelta(hours=1), state=2.0, sum=100.0)],
    )
    _async_write_statistics(
        coordinator.hass,
        _make_stat_meta(other),
        [StatisticData(start=boundary, state=3.0, sum=next_sum)],
    )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    if next_sum == 103.0:
        await async_migrate_statistics(coordinator, "170579", kind)
        assert coordinator._external_migrations[target]["complete"]
    else:
        with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
            await async_migrate_statistics(coordinator, "170579", kind)
        assert not coordinator._external_migrations[target]["complete"]
        assert (
            await history(
                recorder_mock,
                coordinator.hass,
                target,
                start=boundary,
                end=boundary + timedelta(hours=1),
            )
            == []
        )
        # Resume also must reject the bad boundary rather than treating partial data as completion.
        with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
            await async_migrate_statistics(coordinator, "170579", kind)


def test_disjoint_target_tail_is_not_overwritten_by_an_unrelated_baseline():
    with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
        _validate_history_joins(
            "ekz_ha:consumption",
            {
                "sensor.old": {0.0: (2.0, 100.0)},
                "ekz_ha:consumption": {3600.0: (3.0, 3.0)},
            },
            None,
        )


def test_existing_partial_copy_cannot_hide_incompatible_legacy_joins():
    target = "ekz_ha:consumption"
    with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
        _validate_history_joins(
            target,
            {
                "sensor.first": {0.0: (2.0, 100.0)},
                "sensor.second": {3600.0: (3.0, 3.0)},
                target: {0.0: (2.0, 100.0), 3600.0: (3.0, 3.0)},
            },
            None,
        )


async def test_fresh_installation_skips_empty_scans_and_rekeys_checkpoint(energy, monkeypatch):
    coordinator, kind, source, target = energy
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.migration.get_recorder_instance",
        lambda _hass: recorder,
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.migration._async_wait_for_statistics", AsyncMock()
    )
    coordinator._import_checkpoints[source] = {"history_cursor": "2026-10-19"}
    await async_migrate_statistics(coordinator, "170579", kind)
    recorder.async_add_executor_job.assert_awaited_once()
    assert coordinator._external_migrations[target]["complete"]
    assert coordinator._import_checkpoints == {target: {"history_cursor": "2026-10-19"}}


async def test_recorder_timeout_has_actionable_error_and_cause(hass, monkeypatch):
    import asyncio

    recorder = Mock()
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.get_recorder_instance", lambda _hass: recorder
    )
    real_wait = asyncio.wait_for

    async def short_wait(awaitable, *, timeout):
        assert timeout == 60
        return await real_wait(awaitable, timeout=0.001)

    monkeypatch.setattr("custom_components.ekz_ha.statistics.recorder.asyncio.wait_for", short_wait)
    with pytest.raises(UpdateFailed, match="Recorder did not finish.*within 60 seconds") as raised:
        await _async_wait_for_statistics(hass, "ekz_ha:electricity_consumption_test")
    assert isinstance(raised.value.__cause__, TimeoutError)


async def test_beta1_progress_without_tail_revalidates_the_previous_boundary(recorder_mock, energy):
    coordinator, kind, source, target = energy
    boundary = START + timedelta(hours=1)
    state = coordinator._external_migrations[target]
    state.pop("tail")
    state["cursor"] = boundary.isoformat()
    other = next(value for value in state["sources"] if value != source)
    for statistic_id in (source, target):
        _async_write_statistics(
            coordinator.hass,
            _make_stat_meta(statistic_id),
            [StatisticData(start=START, state=2.0, sum=100.0)],
        )
    _async_write_statistics(
        coordinator.hass,
        _make_stat_meta(other),
        [StatisticData(start=boundary, state=3.0, sum=3.0)],
    )
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert not state["complete"]
    assert await history(recorder_mock, coordinator.hass, target, start=boundary) == []


async def test_beta1_prefix_checks_earlier_batch_joins_before_completing(recorder_mock, energy):
    coordinator, kind, source, target = energy
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    boundary = epoch + timedelta(days=366)
    state = coordinator._external_migrations[target]
    state.pop("tail")
    state["cursor"] = (boundary + timedelta(days=367)).isoformat()
    state["end"] = state["cursor"]
    other = next(value for value in state["sources"] if value != source)
    first = StatisticData(start=boundary - timedelta(hours=1), state=2.0, sum=100.0)
    second = StatisticData(start=boundary, state=3.0, sum=3.0)
    later = StatisticData(start=boundary + timedelta(days=366), state=2.0, sum=5.0)
    for statistic_id, rows in (
        (source, [first]),
        (other, [second, later]),
        (target, [first, second, later]),
    ):
        _async_write_statistics(coordinator.hass, _make_stat_meta(statistic_id), rows)
    await recorder_mock.async_add_executor_job(recorder_mock.block_till_done)
    with pytest.raises(UpdateFailed, match="Incompatible cumulative history join"):
        await async_migrate_statistics(coordinator, "170579", kind)
    assert not coordinator._external_migrations[target]["complete"]


async def test_statistics_clear_waits_for_verified_deletion(recorder_mock, energy):
    coordinator, _, source, target = energy
    await seed(recorder_mock, coordinator.hass, source)
    await seed(recorder_mock, coordinator.hass, target)
    assert await _async_clear_statistics(coordinator.hass, [source, target])
    metadata = await recorder_mock.async_add_executor_job(
        partial(get_metadata, coordinator.hass, statistic_ids={source, target})
    )
    assert metadata == {}
    assert await history(recorder_mock, coordinator.hass, target) == []


async def test_statistics_clear_refuses_success_when_metadata_remains(energy, monkeypatch):
    coordinator, _, _, target = energy
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={target: (1, _make_stat_meta(target))})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.get_recorder_instance", lambda _hass: recorder
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder._async_wait_for_statistics", AsyncMock()
    )
    with pytest.raises(UpdateFailed, match="Statistics reset did not remove metadata"):
        await _async_clear_statistics(coordinator.hass, [target])


async def test_statistics_clear_does_not_finish_before_recorder_barrier(energy, monkeypatch):
    coordinator, _, _, target = energy
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    started = asyncio.Event()
    completed = asyncio.Event()

    async def wait(_hass, _statistic_id):
        started.set()
        await completed.wait()

    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.get_recorder_instance",
        lambda _hass: recorder,
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder._async_wait_for_statistics", wait
    )
    task = asyncio.create_task(_async_clear_statistics(coordinator.hass, [target]))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not task.done()
        recorder.async_add_executor_job.assert_not_awaited()
    finally:
        completed.set()
        await task
    assert task.result()


async def test_statistics_clear_propagates_recorder_errors(energy, monkeypatch):
    coordinator, _, _, target = energy
    recorder = Mock(async_clear_statistics=Mock(side_effect=RuntimeError("recorder unavailable")))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.recorder.get_recorder_instance",
        lambda _hass: recorder,
    )
    with pytest.raises(RuntimeError, match="recorder unavailable"):
        await _async_clear_statistics(coordinator.hass, [target])


@pytest.mark.parametrize("kind", ["consumption", "production"])
@pytest.mark.parametrize(
    ("collision", "save_failure"),
    [(collision, None) for collision in (None, "fixed", "stored", "migration")]
    + [(None, failure) for failure in ("initial", "source", "checkpoint", "prediction", "final")],
)
async def test_reset_reimports_from_contract_start_not_old_recorder_tail(
    recorder_mock, hass, monkeypatch, kind, collision, save_failure
):
    from custom_components.ekz_ha import async_setup_entry
    from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
    from custom_components.ekz_ha.statistics.production import ProductionImporter

    coordinators = []
    api = Mock(get_production_installations=AsyncMock(return_value=Mock(contracts=[])))
    monkeypatch.setattr("custom_components.ekz_ha.EkzApiClient", Mock(return_value=api))
    monkeypatch.setattr("custom_components.ekz_ha.async_at_started", Mock(return_value=Mock()))
    monkeypatch.setattr(
        "custom_components.ekz_ha.async_track_time_change", Mock(return_value=Mock())
    )
    entry = Mock(entry_id=f"reset-recorder-{kind}", data={"user": "test", "password": "test"})

    async def discover(coordinator):
        coordinators.append(coordinator)
        coordinator.installations = (
            {"825821": {"contract_start": "2024-03-10"}} if kind == "consumption" else {}
        )
        coordinator.production_installations = (
            {"825821": {"contract_start": "2024-03-10"}} if kind == "production" else {}
        )

    async def forward(_entry, _platforms):
        await setup_sensors(hass, entry, Mock())

    monkeypatch.setattr(EkzCoordinator, "async_config_entry_first_refresh", discover)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(hass.config_entries, "async_forward_entry_setups", forward)
        assert await async_setup_entry(hass, entry)
    coordinator = coordinators[0]
    target = _energy_statistic_id(hass, "825821", kind)
    await seed(recorder_mock, hass, target, total=5177.964)
    meta = (
        coordinator.meta_entities if kind == "consumption" else coordinator.production_meta_entities
    )["825821"]
    meta.set_contract_start(date(2024, 3, 10))
    meta.set_last_import(date(2026, 10, 30))
    meta._last_successful_import = START
    coordinator._import_checkpoints[target] = {"history_cursor": "2026-10-30"}
    fetch = AsyncMock(return_value=ConsumptionData())
    monkeypatch.setattr(
        ConsumptionImporter if kind == "consumption" else ProductionImporter, "fetch_data", fetch
    )

    async def refreshed():
        assert (
            await recorder_mock.async_add_executor_job(
                get_last_statistics, hass, 1, target, True, {"sum"}
            )
            == {}
        )
        await coordinator._do_update_data()

    monkeypatch.setattr(coordinator, "async_request_refresh", refreshed)
    if save_failure and not collision:
        store = {
            "initial": coordinator._external_migration_store,
            "source": coordinator._source_resolution_store,
            "checkpoint": coordinator._import_checkpoint_store,
            "prediction": coordinator._prediction_store,
            "final": coordinator._external_migration_store,
        }[save_failure]
        original_save = store.async_save
        calls = 0

        async def failing_save(data):
            nonlocal calls
            calls += 1
            if save_failure != "final" or calls == 2:
                raise OSError("reset storage failure")
            await original_save(data)

        with monkeypatch.context() as patch:
            patch.setattr(store, "async_save", failing_save)
            with pytest.raises(OSError, match="reset storage failure"):
                await hass.services.async_call(DOMAIN, "reset_statistics", {}, blocking=True)
        saved = await coordinator._external_migration_store.async_load()
        if save_failure == "initial":
            assert not saved[target].get("reset_pending")
            assert len(await history(recorder_mock, hass, target)) == 2
            assert meta._last_import == date(2026, 10, 30)
        else:
            assert saved[target]["reset_pending"]
            with pytest.raises(UpdateFailed, match="Statistics reset is incomplete"):
                await coordinator._do_update_data()
            restarted = EkzCoordinator(hass, api, timedelta(hours=4), entry)
            with monkeypatch.context() as patch:
                patch.setattr(restarted, "_async_discover_installations", AsyncMock())
                await restarted._async_setup()
            with pytest.raises(UpdateFailed, match="Statistics reset is incomplete"):
                await restarted._do_update_data()
            coordinator.catching_up = {"825821": False}
            prediction_service = AsyncMock()
            with monkeypatch.context() as patch:
                patch.setattr(
                    "custom_components.ekz_ha.statistics.prediction_manager._async_get_prediction_service",
                    prediction_service,
                )
                await coordinator.async_extend_predictions(datetime.now(UTC))
            prediction_service.assert_not_awaited()
        fetch.assert_not_awaited()
    if collision:
        alias = (
            f"sensor.electricity_{kind}_ekz_825821"
            if collision == "fixed"
            else f"sensor.previous_{kind}"
        )
        if collision == "stored":
            coordinator._statistic_ids[f"{kind}:825821"] = alias
        elif collision == "migration":
            coordinator._external_migrations[target]["sources"].append(alias)
        er.async_get(hass).async_get_or_create(
            "sensor", "other_integration", "unrelated_energy", suggested_object_id=alias[7:]
        )
        await seed(recorder_mock, hass, alias)
        with pytest.raises(UpdateFailed, match="owned by another entity"):
            await hass.services.async_call(DOMAIN, "reset_statistics", {}, blocking=True)
        assert len(await history(recorder_mock, hass, alias)) == 2
        assert len(await history(recorder_mock, hass, target)) == 2
        assert coordinator._import_checkpoints[target] == {"history_cursor": "2026-10-30"}
        assert meta._last_import == date(2026, 10, 30)
        fetch.assert_not_awaited()
        return
    await hass.services.async_call(DOMAIN, "reset_statistics", {}, blocking=True)
    assert fetch.await_args.args[1].date() == date(2024, 3, 10)
    assert meta._last_import != date(2026, 10, 30)
