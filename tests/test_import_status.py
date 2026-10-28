"""Import diagnostics for consumption and production."""

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.ekz_ha import (
    EkzCoordinator,
    _async_import_chunk,
    _persist_import_checkpoint,
    _restore_import_checkpoint,
)
from custom_components.ekz_ha.api.models import ConsumptionData
from custom_components.ekz_ha.import_status import ImportStatus, get_import_status
from custom_components.ekz_ha.sensor import EkzImportStatusEntity, EkzMetaEntity
from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
from custom_components.ekz_ha.statistics.importer import ZRH
from custom_components.ekz_ha.statistics.production import ProductionImporter


@pytest.fixture(params=["consumption", "production"])
def installation(request, monkeypatch):
    kind = request.param
    coordinator = Mock(
        last_update_success=True,
        last_exception=None,
        next_update_time=datetime(2026, 10, 28, 16, tzinfo=UTC),
        hass=Mock(),
        _daily_statistics={},
        _import_checkpoints={},
        _sum_offsets={},
        _import_checkpoint_store=Mock(async_save=AsyncMock()),
        _source_resolution_store=Mock(async_save=AsyncMock()),
    )
    meta = EkzMetaEntity(
        coordinator, "825821", model="Solar Meter" if kind == "production" else "Electricity Meter"
    )
    sensor = EkzImportStatusEntity(coordinator, meta, kind)
    importer_type = ConsumptionImporter if kind == "consumption" else ProductionImporter
    importer = importer_type(Mock())
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={}),
        async_block_till_done=AsyncMock(),
    )
    for module in ("custom_components.ekz_ha", "custom_components.ekz_ha.statistics.importer"):
        monkeypatch.setattr(f"{module}.get_recorder_instance", lambda _hass: recorder)
    enqueue = Mock()
    monkeypatch.setattr("custom_components.ekz_ha.async_import_statistics", enqueue)
    return SimpleNamespace(
        coordinator=coordinator,
        meta=meta,
        sensor=sensor,
        importer=importer,
        statistic_id=f"sensor.electricity_{kind}_ekz_825821",
        enqueue=enqueue,
    )


async def import_chunk(installation, start):
    return await _async_import_chunk(
        installation.coordinator,
        installation.importer,
        "825821",
        start,
        installation.meta,
        installation.statistic_id,
        0.0,
    )


@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("bounded", [False, True])
async def test_diagnostics_publish_before_blocked_fetch_and_after_chunk(
    installation, monkeypatch, freezer, hass, retry, bounded
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    if retry:
        installation.coordinator.last_update_success = False
        installation.coordinator.last_exception = UpdateFailed("previous failure")
    snapshots = []

    def write():
        snapshots.append(
            (installation.sensor.native_value, dict(installation.sensor.extra_state_attributes))
        )

    for entity, entity_id in (
        (installation.meta, "sensor.test_last_import"),
        (installation.sensor, "sensor.test_import_status"),
    ):
        entity.hass = hass
        entity.entity_id = entity_id
        monkeypatch.setattr(entity, "async_write_ha_state", Mock(side_effect=write))
    entered = asyncio.Event()
    release = asyncio.Event()
    data = ConsumptionData.from_dict(
        {
            "level": "DAY",
            "series": {"values": [{"timestamp": "2007-01-01T00:00:00Z", "value": 1.0}]},
        }
    )
    calls = 0

    async def fetch(*args):
        nonlocal calls
        calls += 1
        if bounded and calls == 1:
            return data
        entered.set()
        await release.wait()
        return data

    if bounded:
        installation.meta._empty_search_days = 3650
    monkeypatch.setattr(installation.importer, "fetch_data", fetch)
    task = asyncio.create_task(import_chunk(installation, datetime(2000, 1, 1, tzinfo=ZRH)))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert not task.done()
        state, attributes = snapshots[-1]
        assert state == "searching"
        assert attributes["requested_from"] == ("2007-01-01" if bounded else "2000-01-01")
        assert attributes["requested_to"] == ("2007-01-31" if bounded else "2000-01-31")
        assert attributes["last_attempt"] is not None
        assert attributes["last_result"] is None
        if bounded:
            assert snapshots[0][1]["requested_from"] == "2000-01-01"
    finally:
        release.set()
        await task
    assert snapshots[-1][0] == "importing"
    assert snapshots[-1][1]["last_result"] == "imported"
    assert installation.meta.native_value is not None
    # A new coordinator failure must still override a completed local chunk.
    installation.coordinator.last_update_success = False
    installation.coordinator.last_exception = UpdateFailed("later installation failed")
    assert installation.sensor.native_value == "failed"


def test_initial_diagnostics_do_not_claim_success(installation):
    installation.meta.set_contract_start(date(2000, 1, 1))
    installation.meta.set_last_import(date(2000, 1, 1))
    assert installation.meta.native_value is None
    assert installation.sensor.native_value == "waiting"
    assert installation.sensor.available
    assert installation.sensor.extra_state_attributes["last_attempt"] is None
    assert installation.sensor.unique_id.endswith("_import_status")
    assert installation.sensor.device_info == installation.meta.device_info


async def test_empty_initial_search_expands_without_fake_last_import(
    installation, monkeypatch, freezer
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    fetch = AsyncMock(return_value=ConsumptionData())
    monkeypatch.setattr(installation.importer, "fetch_data", fetch)
    start = datetime(2000, 1, 1, tzinfo=ZRH)
    for days in (30, 60):
        await import_chunk(installation, start)
        attributes = installation.sensor.extra_state_attributes
        assert installation.sensor.native_value == "searching"
        assert attributes["requested_from"] == "2000-01-01"
        assert attributes["requested_to"] == (start.date() + timedelta(days=days)).isoformat()
        assert attributes["last_result"] == "no_data"
        assert attributes["error_type"] is None
        assert attributes["last_attempt"] == "2026-10-28T12:00:00+00:00"
        assert installation.meta.native_value is None
    installation.enqueue.assert_not_called()


@pytest.mark.parametrize("historical", [False, True])
async def test_success_and_empty_updates_keep_latest_success(
    installation, monkeypatch, freezer, historical
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    start = datetime(2026, 8, 1, tzinfo=ZRH) if historical else datetime(2026, 10, 25, tzinfo=ZRH)
    data = ConsumptionData.from_dict(
        {"level": "DAY", "series": {"values": [{"timestamp": start.isoformat(), "value": 6.0}]}}
    )
    fetch = AsyncMock(return_value=data)
    monkeypatch.setattr(installation.importer, "fetch_data", fetch)
    await import_chunk(installation, start)
    assert installation.sensor.native_value == ("importing" if historical else "current")
    assert installation.sensor.extra_state_attributes["last_result"] == "imported"
    expected = start.astimezone(UTC)
    assert installation.meta.native_value == expected

    fetch.return_value = ConsumptionData()
    result = await import_chunk(installation, start)
    assert bool(result["statistics"]) is not historical
    assert installation.sensor.extra_state_attributes["last_result"] == "no_data"
    assert installation.meta.native_value == expected


@pytest.mark.parametrize("end_day", [27, 28, 29])
async def test_current_requires_window_including_local_today(
    installation, monkeypatch, freezer, end_day
):
    freezer.move_to("2026-10-27 23:30:00+00:00")
    start = date(2026, 10, end_day) - timedelta(days=30)
    installation.meta.set_last_import(start)
    installation.meta.set_history_cursor(start)
    monkeypatch.setattr(
        installation.importer, "fetch_data", AsyncMock(return_value=ConsumptionData())
    )
    await import_chunk(installation, datetime(2026, 1, 1, tzinfo=ZRH))
    assert installation.sensor.extra_state_attributes["requested_to"] == f"2026-10-{end_day}"
    assert installation.sensor.native_value == ("current" if end_day == 29 else "importing")


async def test_failure_diagnostics_survive_restart_without_changing_safe_checkpoint(
    installation, monkeypatch, freezer, hass
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    timestamp = datetime(2026, 8, 2, tzinfo=UTC)
    installation.meta._last_successful_import = timestamp
    installation.meta.set_history_cursor(date(2026, 8, 30))
    installation.meta.set_pending(date(2026, 8, 2), 100.0)
    storage_key = f"ekz_ha.test.failed_status.{installation.statistic_id}"
    installation.coordinator._import_checkpoint_store = Store(hass, 1, storage_key)
    await _persist_import_checkpoint(
        installation.coordinator,
        installation.statistic_id,
        installation.meta,
        [{"start": timestamp, "sum": 101.0, "state": 1.0}],
    )
    before = await Store(hass, 1, storage_key).async_load()
    monkeypatch.setattr(
        installation.importer, "fetch_data", AsyncMock(side_effect=TimeoutError("private"))
    )
    with pytest.raises(UpdateFailed):
        await import_chunk(installation, datetime(2026, 1, 1, tzinfo=ZRH))
    saved = await Store(hass, 1, storage_key).async_load()
    state = saved[installation.statistic_id]
    assert state["import_status"]["state"] == "failed"
    assert state["import_status"]["requested_from"] == "2026-08-02"
    assert state["import_status"]["last_attempt"] == "2026-10-28T12:00:00+00:00"
    assert state["import_status"]["error_type"] == "TimeoutError"
    assert "private" not in str(state)
    assert {key: value for key, value in state.items() if key != "import_status"} == {
        key: value
        for key, value in before[installation.statistic_id].items()
        if key != "import_status"
    }
    restored = EkzMetaEntity(Mock(), "825821")
    _restore_import_checkpoint(
        SimpleNamespace(_import_checkpoints=saved), installation.statistic_id, restored
    )
    assert restored._import_status == installation.meta._import_status
    assert restored.native_value == timestamp
    assert restored._pending_from == date(2026, 8, 2)
    assert restored._checkpoint_needs_replay


async def test_failure_metadata_write_error_preserves_original_error_and_retry(
    installation, monkeypatch, caplog
):
    installation.coordinator._import_checkpoint_store.async_save.side_effect = OSError("disk full")
    fetch = AsyncMock(side_effect=TimeoutError("API timeout"))
    monkeypatch.setattr(installation.importer, "fetch_data", fetch)
    with pytest.raises(UpdateFailed) as raised:
        await import_chunk(installation, datetime(2026, 1, 1, tzinfo=ZRH))
    assert isinstance(raised.value.__cause__, TimeoutError)
    assert "Could not persist failure diagnostics" in caplog.text
    assert installation.sensor.extra_state_attributes["error_type"] == "TimeoutError"
    installation.coordinator._import_checkpoint_store.async_save.side_effect = None
    fetch.side_effect = None
    fetch.return_value = ConsumptionData()
    await import_chunk(installation, datetime(2026, 1, 1, tzinfo=ZRH))
    assert installation.sensor.extra_state_attributes["last_result"] == "no_data"
    assert installation.sensor.extra_state_attributes["error_type"] is None


@pytest.mark.parametrize("first_kind", ["consumption", "production"])
async def test_all_checkpoints_restored_before_first_installation_fails(monkeypatch, first_kind):
    timestamp = datetime(2026, 8, 2, tzinfo=UTC)
    coordinator = SimpleNamespace(
        hass=Mock(),
        api_client=Mock(),
        installations={"first": {"contract_start": "2026-01-01"}}
        if first_kind == "consumption"
        else {},
        production_installations={
            "first" if first_kind == "production" else "later": {"contract_start": "2026-01-01"},
            "last": {"contract_start": "2026-01-01"},
        },
        meta_entities={},
        production_meta_entities={},
        _import_checkpoints={},
        _get_running_sum_before=AsyncMock(return_value=0.0),
        last_sums={},
        last_production_sums={},
        catching_up={},
    )
    for kind, installations, entities in (
        ("consumption", coordinator.installations, coordinator.meta_entities),
        ("production", coordinator.production_installations, coordinator.production_meta_entities),
    ):
        for key in installations:
            meta = EkzMetaEntity(Mock(), key)
            meta._last_successful_import = timestamp
            meta._import_status.begin(timestamp, timestamp + timedelta(days=30), False)
            meta._import_status.last_result = "imported"
            statistic_id = f"sensor.electricity_{kind}_ekz_{key}"
            await _persist_import_checkpoint(
                coordinator, statistic_id, meta, [{"start": timestamp, "sum": 2.0, "state": 1.0}]
            )
            entities[key] = EkzMetaEntity(Mock(), key)
    recorder = Mock(
        async_block_till_done=AsyncMock(), async_add_executor_job=AsyncMock(return_value={})
    )
    replay = Mock()
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda _hass: recorder)
    monkeypatch.setattr("custom_components.ekz_ha.async_import_statistics", replay)

    async def fail(*args):
        assert replay.call_count == 1
        recorder.async_block_till_done.assert_awaited_once()
        raise UpdateFailed("first installation failed")

    monkeypatch.setattr("custom_components.ekz_ha._async_import_chunk", fail)
    with pytest.raises(UpdateFailed):
        await EkzCoordinator._do_update_data(coordinator)
    all_metas = [
        *coordinator.meta_entities.values(),
        *coordinator.production_meta_entities.values(),
    ]
    for meta in all_metas:
        assert meta.native_value == timestamp
        assert meta._import_status.last_result == "imported"
        assert meta._import_status.requested_from == timestamp.date()
    assert coordinator.production_meta_entities["last"]._checkpoint_needs_replay


async def test_failed_attempt_retains_range_and_last_success(installation, monkeypatch, freezer):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    previous = datetime(2026, 10, 15, tzinfo=UTC)
    installation.meta._last_successful_import = previous
    installation.meta.set_last_import(date(2026, 10, 16))
    fetch = AsyncMock(side_effect=TimeoutError("do not expose account details"))
    monkeypatch.setattr(installation.importer, "fetch_data", fetch)
    with pytest.raises(UpdateFailed):
        await import_chunk(installation, datetime(2026, 1, 1, tzinfo=ZRH))
    attributes = installation.sensor.extra_state_attributes
    assert installation.sensor.native_value == "failed"
    assert attributes["requested_from"] == "2026-10-16"
    assert attributes["requested_to"] == "2026-10-29"
    assert attributes["last_result"] == "failed"
    assert attributes["error_type"] == "TimeoutError"
    assert "account details" not in str(attributes)
    assert installation.meta.native_value == previous
    assert installation.meta.available

    fetch.side_effect = None
    fetch.return_value = ConsumptionData()
    await import_chunk(installation, datetime(2026, 1, 1, tzinfo=ZRH))
    assert installation.sensor.native_value == "current"
    assert installation.sensor.extra_state_attributes["error_type"] is None


async def test_failed_chunk_publishes_diagnostics_before_coordinator_completion(
    installation, monkeypatch, hass
):
    states = []
    installation.sensor.hass = hass
    installation.sensor.entity_id = "sensor.test_import_status"
    monkeypatch.setattr(
        installation.sensor,
        "async_write_ha_state",
        lambda: states.append(
            (installation.sensor.native_value, installation.sensor.extra_state_attributes)
        ),
    )
    monkeypatch.setattr(installation.importer, "fetch_data", AsyncMock(side_effect=TimeoutError()))
    with pytest.raises(UpdateFailed):
        await import_chunk(installation, datetime(2000, 1, 1, tzinfo=ZRH))
    assert states[0][0] == "searching"
    assert states[-1][0] == "failed"
    assert states[-1][1]["last_result"] == "failed"
    assert states[-1][1]["error_type"] == "TimeoutError"


async def test_bounded_refetch_reports_actual_failed_range(installation, monkeypatch, freezer):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    installation.meta._empty_search_days = 3650
    start = datetime(2000, 1, 1, tzinfo=ZRH)
    first = datetime(2007, 1, 1, tzinfo=ZRH)
    data = ConsumptionData.from_dict(
        {"level": "DAY", "series": {"values": [{"timestamp": first.isoformat(), "value": 6.0}]}}
    )
    monkeypatch.setattr(
        installation.importer, "fetch_data", AsyncMock(side_effect=[data, TimeoutError()])
    )
    with pytest.raises(UpdateFailed):
        await import_chunk(installation, start)
    assert installation.sensor.extra_state_attributes["requested_from"] == "2007-01-01"
    assert installation.sensor.extra_state_attributes["requested_to"] == "2007-01-31"


async def test_checkpoint_round_trip_and_legacy_success(installation, hass):
    status = get_import_status(installation.meta)
    status.begin(datetime(2026, 8, 1, tzinfo=ZRH), datetime(2026, 8, 31, tzinfo=ZRH), True)
    status.last_result = "no_data"
    timestamp = datetime(2026, 8, 2, tzinfo=UTC)
    installation.meta._last_successful_import = timestamp
    storage_key = f"ekz_ha.test.status.{installation.statistic_id}"
    installation.coordinator._import_checkpoint_store = Store(hass, 1, storage_key)
    await _persist_import_checkpoint(
        installation.coordinator,
        installation.statistic_id,
        installation.meta,
        [{"start": timestamp, "state": 1.0, "sum": 1.0}],
    )
    states = await Store(hass, 1, storage_key).async_load()
    for legacy in (False, True):
        if legacy:
            states[installation.statistic_id].pop("import_status")
            states[installation.statistic_id].pop("last_successful_import")
        restored = EkzMetaEntity(Mock(), "825821")
        _restore_import_checkpoint(
            SimpleNamespace(_import_checkpoints=states), installation.statistic_id, restored
        )
        assert restored.native_value == timestamp
        if not legacy:
            assert restored._import_status == status


async def test_last_import_does_not_move_back_during_reconciliation(
    installation, monkeypatch, freezer
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    previous = datetime(2026, 10, 25, tzinfo=UTC)
    installation.meta._last_successful_import = previous
    data = ConsumptionData.from_dict(
        {
            "level": "DAY",
            "series": {"values": [{"timestamp": "2026-10-15T00:00:00Z", "value": 6.0}]},
        }
    )
    monkeypatch.setattr(installation.importer, "fetch_data", AsyncMock(return_value=data))
    await import_chunk(installation, datetime(2026, 10, 15, tzinfo=ZRH))
    assert installation.meta.native_value == previous


async def test_checkpoint_failure_does_not_publish_success(installation, monkeypatch, freezer):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    installation.coordinator._import_checkpoint_store.async_save.side_effect = OSError("disk full")
    data = ConsumptionData.from_dict(
        {
            "level": "DAY",
            "series": {"values": [{"timestamp": "2026-10-15T00:00:00Z", "value": 6.0}]},
        }
    )
    monkeypatch.setattr(installation.importer, "fetch_data", AsyncMock(return_value=data))
    with pytest.raises(UpdateFailed):
        await import_chunk(installation, datetime(2026, 10, 15, tzinfo=ZRH))
    assert installation.meta.native_value is None
    assert installation.sensor.native_value == "failed"
    assert installation.sensor.extra_state_attributes["error_type"] == "OSError"
    installation.enqueue.assert_not_called()


@pytest.mark.parametrize("consumption", [False, True])
async def test_sensor_setup_creates_separate_import_statuses(hass, consumption):
    from custom_components.ekz_ha.const import DOMAIN
    from custom_components.ekz_ha.sensor import async_setup_entry

    coordinator = Mock(
        installations={"825821": {}} if consumption else {},
        production_installations={"825821": {}},
        _import_checkpoints={},
    )
    hass.data[DOMAIN] = {"coordinator": coordinator}
    add = Mock()
    await async_setup_entry(hass, Mock(), add)
    assert add.call_args.args[1] is False
    sensors = [
        entity for entity in add.call_args.args[0] if isinstance(entity, EkzImportStatusEntity)
    ]
    assert len(sensors) == (2 if consumption else 1)
    assert len({sensor.unique_id for sensor in sensors}) == len(sensors)
    assert any("production" in sensor.unique_id for sensor in sensors)


def test_coordinator_failure_keeps_diagnostics_available(installation):
    installation.coordinator.last_update_success = False
    installation.coordinator.last_exception = UpdateFailed("private error message")
    assert installation.sensor.available
    assert installation.sensor.native_value == "failed"
    attributes = installation.sensor.extra_state_attributes
    assert attributes["coordinator_error_type"] == "UpdateFailed"
    assert attributes["next_update"] is None
    assert "private" not in str(attributes)


def test_status_round_trip_and_translation_states(installation):
    status = ImportStatus()
    status.begin(datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 10, 31, tzinfo=UTC), False)
    status.last_result = "imported"
    assert ImportStatus.restore({"state": status.state, **status.as_attributes()}) == status
    root = Path(__file__).parent.parent / "custom_components" / "ekz_ha"
    for filename in ("strings.json", "translations/en.json", "translations/de.json"):
        translations = json.loads((root / filename).read_text())["entity"]["sensor"]
        for kind in ("consumption", "production"):
            assert set(translations[f"import_status_{kind}"]["state"]) == set(
                installation.sensor.options
            )


async def test_setup_returns_with_registered_sensors_while_initial_import_is_blocked(
    hass, monkeypatch
):
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.ekz_ha import async_setup_entry
    from custom_components.ekz_ha.const import DOMAIN
    from custom_components.ekz_ha.sensor import async_setup_entry as setup_sensors

    entry = MockConfigEntry(domain=DOMAIN, data={"user": "test", "password": "test"})
    started = asyncio.Event()
    release = asyncio.Event()
    registrations = []

    async def discover(coordinator):
        coordinator.installations = {"test": {"contract_start": "2026-01-01"}}

    async def blocked_import(coordinator):
        assert coordinator.meta_entities["test"] in registrations
        assert hass.services.has_service(DOMAIN, "reset_statistics")
        started.set()
        await release.wait()

    async def forward(_entry, _platforms):
        def add(entities, update_before_add):
            assert update_before_add is False
            registrations.extend(entities)

        await setup_sensors(hass, entry, add)

    monkeypatch.setattr("custom_components.ekz_ha.EkzApiClient", Mock())
    monkeypatch.setattr(EkzCoordinator, "async_config_entry_first_refresh", discover)
    monkeypatch.setattr(EkzCoordinator, "async_request_refresh", blocked_import)
    with patch.object(hass.config_entries, "async_forward_entry_setups", side_effect=forward):
        assert await async_setup_entry(hass, entry)
    await asyncio.wait_for(started.wait(), timeout=1)
    tasks = list(entry._background_tasks)
    assert len(tasks) == 1
    task = tasks[0]
    assert not task.done()
    try:
        # HA's startup barrier must not wait for the historical import.
        await asyncio.wait_for(hass.async_block_till_done(), timeout=1)
        assert not task.done()
        assert registrations
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.parametrize("kind", ["consumption", "production", "both"])
@pytest.mark.parametrize("changed", [False, True])
async def test_offline_restart_registers_saved_diagnostics_and_retries_discovery(
    hass, monkeypatch, kind, changed
):
    from custom_components.ekz_ha import async_setup_entry
    from custom_components.ekz_ha.const import DOMAIN
    from custom_components.ekz_ha.sensor import async_setup_entry as setup_sensors

    entry = Mock(entry_id=f"offline-{kind}", data={"user": "test", "password": "test"})
    entry.async_create_background_task.side_effect = lambda _hass, coroutine, _name: (
        coroutine.close()
    )
    known = {
        "consumption": {"825821": {"contract_start": "2026-01-01"}} if kind != "production" else {},
        "production": {"solar": {"contract_start": "2026-02-01"}} if kind != "consumption" else {},
    }
    await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.installations").async_save(known)
    timestamp = datetime(2026, 8, 2, tzinfo=UTC)
    checkpoint = {
        f"sensor.electricity_{data_type}_ekz_{key}": {
            "last_successful_import": timestamp.isoformat(),
            "import_status": {
                "state": "failed",
                "last_result": "failed",
                "requested_from": "2026-08-01",
                "requested_to": "2026-08-31",
                "error_type": "TimeoutError",
            },
        }
        for data_type, installations in known.items()
        for key in installations
    }
    await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.import_checkpoint").async_save(checkpoint)
    api = Mock(
        get_consumption_installations=AsyncMock(side_effect=TimeoutError("offline")),
    )
    monkeypatch.setattr("custom_components.ekz_ha.EkzApiClient", Mock(return_value=api))

    async def first_refresh(coordinator):
        try:
            await coordinator._async_setup()
        except TimeoutError as err:
            coordinator.last_exception = err
            coordinator.last_update_success = False
            raise ConfigEntryNotReady from err

    monkeypatch.setattr(EkzCoordinator, "async_config_entry_first_refresh", first_refresh)
    add = Mock()

    async def forward(_entry, platforms):
        assert platforms == ["sensor"]
        await setup_sensors(hass, entry, add)

    with patch.object(hass.config_entries, "async_forward_entry_setups", side_effect=forward):
        assert await async_setup_entry(hass, entry)
    coordinator = hass.data[DOMAIN]["coordinator"]
    assert coordinator.installations == known["consumption"]
    assert coordinator.production_installations == known["production"]
    sensors = add.call_args.args[0]
    metas = [entity for entity in sensors if isinstance(entity, EkzMetaEntity)]
    statuses = [entity for entity in sensors if isinstance(entity, EkzImportStatusEntity)]
    assert len(metas) == len(checkpoint)
    for meta in metas:
        assert meta.available
        assert meta.native_value == timestamp
    for status in statuses:
        assert status.available
        assert status.native_value == "failed"
        assert status.extra_state_attributes["requested_from"] == "2026-08-01"
        assert status.extra_state_attributes["coordinator_error_type"] == "TimeoutError"
    assert coordinator._retry_setup

    # No cached installation may import until discovery succeeds again.
    imports = AsyncMock()
    monkeypatch.setattr(coordinator, "_do_update_data", imports)
    with pytest.raises(TimeoutError):
        await coordinator._async_update_data()
    imports.assert_not_awaited()

    from custom_components.ekz_ha.api.models import InstallationContract, InstallationSelectionData

    api.get_consumption_installations.side_effect = None
    api.get_consumption_installations.return_value = InstallationSelectionData(
        contracts=[InstallationContract.from_dict({"anlage": "825821", "einzdat": "2026-01-01"})]
        if known["consumption"]
        else []
    )
    api.get_production_installations = AsyncMock(
        return_value=InstallationSelectionData(
            contracts=[InstallationContract.from_dict({"anlage": "solar", "einzdat": "2026-02-01"})]
            if known["production"]
            else []
        )
    )
    api.get_production_15min = AsyncMock(return_value=ConsumptionData())
    expected = known
    if changed:
        expected = {
            data_type: {f"{key}-new": info for key, info in installations.items()}
            for data_type, installations in known.items()
        }
        for data_type in ("consumption", "production"):
            method = getattr(api, f"get_{data_type}_installations")
            method.return_value = InstallationSelectionData(
                contracts=[
                    InstallationContract.from_dict(
                        {"anlage": key, "einzdat": info["contract_start"]}
                    )
                    for key, info in expected[data_type].items()
                ]
            )
        api.get_production_installations.side_effect = TimeoutError("rediscovery interrupted")
        with pytest.raises(TimeoutError):
            await coordinator._async_update_data()
        imports.assert_not_awaited()
        assert coordinator._retry_setup
        api.get_production_installations.side_effect = None
    with patch.object(hass.config_entries, "async_reload", new_callable=AsyncMock) as reload:
        await coordinator._async_update_data()
        await hass.async_block_till_done()
        if changed:
            imports.assert_not_awaited()
            reload.assert_awaited_once_with(entry.entry_id)
        else:
            imports.assert_awaited_once()
            reload.assert_not_awaited()
    assert not coordinator._retry_setup
    assert await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.installations").async_load() == expected


async def test_empty_historical_search_remains_searching_with_recorded_rows(
    installation, monkeypatch, freezer
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    start = datetime(2000, 1, 1, tzinfo=ZRH)
    installation.meta._empty_search_days = 30
    installation.meta._recent_statistics = [
        {"start": start.astimezone(UTC), "state": 1.0, "sum": 1.0}
    ]
    monkeypatch.setattr(
        installation.importer, "fetch_data", AsyncMock(return_value=ConsumptionData())
    )
    result = await import_chunk(installation, start)
    assert result["statistics"]
    assert not result["fetched_data"]
    assert installation.meta._empty_search_days == 60
    assert installation.sensor.native_value == "searching"
    assert installation.sensor.extra_state_attributes["last_result"] == "no_data"


@pytest.mark.parametrize("recorded_daily", [False, True])
@pytest.mark.parametrize("accepted_next_hour", [False, True])
async def test_only_accepted_readings_report_imported(
    installation, monkeypatch, freezer, recorded_daily, accepted_next_hour
):
    freezer.move_to("2026-10-28 12:00:00+00:00")
    start = datetime(2026, 10, 25, tzinfo=ZRH)
    installation.meta.set_last_import(start.date())
    installation.meta.set_history_cursor(start.date())
    installation.meta._recent_statistics = [
        {"start": start.astimezone(UTC), "state": 24.0, "sum": 24.0}
    ]
    if recorded_daily:
        installation.meta._daily_statistics = {start.date().isoformat()}
    values = [{"timestamp": start.isoformat(), "value": 0.1}]
    if accepted_next_hour:
        next_time = start + (timedelta(days=1) if recorded_daily else timedelta(hours=1))
        values.append({"timestamp": next_time.isoformat(), "value": 0.2})
    data = ConsumptionData.from_dict({"level": "QUARTER_HOUR", "series": {"values": values}})
    monkeypatch.setattr(installation.importer, "fetch_data", AsyncMock(return_value=data))
    result = await import_chunk(installation, start)
    assert result["statistics"][0]["state"] == 24.0
    assert result["fetched_data"] is accepted_next_hour
    assert installation.sensor.extra_state_attributes["last_result"] == (
        "imported" if accepted_next_hour else "no_data"
    )


@pytest.mark.parametrize("failure", ["offline", "auth"])
async def test_startup_without_usable_cache_does_not_hide_setup_errors(hass, monkeypatch, failure):
    from homeassistant.exceptions import ConfigEntryAuthFailed

    from custom_components.ekz_ha import async_setup_entry
    from custom_components.ekz_ha.const import DOMAIN

    entry = Mock(entry_id=f"no-fallback-{failure}", data={"user": "test", "password": "test"})
    monkeypatch.setattr(
        "custom_components.ekz_ha.EkzApiClient", Mock(return_value=Mock(close=AsyncMock()))
    )
    error = ConfigEntryAuthFailed("reauth required") if failure == "auth" else ConfigEntryNotReady()
    if failure == "auth":
        await Store(hass, 1, f"{DOMAIN}.{entry.entry_id}.installations").async_save(
            {"consumption": {"825821": {"contract_start": "2026-01-01"}}, "production": {}}
        )
    monkeypatch.setattr(
        EkzCoordinator, "async_config_entry_first_refresh", AsyncMock(side_effect=error)
    )
    with patch.object(
        hass.config_entries, "async_forward_entry_setups", new_callable=AsyncMock
    ) as forward:
        with pytest.raises(type(error)):
            await async_setup_entry(hass, entry)
        forward.assert_not_awaited()
