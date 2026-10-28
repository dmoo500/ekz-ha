"""Import diagnostics for consumption and production."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.ekz_ha import (
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
    installation.meta._recent_statistics = []
    await import_chunk(installation, start)
    assert installation.sensor.extra_state_attributes["last_result"] == "no_data"
    assert installation.meta.native_value == expected


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
    )
    hass.data[DOMAIN] = {"coordinator": coordinator}
    add = Mock()
    await async_setup_entry(hass, Mock(), add)
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
