"""Regression cases for Copilot feedback on historical imports and rediscovery."""

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.components.recorder.statistics import statistics_during_period

from custom_components.ekz_ha.api.models import (
    ConsumptionData,
    InstallationContract,
    InstallationSelectionData,
)
from custom_components.ekz_ha.coordinator import EkzCoordinator
from custom_components.ekz_ha.sensor import EkzMetaEntity
from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
from custom_components.ekz_ha.statistics.importer import ZRH, get_import_start
from custom_components.ekz_ha.statistics.production import ProductionImporter


async def test_supply_point_failure_leaves_production_rediscovery_retryable():
    contracts = InstallationSelectionData(
        contracts=[
            InstallationContract("", "", "", "covered", "site1", einzdat="2026-01-01"),
            InstallationContract("", "", "", "uncovered", "site2", einzdat="2026-01-01"),
        ]
    )
    api = Mock(
        get_consumption_installations=AsyncMock(side_effect=TimeoutError()),
        get_production_installations=AsyncMock(
            return_value=InstallationSelectionData(
                contracts=[InstallationContract("", "", "", "solar", "site1", einzdat="2026-01-01")]
            )
        ),
        get_production_15min=AsyncMock(
            return_value=ConsumptionData.from_dict(
                {"series": {"values": [{"timestamp": "20261020000000", "value": 1.0}]}}
            )
        ),
    )
    coordinator = SimpleNamespace(
        api_client=api,
        installations={key: {"contract_start": "2026-01-01"} for key in ("covered", "uncovered")},
        production_installations={},
    )
    with pytest.raises(TimeoutError):
        await EkzCoordinator._do_update_data(coordinator)
    assert coordinator.production_installations == {}
    api.get_consumption_installations.side_effect = None
    api.get_consumption_installations.return_value = contracts
    await EkzCoordinator._do_update_data(coordinator)
    assert api.get_production_installations.await_count == 2
    api.get_production_15min.assert_awaited_once()
    assert api.get_production_15min.await_args.args[0] == "uncovered"
    assert set(coordinator.production_installations) == {"solar", "uncovered"}


@pytest.mark.parametrize("contract_age", [1, 3, 29])
async def test_completed_search_never_resumes_before_recent_contract(
    freezer, monkeypatch, contract_age
):
    freezer.move_to("2026-10-28T12:00:00+00:00")
    today = date(2026, 10, 28)
    start = today - timedelta(days=contract_age)
    meta = EkzMetaEntity(Mock(), "test")
    meta.set_contract_start(start)
    meta.set_last_import(start)
    importer = ProductionImporter(Mock())
    fetch = AsyncMock(return_value=ConsumptionData())
    monkeypatch.setattr(importer, "fetch_data", fetch)
    await importer.import_statistics(None, "test", start, meta)
    assert meta._history_cursor >= start
    assert meta._empty_search_days is None
    fetch.return_value = ConsumptionData.from_dict(
        {
            "level": "QUARTER_HOUR",
            "series": {"values": [{"timestamp": "2026-10-28T00:00:00+01:00", "value": 2.0}]},
        }
    )
    result = await importer.import_statistics(None, "test", start, meta)
    assert fetch.await_args.args[1].date() >= start
    assert fetch.await_args.args[2].date() == today + timedelta(days=1)
    assert result["statistics"]


@pytest.mark.parametrize("kind", ["consumption", "production"])
async def test_empty_response_preserved_energy_is_offset_of_next_nonempty_chunk(
    hass, monkeypatch, freezer, kind
):
    freezer.move_to("2026-10-28T12:00:00+00:00")
    start = date(2000, 1, 1)
    first = datetime(2000, 1, 2, tzinfo=ZRH).astimezone(UTC)
    statistic_id = f"sensor.electricity_{kind}_ekz_test"
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id=f"offset-{kind}"))
    info = {"test": {"contract_start": start.isoformat()}}
    coordinator.installations = info if kind == "consumption" else {}
    coordinator.production_installations = info if kind == "production" else {"unused": {}}
    meta = EkzMetaEntity(
        coordinator, "test", model="Solar Meter" if kind == "production" else "Electricity Meter"
    )
    meta.set_contract_start(start)
    meta.set_last_import(start)
    meta._checkpoint_restored = True
    meta._checkpoint_needs_replay = False
    meta._recent_statistics = [
        {"start": first, "state": 1.0, "sum": 1.0, "source_resolution": "day"}
    ]
    coordinator.meta_entities = {"test": meta} if kind == "consumption" else {}
    coordinator.production_meta_entities = {"test": meta} if kind == "production" else {}
    coordinator._daily_statistics = {statistic_id: ["2000-01-02"]}
    persisted = [{"start": first.timestamp(), "state": 1.0, "sum": 1.0}]

    async def query(function, _hass, *args):
        if function is statistics_during_period:
            date_from, date_to = args[:2]
            return {
                statistic_id: [
                    row
                    for row in persisted
                    if date_from.timestamp() <= row["start"] < date_to.timestamp()
                ]
            }
        return {}

    recorder = Mock(
        async_add_executor_job=AsyncMock(side_effect=query),
        async_block_till_done=AsyncMock(),
    )
    for module in (
        "coordinator",
        "statistics.checkpoint",
        "statistics.prediction_manager",
        "statistics.recorder",
        "statistics.importer",
    ):
        monkeypatch.setattr(
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda _hass: recorder
        )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint.async_import_statistics", Mock()
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager.async_import_statistics", Mock()
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda *_args: None,
    )
    data = ConsumptionData.from_dict(
        {
            "level": "DAY",
            "series": {"values": [{"timestamp": "2000-02-01T00:00:00+01:00", "value": 2.0}]},
        }
    )
    fetch = AsyncMock(side_effect=[ConsumptionData(), data, data])
    importer_type = ConsumptionImporter if kind == "consumption" else ProductionImporter
    monkeypatch.setattr(importer_type, "fetch_data", fetch)
    await coordinator._do_update_data()
    assert meta._empty_search_from == date(2000, 1, 31)
    assert get_import_start(meta) == date(2000, 1, 31)
    assert coordinator._sum_offsets[statistic_id][date(2000, 1, 31)] == 1.0
    await coordinator._do_update_data()
    sums = coordinator.last_sums if kind == "consumption" else coordinator.last_production_sums
    assert sums["test"] == 3.0
    assert meta._recent_statistics[-1]["sum"] == 3.0


def test_pending_day_takes_priority_over_forward_search_cursor():
    meta = EkzMetaEntity(Mock(), "test")
    meta._empty_search_days = 960
    meta._empty_search_from = date(2026, 10, 20)
    meta.set_pending(date(2026, 10, 15), 100.0)
    assert get_import_start(meta) == date(2026, 10, 15)
