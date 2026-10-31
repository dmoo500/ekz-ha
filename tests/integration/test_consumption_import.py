"""Integration tests for consumption data import using real JSON fixtures."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.ekz_ha.api.models import (
    ConsumptionData,
    InstallationContract,
    InstallationSelectionData,
)
from custom_components.ekz_ha.coordinator import EkzCoordinator
from custom_components.ekz_ha.data import DataAggregator, StatisticsTransformer
from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
from custom_components.ekz_ha.statistics.importer import ZRH
from custom_components.ekz_ha.statistics.prediction import PredictionService
from custom_components.ekz_ha.statistics.production import ProductionImporter
from custom_components.ekz_ha.statistics.recorder import _make_stat_meta


class TestConsumptionImportWithRealData:
    """Test consumption import with real JSON data from EKZ API."""

    @pytest.fixture
    def json_15min_data(self):
        """Load real 15-minute data from JSON fixture."""
        fixture_path = (
            Path(__file__).parent.parent
            / "fixtures"
            / "import_2026-03-06-2026-03-16-PK_VERB_15MIN.json"
        )
        with open(fixture_path) as f:
            return json.load(f)

    @pytest.fixture
    def json_daily_data(self):
        """Load real daily data from JSON fixture."""
        fixture_path = (
            Path(__file__).parent.parent
            / "fixtures"
            / "import_2026_03_16-2026_04_16-PK_VERB_TAG_METER.json"
        )
        with open(fixture_path) as f:
            return json.load(f)

    def test_parse_15min_json_to_consumption_data(self, json_15min_data):
        """Test parsing 15-minute JSON response to ConsumptionData model."""
        data = ConsumptionData.from_dict(json_15min_data)

        assert data.level == "QUARTER_HOUR"
        assert not data.is_empty()

        # Check that we have HT and NT series
        assert data.series_ht is not None
        assert data.series_nt is not None
        assert len(data.series_ht.values) > 0
        assert len(data.series_nt.values) > 0

        # Verify values are filtered (no NOT_AVAILABLE)
        for value in data.series_ht.values:
            assert value.status not in ("NOT_AVAILABLE", "MISSING")

    def test_parse_daily_json_to_consumption_data(self, json_daily_data):
        """Test parsing daily JSON response to ConsumptionData model."""
        data = ConsumptionData.from_dict(json_daily_data)

        # Daily data might not have HT/NT split, could be in 'series'
        assert data.level in ("DAY", "QUARTER_HOUR")

        # Get all values
        all_values = data.get_all_values()
        assert len(all_values) > 0

        # Verify timestamps are valid
        for value in all_values[:10]:  # Check first 10
            assert value.timestamp is not None

    def test_full_15min_import_workflow(self, json_15min_data):
        """Test complete import workflow: parse → aggregate → transform."""
        # 1. Parse JSON to model
        consumption_data = ConsumptionData.from_dict(json_15min_data)

        # 2. Get all values
        values = consumption_data.get_all_values()
        assert len(values) > 0

        # 3. Merge tariffs (HT + NT)
        aggregator = DataAggregator()
        merged_values = aggregator.merge_tariffs(values)

        # Should have fewer or equal values after merge (HT+NT combined)
        assert len(merged_values) <= len(values)

        # 4. Aggregate to hourly (4 x 15-min slots)
        hourly_values = aggregator.aggregate_to_hourly(merged_values)

        # Should have ~1/4 of original slots
        assert len(hourly_values) > 0
        assert len(hourly_values) <= len(merged_values) / 2  # At least some aggregation

        # 5. Transform to statistics format
        transformer = StatisticsTransformer()
        statistics = transformer.values_to_statistics(hourly_values, running_sum_offset=0.0)

        # Verify statistics structure
        assert len(statistics) == len(hourly_values)

        for stat in statistics[:5]:  # Check first 5
            assert "start" in stat
            assert "sum" in stat
            assert "state" in stat
            assert isinstance(stat["start"], datetime)
            assert stat["start"].tzinfo is not None  # Must have timezone
            assert stat["sum"] >= 0
            assert stat["state"] >= 0

    def test_running_sum_accumulation(self, json_15min_data):
        """Test that running sum accumulates correctly."""
        consumption_data = ConsumptionData.from_dict(json_15min_data)
        values = consumption_data.get_all_values()

        aggregator = DataAggregator()
        merged = aggregator.merge_tariffs(values)
        hourly = aggregator.aggregate_to_hourly(merged)

        transformer = StatisticsTransformer()

        # Test with offset
        offset = 1000.0
        statistics = transformer.values_to_statistics(hourly, running_sum_offset=offset)

        # First stat should start above offset
        assert statistics[0]["sum"] >= offset

        # Sum should be monotonically increasing
        for i in range(1, len(statistics)):
            assert statistics[i]["sum"] >= statistics[i - 1]["sum"]

        # Last sum should be offset + total consumption
        total_consumption = sum(v.value for v in hourly)
        assert abs(statistics[-1]["sum"] - (offset + total_consumption)) < 0.01

    def test_data_quality_checks(self, json_15min_data):
        """Test data quality: no duplicates, timestamps ordered, values reasonable."""
        consumption_data = ConsumptionData.from_dict(json_15min_data)
        values = consumption_data.get_all_values()

        aggregator = DataAggregator()
        merged = aggregator.merge_tariffs(values)

        # Check for duplicates by timestamp
        timestamps = [v.timestamp for v in merged]
        assert len(timestamps) == len(set(timestamps)), "Found duplicate timestamps"

        # Check timestamps are ordered
        for i in range(1, len(merged)):
            assert merged[i].timestamp >= merged[i - 1].timestamp, "Timestamps not ordered"

        # Check values are reasonable (0-100 kWh per 15-min slot)
        for value in merged:
            assert 0 <= value.value <= 100, f"Unreasonable value: {value.value} kWh"

    def test_empty_data_handling(self):
        """Test handling of empty/invalid data."""
        # Empty response
        empty_data = ConsumptionData.from_dict({})
        assert empty_data.is_empty()

        # No values
        no_values_data = ConsumptionData.from_dict(
            {
                "seriesHt": {"level": "QUARTER_HOUR", "values": []},
                "seriesNt": {"level": "QUARTER_HOUR", "values": []},
            }
        )
        assert no_values_data.is_empty()

    def test_timezone_handling(self, json_15min_data):
        """Test that all timestamps are properly converted to UTC."""
        consumption_data = ConsumptionData.from_dict(json_15min_data)
        values = consumption_data.get_all_values()

        aggregator = DataAggregator()
        merged = aggregator.merge_tariffs(values)
        hourly = aggregator.aggregate_to_hourly(merged)

        transformer = StatisticsTransformer()
        statistics = transformer.values_to_statistics(hourly)

        # All timestamps must be timezone-aware and in UTC
        for stat in statistics:
            dt = stat["start"]
            assert dt.tzinfo is not None, "Timestamp missing timezone"
            assert dt.tzinfo.tzname(dt) in ("UTC", "UTC+00:00"), f"Not UTC: {dt.tzinfo}"


async def test_empty_discovery_is_retryable_and_production_only_is_supported():
    production = InstallationSelectionData(
        contracts=[InstallationContract("", "", "", "solar", "", einzdat="2026-01-01")]
    )
    coordinator = SimpleNamespace(
        _external_migration_store=Mock(async_load=AsyncMock(return_value={})),
        _statistic_id_store=Mock(async_load=AsyncMock(return_value={})),
        api_client=SimpleNamespace(
            get_consumption_installations=AsyncMock(return_value=InstallationSelectionData()),
            get_production_installations=AsyncMock(
                side_effect=[InstallationSelectionData(), production]
            ),
        ),
    )
    coordinator._async_discover_installations = MethodType(
        EkzCoordinator._async_discover_installations, coordinator
    )
    with pytest.raises(UpdateFailed):
        await EkzCoordinator._async_setup(coordinator)
    await EkzCoordinator._async_setup(coordinator)
    assert coordinator.installations == {}
    assert coordinator.production_installations == {"solar": {"contract_start": "2026-01-01"}}


@pytest.mark.parametrize("production_available", [False, True])
async def test_config_flow_checks_production_before_rejecting_account(
    monkeypatch, production_available
):
    from custom_components.ekz_ha.config_flow import EkzConfigFlow

    client = SimpleNamespace(
        get_consumption_installations=AsyncMock(return_value=InstallationSelectionData()),
        get_production_installations=AsyncMock(
            return_value=InstallationSelectionData(
                contracts=[InstallationContract("", "", "", "solar", "")]
                if production_available
                else []
            )
        ),
        close=AsyncMock(),
    )
    monkeypatch.setattr("custom_components.ekz_ha.config_flow.EkzApiClient", lambda *args: client)
    flow = SimpleNamespace(
        _reauth=False,
        _credentials={"user": "test", "password": "test", "totp_secret": "JBSWY3DPEHPK3PXP"},
        async_create_entry=Mock(return_value="created"),
        async_show_form=Mock(return_value="form"),
    )

    result = await EkzConfigFlow.async_step_confirm(flow, {})

    client.get_production_installations.assert_awaited_once()
    client.close.assert_awaited_once()
    if production_available:
        assert result == "created"
        flow.async_create_entry.assert_called_once()
    else:
        assert result == "form"
        assert flow.async_show_form.call_args.kwargs["errors"] == {"base": "no_contracts"}


async def test_production_only_update_imports_without_consumption_metadata(
    monkeypatch, register_energy_entities
):
    import_date = date(2026, 10, 15)
    metadata = SimpleNamespace(_last_import=import_date, _contract_start=import_date)
    coordinator = SimpleNamespace(
        hass=Mock(),
        api_client=Mock(),
        installations={},
        production_installations={"solar": {"contract_start": "2026-01-01"}},
        meta_entities={},
        production_meta_entities={"solar": metadata},
        last_production_sums={},
        update_interval=timedelta(minutes=20),
    )
    register_energy_entities(coordinator)
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
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
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda hass: recorder
        )
    import_statistics = Mock()
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics", import_statistics
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics",
        import_statistics,
    )
    fetch = AsyncMock(
        return_value={
            "fetched_data": True,
            "statistics": [{"start": datetime(2026, 10, 15, tzinfo=UTC), "state": 1.0, "sum": 1.0}],
        }
    )
    monkeypatch.setattr(ProductionImporter, "import_statistics", fetch)

    await EkzCoordinator._do_update_data(coordinator)

    fetch.assert_awaited_once()
    import_statistics.assert_called_once()
    assert coordinator.last_production_sums == {"solar": 1.0}


@pytest.mark.parametrize("consumption_available", [False, True])
@pytest.mark.parametrize("healthy_production", [False, True])
@pytest.mark.parametrize(
    "failure", ["api", "replay", "auth", "metadata", "prepare_barrier", "prepare_auth"]
)
async def test_production_failures_do_not_abort_other_installations(
    hass,
    monkeypatch,
    freezer,
    caplog,
    consumption_available,
    healthy_production,
    failure,
    register_energy_entities,
):
    from homeassistant.exceptions import ConfigEntryAuthFailed

    from custom_components.ekz_ha.api.session import EkzAuthenticationError
    from custom_components.ekz_ha.import_status import get_import_status
    from custom_components.ekz_ha.sensor import EkzImportStatusEntity, EkzMetaEntity

    freezer.move_to("2026-10-28 12:00:00+00:00")
    coordinator = EkzCoordinator(hass, Mock(), timedelta(hours=4), Mock(entry_id="isolation"))
    coordinator.installations = (
        {"consumption": {"contract_start": "2026-10-01"}} if consumption_available else {}
    )
    coordinator.production_installations = {
        "failed": {"contract_start": "2026-10-01"},
        **({"healthy": {"contract_start": "2026-10-01"}} if healthy_production else {}),
    }
    coordinator.meta_entities = {
        key: EkzMetaEntity(coordinator, key) for key in coordinator.installations
    }
    coordinator.production_meta_entities = {
        key: EkzMetaEntity(coordinator, key, model="Solar Meter")
        for key in coordinator.production_installations
    }
    register_energy_entities(coordinator)
    for meta in [
        *coordinator.meta_entities.values(),
        *coordinator.production_meta_entities.values(),
    ]:
        meta.set_contract_start(date(2026, 10, 1))
        meta.set_last_import(date(2026, 10, 25))
        meta._checkpoint_restored = True
        meta._checkpoint_needs_replay = False
    failed_meta = coordinator.production_meta_entities["failed"]
    failed_meta._empty_search_from = date(2026, 10, 25)
    failed_meta._empty_search_days = 30
    failed_meta._pending_sum_offset = 100.0
    failed_id = "ekz_ha:electricity_production_failed"
    coordinator.last_production_sums = {"failed": 100.0}
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
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics", Mock()
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics", Mock()
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda *_args: None,
    )
    result = {
        "fetched_data": False,
        "statistics": [],
        "to_date": date(2026, 10, 29),
    }
    consumption_import = AsyncMock(return_value=result)
    monkeypatch.setattr(ConsumptionImporter, "import_statistics", consumption_import)
    attempted = []

    async def production_import(_importer, _hass, key, _start, meta, **_kwargs):
        attempted.append(key)
        get_import_status(meta).begin(
            datetime(2026, 10, 25, tzinfo=ZRH), datetime(2026, 10, 29, tzinfo=ZRH), False
        )
        if key == "failed":
            # Simulate progress before failure to verify the existing rollback remains active.
            meta._empty_search_from = date(2026, 10, 29)
            meta._pending_sum_offset = 999.0
            if failure == "auth":
                raise EkzAuthenticationError("rejected")
            raise TimeoutError("production unavailable")
        return result

    monkeypatch.setattr(ProductionImporter, "import_statistics", production_import)
    if failure in ("metadata", "prepare_barrier", "prepare_auth"):
        preparation_failed = False

        from custom_components.ekz_ha.statistics.migration import _async_prepare_energy_statistic

        original_prepare = _async_prepare_energy_statistic

        async def prepare(coordinator, key, kind):
            nonlocal preparation_failed
            if key == "failed" and not preparation_failed:
                preparation_failed = True
                if failure == "prepare_auth":
                    raise EkzAuthenticationError("rejected during preparation")
                if failure == "prepare_barrier":
                    raise OSError("recorder unavailable")
                raise OSError("metadata unavailable")
            return await original_prepare(coordinator, key, kind)

        monkeypatch.setattr(
            "custom_components.ekz_ha.coordinator._async_prepare_energy_statistic", prepare
        )
    if failure == "replay":
        failed_meta._checkpoint_needs_replay = True
        failed_meta._recent_statistics = [
            {"start": datetime(2026, 10, 25, tzinfo=UTC), "state": 1.0, "sum": 100.0}
        ]
        failed_meta._checkpoint_restored = True
        recorder.async_block_till_done.side_effect = (
            [None, OSError("recorder unavailable"), None]
            if consumption_available
            else [OSError("recorder unavailable"), None]
        )

    if failure in ("auth", "prepare_auth"):
        with pytest.raises(ConfigEntryAuthFailed):
            await coordinator._do_update_data()
    elif not consumption_available and not healthy_production:
        with pytest.raises(UpdateFailed):
            await coordinator._do_update_data()
    else:
        await coordinator._do_update_data()
        assert coordinator.next_update_time is not None
        if healthy_production:
            healthy_meta = coordinator.production_meta_entities["healthy"]
            assert healthy_meta._import_status.state == "current"
            assert (
                EkzImportStatusEntity(coordinator, healthy_meta, "production").native_value
                == "current"
            )

    assert consumption_import.await_count == int(consumption_available)
    if failure == "prepare_auth":
        assert preparation_failed
        assert not attempted
        return
    assert failed_meta._import_status.state == "failed"
    assert failed_meta._import_status.last_result == "failed"
    assert (
        failed_meta._import_status.error_type
        == {
            "api": "TimeoutError",
            "replay": "OSError",
            "auth": "EkzAuthenticationError",
            "metadata": "OSError",
            "prepare_barrier": "OSError",
            "prepare_auth": "EkzAuthenticationError",
        }[failure]
    )
    if failure in ("api", "replay", "auth"):
        assert coordinator._import_checkpoints[failed_id]["import_status"]["state"] == "failed"
    else:
        assert failed_id not in coordinator._import_checkpoints
    assert failed_meta._empty_search_from == date(2026, 10, 25)
    assert failed_meta._pending_sum_offset == 100.0
    assert coordinator.last_production_sums["failed"] == 100.0
    assert ("healthy" in attempted) == (
        healthy_production and failure not in ("auth", "prepare_auth")
    )
    if failure == "replay":
        assert failed_meta._checkpoint_needs_replay
        assert "failed" not in attempted
    if failure not in ("auth", "prepare_auth"):
        assert "failed for failed" in caplog.text
        recorder.async_block_till_done.side_effect = None

        async def recovered_import(_importer, _hass, _key, _start, meta, **_kwargs):
            meta._empty_search_days = None
            meta._empty_search_from = None
            get_import_status(meta).begin(
                datetime(2026, 10, 25, tzinfo=ZRH), datetime(2026, 10, 29, tzinfo=ZRH), False
            )
            return result

        monkeypatch.setattr(ProductionImporter, "import_statistics", recovered_import)
        await coordinator._do_update_data()
        assert failed_meta._import_status.state == "current"
        assert failed_meta._import_status.error_type is None
        assert not failed_meta._checkpoint_needs_replay
        recovered_id = failed_id
        assert coordinator._import_checkpoints[recovered_id]["import_status"]["state"] == "current"


@pytest.mark.parametrize("mean_type", [None, 0])
def test_statistic_metadata_matches_home_assistant_version(monkeypatch, mean_type):
    monkeypatch.setattr("custom_components.ekz_ha.statistics.recorder.MEAN_TYPE_NONE", mean_type)
    metadata = _make_stat_meta("ekz_ha:electricity_consumption_test")
    if mean_type is None:
        assert metadata["has_mean"] is False
        assert "mean_type" not in metadata
    else:
        assert metadata["mean_type"] == mean_type
        assert "has_mean" not in metadata


@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
@pytest.mark.parametrize("have_averages", [False, True])
async def test_daily_import_clears_whole_day_before_predicting(
    monkeypatch, freezer, local_day, have_averages, register_energy_entities
):
    start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH).astimezone(UTC)
    end = datetime.combine(
        local_day + timedelta(days=1), datetime.min.time(), tzinfo=ZRH
    ).astimezone(UTC)
    freezer.move_to(end + timedelta(hours=3))
    service = PredictionService()
    if have_averages:
        service.merge_raw_accumulator({end.month * 100 + hour: (1.0, 4) for hour in range(24)})
        service.calculate_averages()
    key = "825821"
    prediction_id = f"ekz_ha:electricity_consumption_{key}_prediction"
    metadata = SimpleNamespace(_last_import=local_day, _contract_start=local_day)
    coordinator = SimpleNamespace(
        hass=Mock(),
        api_client=Mock(
            get_production_installations=AsyncMock(return_value=InstallationSelectionData()),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
        installations={key: {"contract_start": local_day.isoformat()}},
        production_installations={},
        meta_entities={key: metadata},
        production_meta_entities={},
        last_sums={},
        last_production_sums={},
        catching_up={},
        prediction_services={key: service},
        migrated_prediction_statistics=set(),
        update_interval=timedelta(minutes=20),
    )
    register_energy_entities(coordinator)
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
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
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda hass: recorder
        )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda hass, key: prediction_id,
    )
    result = {
        "fetched_data": True,
        "statistics": [{"start": start, "end": end, "state": 24.0, "sum": 24.0}],
        "to_date": local_day + timedelta(days=1),
    }
    monkeypatch.setattr(ConsumptionImporter, "import_statistics", AsyncMock(return_value=result))
    imported = {}
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics",
        lambda hass, metadata, rows: imported.update({metadata["statistic_id"]: rows}),
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics",
        lambda hass, metadata, rows: imported.update({metadata["statistic_id"]: rows}),
    )

    await EkzCoordinator._do_update_data(coordinator)

    predictions = imported[prediction_id]
    cleared = [row for row in predictions if row["start"] < end]
    assert len(cleared) == int((end - start).total_seconds() / 3600)
    assert all(row["sum"] == 0 and row["state"] == 0 for row in cleared)
    future = [row for row in predictions if row["start"] >= end]
    assert len(future) == (3 if have_averages else 0)
    if have_averages:
        assert future[0]["start"] == end
        assert future[-1]["sum"] == 3.0


@pytest.mark.parametrize(
    ("delivered_slots", "expected_partial"),
    [(1, 1.5), (2, 1.0), (3, 0.5), (4, 0.0)],
)
async def test_partial_last_hour_is_completed_by_prediction(
    monkeypatch, freezer, delivered_slots, expected_partial, register_energy_entities
):
    now = datetime(2026, 10, 26, 13, tzinfo=UTC)
    freezer.move_to(now)
    hour_start = datetime(2026, 10, 26, 10, tzinfo=UTC)
    service = PredictionService()
    service.merge_raw_accumulator({1010: (2.0, 4), 1011: (1.0, 4), 1012: (3.0, 4)})
    service.calculate_averages()
    key = "825821"
    prediction_id = f"ekz_ha:electricity_consumption_{key}_prediction"
    local_day = date(2026, 10, 26)
    metadata = SimpleNamespace(_last_import=local_day, _contract_start=local_day)
    coordinator = SimpleNamespace(
        hass=Mock(),
        api_client=Mock(
            get_production_installations=AsyncMock(return_value=InstallationSelectionData()),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
        installations={key: {"contract_start": local_day.isoformat()}},
        production_installations={},
        meta_entities={key: metadata},
        production_meta_entities={},
        last_sums={},
        last_production_sums={},
        catching_up={},
        prediction_services={key: service},
        migrated_prediction_statistics=set(),
        update_interval=timedelta(minutes=20),
    )
    register_energy_entities(coordinator)
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
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
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda hass: recorder
        )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda hass, key: prediction_id,
    )
    result = {
        "fetched_data": True,
        "statistics": [
            {
                "start": hour_start,
                "end": hour_start + timedelta(hours=1),
                "state": 1.0,
                "sum": 1.0,
                "slots": {
                    f"2026102610{index * 15:02d}00": 0.25 for index in range(delivered_slots)
                },
                "source_resolution": "hour",
            }
        ],
        "to_date": local_day,
    }
    monkeypatch.setattr(ConsumptionImporter, "import_statistics", AsyncMock(return_value=result))
    imported = {}
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics",
        lambda hass, metadata, rows: imported.update({metadata["statistic_id"]: rows}),
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics",
        lambda hass, metadata, rows: imported.update({metadata["statistic_id"]: rows}),
    )

    await EkzCoordinator._do_update_data(coordinator)

    predictions = imported[prediction_id]
    assert [row["start"] for row in predictions] == sorted(row["start"] for row in predictions)
    assert len({row["start"] for row in predictions}) == len(predictions)
    partial = next(row for row in predictions if row["start"] == hour_start)
    assert partial["state"] == pytest.approx(expected_partial)
    assert partial["sum"] == pytest.approx(expected_partial)
    forward = [row for row in predictions if row["start"] > hour_start]
    assert [row["sum"] for row in forward] == pytest.approx(
        [expected_partial + 1.0, expected_partial + 4.0]
    )


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
@pytest.mark.parametrize(
    "failure", ["recorder", "recorder_barrier", "source_store", "checkpoint_store"]
)
async def test_failed_write_does_not_advance_import_state(
    monkeypatch, entity_type, failure, register_energy_entities
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    key = "825821"
    first_day = date(2026, 8, 1)
    statistic_id = f"ekz_ha:electricity_{entity_type}_{key}"
    meta = EkzMetaEntity(Mock(), key)
    meta.set_contract_start(first_day)
    meta.set_last_import(first_day)
    info = {key: {"contract_start": first_day.isoformat()}}
    coordinator = SimpleNamespace(
        hass=Mock(),
        api_client=Mock(
            get_production_installations=AsyncMock(return_value=InstallationSelectionData()),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
        installations=info if entity_type == "consumption" else {},
        production_installations=info if entity_type == "production" else {},
        meta_entities={key: meta} if entity_type == "consumption" else {},
        production_meta_entities={key: meta} if entity_type == "production" else {},
        last_sums={key: 100.0},
        last_production_sums={key: 100.0},
        catching_up={},
        prediction_services={},
        migrated_prediction_statistics=set(),
        _sum_offsets={},
        _daily_statistics={},
        _import_checkpoints={},
        _source_resolution_store=Mock(
            async_save=AsyncMock(
                side_effect=OSError("source failure") if failure == "source_store" else None
            )
        ),
        _import_checkpoint_store=Mock(
            async_save=AsyncMock(
                side_effect=OSError("checkpoint failure") if failure == "checkpoint_store" else None
            )
        ),
        update_interval=timedelta(minutes=20),
    )
    register_energy_entities(coordinator)
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={}),
        async_block_till_done=AsyncMock(
            side_effect=OSError("recorder barrier failure")
            if failure == "recorder_barrier"
            else None
        ),
    )
    for module in (
        "coordinator",
        "statistics.checkpoint",
        "statistics.prediction_manager",
        "statistics.recorder",
    ):
        monkeypatch.setattr(
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda hass: recorder
        )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics",
        Mock(side_effect=ValueError("recorder failure") if failure == "recorder" else None),
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics",
        Mock(side_effect=ValueError("recorder failure") if failure == "recorder" else None),
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda hass, key: None,
    )

    imported = Mock()

    async def import_chunk(
        self, hass, installation_id, contract_start, metadata, running_sum_offset, *, statistic_id
    ):
        imported(statistic_id)
        metadata.set_last_import(date(2026, 8, 20))
        metadata.set_history_cursor(date(2026, 8, 30))
        metadata.set_pending(date(2026, 8, 15), 105.0)
        return {
            "fetched_data": True,
            "statistics": [
                {
                    "start": datetime(2026, 8, 20, tzinfo=UTC),
                    "state": 1.0,
                    "sum": 101.0,
                    "source_resolution": "day",
                }
            ],
            "to_date": date(2026, 8, 31),
        }

    importer_type = ConsumptionImporter if entity_type == "consumption" else ProductionImporter
    monkeypatch.setattr(importer_type, "import_statistics", import_chunk)

    with pytest.raises(UpdateFailed) as raised:
        await EkzCoordinator._do_update_data(coordinator)

    imported.assert_called_once_with(statistic_id)
    assert isinstance(raised.value.__cause__, ValueError if failure == "recorder" else OSError)
    assert (
        str(raised.value.__cause__)
        == {
            "recorder": "recorder failure",
            "recorder_barrier": "recorder barrier failure",
            "source_store": "source failure",
            "checkpoint_store": "checkpoint failure",
        }[failure]
    )
    assert meta._last_import == first_day
    assert meta._history_cursor is None
    assert meta._pending_from is None
    assert meta._recent_statistics == []
    assert coordinator._sum_offsets == {}
    assert coordinator._daily_statistics == {}
    if failure == "checkpoint_store":
        assert statistic_id not in coordinator._import_checkpoints
    else:
        checkpoint = coordinator._import_checkpoints[statistic_id]
        assert checkpoint["import_status"]["state"] == "failed"
        assert not checkpoint.get("statistics")
        assert not checkpoint.get("history_cursor")
        assert not checkpoint.get("pending_from")
    assert coordinator.last_sums[key] == coordinator.last_production_sums[key] == 100.0


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
async def test_restart_replays_unflushed_historical_checkpoint(
    monkeypatch, hass, freezer, entity_type, register_energy_entities
):
    from homeassistant.helpers.storage import Store

    from custom_components.ekz_ha.sensor import EkzMetaEntity
    from custom_components.ekz_ha.statistics.checkpoint import _persist_import_checkpoint

    freezer.move_to("2026-10-28 12:00:00+00:00")
    key = "825821"
    statistic_id = f"ekz_ha:electricity_{entity_type}_{key}"
    first_day = date(2026, 8, 1)
    old_meta = EkzMetaEntity(Mock(), key)
    old_meta.set_history_cursor(date(2026, 8, 30))
    start = datetime(2026, 8, 1, tzinfo=ZRH).astimezone(UTC)
    rows = [
        {"start": start, "state": 2.0, "sum": 102.0, "source_resolution": "hour"},
        {
            "start": start + timedelta(days=1),
            "state": 3.0,
            "sum": 105.0,
            "source_resolution": "hour",
        },
    ]
    storage_key = f"ekz_ha.test.unflushed.{entity_type}"
    old = SimpleNamespace(_import_checkpoint_store=Store(hass, 1, storage_key))
    await _persist_import_checkpoint(old, statistic_id, old_meta, rows)

    # Only an older chunk has reached the recorder before the process restarts.
    persisted = [{"start": start - timedelta(days=1), "state": 100.0, "sum": 100.0}]
    pending = []

    def enqueue(_hass, metadata, statistics):
        if metadata["statistic_id"] == statistic_id:
            pending.extend(statistics)

    async def flush():
        by_start = {row["start"]: row for row in persisted + pending}
        persisted[:] = sorted(by_start.values(), key=lambda row: row["start"])
        pending.clear()

    async def query(function, _hass, *args):
        if function.__name__ == "get_last_statistics":
            return {statistic_id: persisted[-1:]} if args[1] == statistic_id else {}
        start, end = args[:2]
        return {statistic_id: [row for row in persisted if start <= row["start"] < end]}

    recorder = Mock(
        async_add_executor_job=AsyncMock(side_effect=query),
        async_block_till_done=AsyncMock(side_effect=flush),
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
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics", enqueue
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics", enqueue
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._prediction_statistic_id",
        lambda *_args: None,
    )
    meta = EkzMetaEntity(Mock(), key)
    meta.set_contract_start(first_day)
    info = {key: {"contract_start": first_day.isoformat()}}
    coordinator = SimpleNamespace(
        hass=hass,
        api_client=Mock(
            get_production_installations=AsyncMock(return_value=InstallationSelectionData()),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
        installations=info if entity_type == "consumption" else {},
        production_installations=info if entity_type == "production" else {},
        meta_entities={key: meta} if entity_type == "consumption" else {},
        production_meta_entities={key: meta} if entity_type == "production" else {},
        last_sums={},
        last_production_sums={},
        catching_up={},
        prediction_services={},
        migrated_prediction_statistics=set(),
        _daily_statistics={},
        _import_checkpoints=await Store(hass, 1, storage_key).async_load(),
        update_interval=timedelta(minutes=20),
    )
    register_energy_entities(coordinator)
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    starts = []

    async def import_next(
        self, _hass, installation_id, contract_start, metadata, running_sum_offset, *, statistic_id
    ):
        from custom_components.ekz_ha.statistics.importer import get_import_start

        starts.append(get_import_start(metadata))
        assert running_sum_offset == 105.0
        metadata.set_history_cursor(date(2026, 9, 28))
        return {
            "fetched_data": True,
            "statistics": [
                {
                    "start": datetime(2026, 9, 1, tzinfo=UTC),
                    "state": 4.0,
                    "sum": running_sum_offset + 4.0,
                }
            ],
            "to_date": date(2026, 9, 29),
        }

    importer_type = ConsumptionImporter if entity_type == "consumption" else ProductionImporter
    monkeypatch.setattr(importer_type, "import_statistics", import_next)
    await EkzCoordinator._do_update_data(coordinator)
    await flush()

    assert starts == [date(2026, 8, 30)]
    assert [row["sum"] for row in persisted] == [100.0, 102.0, 105.0, 109.0]


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
@pytest.mark.parametrize("already_flushed", [False, True])
async def test_checkpoint_replay_is_idempotent_in_real_recorder(
    recorder_mock, hass, entity_type, already_flushed
):
    from homeassistant.components.recorder.statistics import (
        async_add_external_statistics as enqueue,
    )
    from homeassistant.components.recorder.statistics import statistics_during_period

    from custom_components.ekz_ha.statistics.checkpoint import _async_replay_import_checkpoint

    statistic_id = f"ekz_ha:electricity_{entity_type}_825821"
    start = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0) - timedelta(days=5)
    rows = [
        {"start": start + timedelta(hours=1), "state": 2.0, "sum": 102.0},
        {"start": start + timedelta(hours=2), "state": 3.0, "sum": 105.0},
    ]
    existing = [{"start": start, "state": 100.0, "sum": 100.0}]
    if already_flushed:
        existing.extend(rows)
    existing.append({"start": start + timedelta(hours=3), "state": 4.0, "sum": 109.0})
    enqueue(hass, _make_stat_meta(statistic_id), existing)
    await recorder_mock.async_block_till_done()
    coordinator = SimpleNamespace(hass=hass)
    meta = SimpleNamespace(_checkpoint_needs_replay=True, _recent_statistics=rows)

    for _ in range(2):
        meta._checkpoint_needs_replay = True
        await _async_replay_import_checkpoint(coordinator, statistic_id, meta)
        assert not meta._checkpoint_needs_replay
        result = await recorder_mock.async_add_executor_job(
            statistics_during_period,
            hass,
            start,
            start + timedelta(hours=4),
            {statistic_id},
            "hour",
            None,
            {"state", "sum"},
        )
        assert [row["sum"] for row in result[statistic_id]] == [100.0, 102.0, 105.0, 109.0]
        assert [row["state"] for row in result[statistic_id]] == [100.0, 2.0, 3.0, 4.0]


async def test_checkpoint_replay_failure_remains_retryable(monkeypatch):
    from custom_components.ekz_ha.statistics.checkpoint import _async_replay_import_checkpoint

    meta = SimpleNamespace(
        _checkpoint_needs_replay=True,
        _recent_statistics=[{"start": datetime(2026, 8, 1, tzinfo=UTC), "state": 2.0, "sum": 2.0}],
    )
    recorder = Mock(async_block_till_done=AsyncMock(side_effect=OSError("recorder unavailable")))
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
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics", enqueue
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics", enqueue
    )
    coordinator = SimpleNamespace(hass=Mock())
    with pytest.raises(UpdateFailed, match="Checkpoint replay failed"):
        await _async_replay_import_checkpoint(coordinator, "sensor.test_consumption", meta)
    assert meta._checkpoint_needs_replay
    recorder.async_block_till_done.side_effect = None
    await _async_replay_import_checkpoint(coordinator, "sensor.test_consumption", meta)
    assert not meta._checkpoint_needs_replay
    assert enqueue.call_count == 2


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
async def test_previous_chunk_is_committed_before_checkpoint_replacement(monkeypatch, entity_type):
    from custom_components.ekz_ha.sensor import EkzMetaEntity
    from custom_components.ekz_ha.statistics.checkpoint import _async_import_chunk

    start = datetime(2026, 8, 1, tzinfo=UTC)
    persisted = []
    queued = []
    saved = []

    async def flush():
        persisted.extend(queued)
        queued.clear()

    async def save(checkpoints):
        if saved:
            assert persisted == saved[-1]["statistics"]
        saved.append(checkpoints[statistic_id])

    def enqueue(_hass, metadata, rows):
        queued.extend(
            {
                **row,
                "start": row["start"].isoformat(),
                "end": (row["start"] + timedelta(hours=1)).isoformat(),
            }
            for row in rows
        )

    recorder = Mock(async_block_till_done=AsyncMock(side_effect=flush))
    for module in (
        "coordinator",
        "statistics.checkpoint",
        "statistics.prediction_manager",
        "statistics.recorder",
    ):
        monkeypatch.setattr(
            f"custom_components.ekz_ha.{module}.get_recorder_instance", lambda _hass: recorder
        )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.checkpoint._async_write_statistics", enqueue
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.prediction_manager._async_write_statistics", enqueue
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(start.date())
    coordinator = SimpleNamespace(
        hass=Mock(), _import_checkpoint_store=Mock(async_save=AsyncMock(side_effect=save))
    )
    importer_type = ConsumptionImporter if entity_type == "consumption" else ProductionImporter
    importer = importer_type(Mock())
    statistic_id = f"ekz_ha:electricity_{entity_type}_825821"
    for index in range(2):
        row = {"start": start + timedelta(days=index), "state": 1.0, "sum": float(index + 1)}
        monkeypatch.setattr(
            importer,
            "import_statistics",
            AsyncMock(return_value={"statistics": [row], "fetched_data": True}),
        )
        await _async_import_chunk(coordinator, importer, "825821", start, meta, statistic_id, 0.0)
    assert len(persisted) == 1
    assert len(queued) == 1
    assert len(saved) == 2
