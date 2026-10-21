"""Integration tests for consumption data import using real JSON fixtures."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.ekz_ha import EkzCoordinator, _make_stat_meta
from custom_components.ekz_ha.api.models import (
    ConsumptionData,
    InstallationContract,
    InstallationSelectionData,
)
from custom_components.ekz_ha.data import DataAggregator, StatisticsTransformer
from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
from custom_components.ekz_ha.statistics.importer import ZRH
from custom_components.ekz_ha.statistics.prediction import PredictionService
from custom_components.ekz_ha.statistics.production import ProductionImporter


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
        api_client=SimpleNamespace(
            get_consumption_installations=AsyncMock(return_value=InstallationSelectionData()),
            get_production_installations=AsyncMock(
                side_effect=[InstallationSelectionData(), production]
            ),
        )
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


async def test_production_only_update_imports_without_consumption_metadata(monkeypatch):
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
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    import_statistics = Mock()
    monkeypatch.setattr("custom_components.ekz_ha.async_import_statistics", import_statistics)
    fetch = AsyncMock(
        return_value={
            "statistics": [{"start": datetime(2026, 10, 15, tzinfo=UTC), "state": 1.0, "sum": 1.0}]
        }
    )
    monkeypatch.setattr(ProductionImporter, "import_statistics", fetch)

    await EkzCoordinator._do_update_data(coordinator)

    fetch.assert_awaited_once()
    import_statistics.assert_called_once()
    assert coordinator.last_production_sums == {"solar": 1.0}


@pytest.mark.parametrize("mean_type", [None, 0])
def test_statistic_metadata_matches_home_assistant_version(monkeypatch, mean_type):
    monkeypatch.setattr("custom_components.ekz_ha.MEAN_TYPE_NONE", mean_type)
    metadata = _make_stat_meta("sensor.electricity_consumption_ekz_test")
    if mean_type is None:
        assert metadata["has_mean"] is False
        assert "mean_type" not in metadata
    else:
        assert metadata["mean_type"] == mean_type
        assert "has_mean" not in metadata


@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
@pytest.mark.parametrize("have_averages", [False, True])
async def test_daily_import_clears_whole_day_before_predicting(
    monkeypatch, freezer, local_day, have_averages
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
    prediction_id = f"sensor.electricity_consumption_ekz_{key}_prediction"
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
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    monkeypatch.setattr(
        "custom_components.ekz_ha._prediction_statistic_id", lambda hass, key: prediction_id
    )
    result = {
        "statistics": [{"start": start, "end": end, "state": 24.0, "sum": 24.0}],
        "to_date": local_day + timedelta(days=1),
    }
    monkeypatch.setattr(ConsumptionImporter, "import_statistics", AsyncMock(return_value=result))
    imported = {}
    monkeypatch.setattr(
        "custom_components.ekz_ha.async_import_statistics",
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


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
@pytest.mark.parametrize("failure", ["recorder", "source_store", "checkpoint_store"])
async def test_failed_write_does_not_advance_import_state(monkeypatch, entity_type, failure):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    key = "825821"
    first_day = date(2026, 8, 1)
    statistic_id = f"sensor.electricity_{entity_type}_ekz_{key}"
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
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    monkeypatch.setattr(
        "custom_components.ekz_ha.async_import_statistics",
        Mock(side_effect=ValueError("recorder failure") if failure == "recorder" else None),
    )
    monkeypatch.setattr("custom_components.ekz_ha._prediction_statistic_id", lambda hass, key: None)

    async def import_chunk(
        self, hass, installation_id, contract_start, metadata, running_sum_offset
    ):
        metadata.set_last_import(date(2026, 8, 20))
        metadata.set_history_cursor(date(2026, 8, 30))
        metadata.set_pending(date(2026, 8, 15), 105.0)
        return {
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

    with pytest.raises(UpdateFailed):
        await EkzCoordinator._do_update_data(coordinator)

    assert meta._last_import == first_day
    assert meta._history_cursor is None
    assert meta._pending_from is None
    assert meta._recent_statistics == []
    assert coordinator._sum_offsets == {}
    assert coordinator._daily_statistics == {}
    assert statistic_id not in coordinator._import_checkpoints
    assert coordinator.last_sums[key] == coordinator.last_production_sums[key] == 100.0
