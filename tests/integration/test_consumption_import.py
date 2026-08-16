"""Integration tests for consumption data import using real JSON fixtures."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from custom_components.ekz_ha.api.models import ConsumptionData
from custom_components.ekz_ha.data import DataAggregator, StatisticsTransformer


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
