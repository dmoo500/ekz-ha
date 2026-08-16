"""Tests for data aggregation."""

import pytest

from custom_components.ekz_ha.api.models import ApiValue
from custom_components.ekz_ha.data.aggregator import DataAggregator, normalize_timestamp


class TestNormalizeTimestamp:
    """Tests for timestamp normalization."""

    def test_normalize_standard_format(self):
        """Test normalizing standard YYYYMMDDHHmmss format."""
        result = normalize_timestamp("20260813120000")
        assert result == "20260813120000"

    def test_normalize_iso_format(self):
        """Test normalizing ISO 8601 format."""
        result = normalize_timestamp("2026-08-13T12:00:00")
        assert result == "20260813120000"

    def test_normalize_space_separated(self):
        """Test normalizing space-separated format."""
        result = normalize_timestamp("2026-08-13 12:00:00")
        assert result == "20260813120000"

    def test_normalize_partial_timestamp(self):
        """Test normalizing partial timestamp."""
        result = normalize_timestamp("20260813")
        assert result == "20260813000000"


class TestDataAggregator:
    """Tests for DataAggregator."""

    def test_merge_tariffs_single_timestamp(self):
        """Test merging HT and NT values for same timestamp."""
        values = [
            ApiValue(timestamp="20260813120000", value=1.0, status="VALID", date="2026-08-13", tariff="HT"),
            ApiValue(timestamp="20260813120000", value=0.5, status="VALID", date="2026-08-13", tariff="NT"),
        ]
        
        aggregator = DataAggregator()
        merged = aggregator.merge_tariffs(values)
        
        assert len(merged) == 1
        assert merged[0].value == 1.5
        assert merged[0].tariff == "TOTAL"

    def test_merge_tariffs_multiple_timestamps(self):
        """Test merging with multiple timestamps."""
        values = [
            ApiValue(timestamp="20260813120000", value=1.0, status="VALID", date="2026-08-13", tariff="HT"),
            ApiValue(timestamp="20260813120000", value=0.5, status="VALID", date="2026-08-13", tariff="NT"),
            ApiValue(timestamp="20260813130000", value=2.0, status="VALID", date="2026-08-13", tariff="HT"),
            ApiValue(timestamp="20260813130000", value=1.0, status="VALID", date="2026-08-13", tariff="NT"),
        ]
        
        aggregator = DataAggregator()
        merged = aggregator.merge_tariffs(values)
        
        assert len(merged) == 2
        assert merged[0].value == 1.5  # 12:00
        assert merged[1].value == 3.0  # 13:00

    def test_aggregate_to_hourly_four_slots(self):
        """Test aggregating 4x 15-minute slots to hourly."""
        values = [
            ApiValue(timestamp="20260813120000", value=0.25, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813121500", value=0.30, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813123000", value=0.20, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813124500", value=0.25, status="VALID", date="2026-08-13", tariff="TOTAL"),
        ]
        
        aggregator = DataAggregator()
        hourly = aggregator.aggregate_to_hourly(values)
        
        assert len(hourly) == 1
        assert hourly[0].value == 1.0
        assert hourly[0].timestamp == "20260813120000"  # Top of hour

    def test_aggregate_to_hourly_multiple_hours(self):
        """Test aggregating multiple hours."""
        values = [
            ApiValue(timestamp="20260813120000", value=0.25, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813121500", value=0.25, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813123000", value=0.25, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813124500", value=0.25, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813130000", value=0.5, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813131500", value=0.5, status="VALID", date="2026-08-13", tariff="TOTAL"),
        ]
        
        aggregator = DataAggregator()
        hourly = aggregator.aggregate_to_hourly(values)
        
        assert len(hourly) == 2
        assert hourly[0].value == 1.0   # 12:00
        assert hourly[1].value == 1.0   # 13:00 (partial hour)

    def test_aggregate_to_daily(self):
        """Test aggregating to daily values."""
        values = [
            ApiValue(timestamp="20260813000000", value=1.0, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813010000", value=1.5, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260813020000", value=2.0, status="VALID", date="2026-08-13", tariff="TOTAL"),
            ApiValue(timestamp="20260814000000", value=3.0, status="VALID", date="2026-08-14", tariff="TOTAL"),
        ]
        
        aggregator = DataAggregator()
        daily = aggregator.aggregate_to_daily(values)
        
        assert len(daily) == 2
        assert daily[0].value == 4.5  # 2026-08-13
        assert daily[0].date == "2026-08-13"
        assert daily[1].value == 3.0  # 2026-08-14
        assert daily[1].date == "2026-08-14"
