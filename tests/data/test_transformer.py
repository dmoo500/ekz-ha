"""Tests for statistics transformation."""

import zoneinfo
from datetime import datetime

from custom_components.ekz_ha.api.models import ApiValue
from custom_components.ekz_ha.data.transformer import StatisticsTransformer

UTC = zoneinfo.ZoneInfo("UTC")


class TestStatisticsTransformer:
    """Tests for StatisticsTransformer."""

    def test_values_to_statistics_single_value(self):
        """Test converting single value to statistics."""
        values = [
            ApiValue(
                timestamp="20260813120000",
                value=1.5,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
        ]

        transformer = StatisticsTransformer()
        stats = transformer.values_to_statistics(values, running_sum_offset=10.0)

        assert len(stats) == 1
        assert stats[0]["state"] == 1.5
        assert stats[0]["sum"] == 11.5  # 10.0 + 1.5
        assert stats[0]["start"].year == 2026
        assert stats[0]["start"].month == 8
        assert stats[0]["start"].day == 13
        assert stats[0]["start"].hour == 12

    def test_values_to_statistics_running_sum(self):
        """Test that running sum accumulates correctly."""
        values = [
            ApiValue(
                timestamp="20260813120000",
                value=1.0,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
            ApiValue(
                timestamp="20260813130000",
                value=2.0,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
            ApiValue(
                timestamp="20260813140000",
                value=1.5,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
        ]

        transformer = StatisticsTransformer()
        stats = transformer.values_to_statistics(values, running_sum_offset=100.0)

        assert len(stats) == 3
        assert stats[0]["sum"] == 101.0  # 100 + 1.0
        assert stats[1]["sum"] == 103.0  # 101 + 2.0
        assert stats[2]["sum"] == 104.5  # 103 + 1.5

    def test_values_to_statistics_with_zero_offset(self):
        """Test conversion with zero offset."""
        values = [
            ApiValue(
                timestamp="20260813120000",
                value=5.0,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
        ]

        transformer = StatisticsTransformer()
        stats = transformer.values_to_statistics(values, running_sum_offset=0.0)

        assert stats[0]["sum"] == 5.0

    def test_values_to_statistics_timestamps_are_utc(self):
        """Test that timestamps are in UTC timezone."""
        values = [
            ApiValue(
                timestamp="20260813120000",
                value=1.0,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
        ]

        transformer = StatisticsTransformer()
        stats = transformer.values_to_statistics(values)

        assert stats[0]["start"].tzinfo == UTC

    def test_get_last_import_date_single_stat(self):
        """Test getting last import date from single statistic."""
        stats = [
            {
                "start": datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC),
                "sum": 100.0,
                "state": 1.0,
            }
        ]

        transformer = StatisticsTransformer()
        last = transformer.get_last_import_date(stats)

        assert last == datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC)

    def test_get_last_import_date_multiple_stats(self):
        """Test getting last import date from multiple statistics."""
        stats = [
            {
                "start": datetime(2026, 8, 13, 10, 0, 0, tzinfo=UTC),
                "sum": 100.0,
                "state": 1.0,
            },
            {
                "start": datetime(2026, 8, 13, 14, 0, 0, tzinfo=UTC),
                "sum": 102.0,
                "state": 2.0,
            },
            {
                "start": datetime(2026, 8, 13, 12, 0, 0, tzinfo=UTC),
                "sum": 101.0,
                "state": 1.0,
            },
        ]

        transformer = StatisticsTransformer()
        last = transformer.get_last_import_date(stats)

        assert last == datetime(2026, 8, 13, 14, 0, 0, tzinfo=UTC)

    def test_get_last_import_date_empty(self):
        """Test getting last import date from empty list."""
        transformer = StatisticsTransformer()
        last = transformer.get_last_import_date([])

        assert last is None
