"""Tests for data validation."""

import zoneinfo
from datetime import datetime

from custom_components.ekz_ha.api.models import ApiValue
from custom_components.ekz_ha.data.validator import (
    DataValidator,
    is_dst,
)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")


class TestDSTFunctions:
    """Tests for DST detection functions."""

    def test_is_dst_summer(self):
        """Test DST detection in summer."""
        summer_date = datetime(2026, 8, 1)  # August is DST in Europe
        assert is_dst(summer_date, ZRH) is True

    def test_is_dst_winter(self):
        """Test DST detection in winter."""
        winter_date = datetime(2026, 1, 1)  # January is not DST
        assert is_dst(winter_date, ZRH) is False

    def test_is_dst_switchover_spring(self):
        """Test DST switchover detection in spring."""
        # Note: Actual DST switchover dates vary by year
        # This test is a placeholder for future DST switchover detection
        # DST transition dates: last Sunday in March (spring forward)
        pass


class TestDataValidator:
    """Tests for DataValidator."""

    def test_count_slots_per_day_single_day(self):
        """Test counting slots for a single day."""
        values = [
            ApiValue(
                timestamp=f"202608131{str(i).zfill(2)}00",
                value=0.5,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            )
            for i in range(24)  # 24 hourly values
        ]

        validator = DataValidator()
        counts = validator.count_slots_per_day(values)

        assert counts["2026-08-13"] == 24

    def test_count_slots_per_day_multiple_days(self):
        """Test counting slots for multiple days."""
        values = [
            ApiValue(
                timestamp="20260813120000",
                value=0.5,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
            ApiValue(
                timestamp="20260813130000",
                value=0.5,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            ),
            ApiValue(
                timestamp="20260814120000",
                value=0.5,
                status="VALID",
                date="2026-08-14",
                tariff="TOTAL",
            ),
        ]

        validator = DataValidator()
        counts = validator.count_slots_per_day(values)

        assert counts["2026-08-13"] == 2
        assert counts["2026-08-14"] == 1

    def test_expected_slots_for_date_regular_day(self):
        """Test expected slots for regular day."""
        validator = DataValidator()
        expected = validator.expected_slots_for_date("2026-08-13", ZRH)

        assert expected == 96  # 24h * 4 slots/h

    def test_find_complete_days_full_day(self):
        """Test finding complete days with 96 slots."""
        # Create 96 15-minute slots for 2026-08-13
        values = [
            ApiValue(
                timestamp=f"20260813{str(h).zfill(2)}{str(m).zfill(2)}00",
                value=0.25,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            )
            for h in range(24)
            for m in [0, 15, 30, 45]
        ]

        validator = DataValidator()
        complete = validator.find_complete_days(values, ZRH)

        assert "2026-08-13" in complete

    def test_find_complete_days_partial_day(self):
        """Test that partial days are not considered complete."""
        # Create only 50 slots for 2026-08-13
        values = [
            ApiValue(
                timestamp=f"20260813{str(i).zfill(4)}00",
                value=0.25,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            )
            for i in range(50)
        ]

        validator = DataValidator()
        complete = validator.find_complete_days(values, ZRH)

        assert "2026-08-13" not in complete

    def test_get_latest_complete_date(self):
        """Test getting the latest complete date."""
        # Create complete data for 2026-08-13 and partial for 2026-08-14
        values = [
            ApiValue(
                timestamp=f"20260813{str(h).zfill(2)}{str(m).zfill(2)}00",
                value=0.25,
                status="VALID",
                date="2026-08-13",
                tariff="TOTAL",
            )
            for h in range(24)
            for m in [0, 15, 30, 45]
        ] + [
            ApiValue(
                timestamp="20260814120000",
                value=0.25,
                status="VALID",
                date="2026-08-14",
                tariff="TOTAL",
            ),
        ]

        validator = DataValidator()
        latest = validator.get_latest_complete_date(values, ZRH)

        assert latest is not None
        assert latest.strftime("%Y-%m-%d") == "2026-08-13"
