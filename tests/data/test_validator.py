"""Tests for data validation."""

import zoneinfo
from datetime import UTC, datetime, timedelta

import pytest

from custom_components.ekz_ha.api.models import ApiValue
from custom_components.ekz_ha.data.validator import (
    DataValidator,
    day_bounds,
    day_slots,
    is_dst,
)

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")


def complete_day_values(date_str):
    local_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ZRH)
    start = local_start.astimezone(UTC)
    end = (local_start + timedelta(days=1)).astimezone(UTC)
    return [
        ApiValue(
            (start + timedelta(minutes=15 * index)).strftime("%Y%m%d%H%M%S"),
            0.25,
            "VALID",
            date_str,
        )
        for index in range(int((end - start).total_seconds() / 900))
    ]


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
        values = complete_day_values("2026-08-13")

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
        values = complete_day_values("2026-08-13") + [
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

    @pytest.mark.parametrize("invalid", ["duplicate", "shifted", "outside"])
    def test_complete_day_requires_exact_timestamps(self, invalid):
        values = complete_day_values("2026-08-13")
        if invalid == "duplicate":
            values[-1] = values[0]
        elif invalid == "shifted":
            values[-1].timestamp = "20260813214600"
        else:
            values[-1].timestamp = "20260813220000"

        assert DataValidator.find_complete_days(values) == []


@pytest.mark.parametrize(
    ("day", "count", "start", "end"),
    [
        ("2026-01-15", 96, "2026-01-14T23:00:00+00:00", "2026-01-15T23:00:00+00:00"),
        ("2026-03-29", 92, "2026-03-28T23:00:00+00:00", "2026-03-29T22:00:00+00:00"),
        ("2026-07-15", 96, "2026-07-14T22:00:00+00:00", "2026-07-15T22:00:00+00:00"),
        ("2026-10-25", 100, "2026-10-24T22:00:00+00:00", "2026-10-25T23:00:00+00:00"),
    ],
)
def test_day_grid_has_expected_utc_boundaries(freezer, day, count, start, end):
    freezer.move_to("2026-11-01 12:00:00+00:00")
    assert day_bounds(day) == (datetime.fromisoformat(start), datetime.fromisoformat(end))
    slots = day_slots(day)
    assert len(slots) == len(set(slots)) == count
    assert len(day_slots(day, minutes=60)) == count // 4
    assert slots[0].isoformat() == start
    assert (slots[-1] + timedelta(minutes=15)).isoformat() == end
    assert DataValidator.expected_slots_for_date(day, ZRH) == count
    assert DataValidator.find_complete_days(complete_day_values(day)) == [day]
