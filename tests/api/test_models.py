"""Tests for API models."""

import pytest

from custom_components.ekz_ha.api.models import (
    ApiValue,
    ConsumptionData,
    SeriesData,
    ValueOrigin,
)


class TestApiValue:
    """Tests for ApiValue dataclass."""

    def test_from_dict_standard_format(self):
        """Test creating ApiValue from standard API response."""
        data = {
            "timestamp": "20260813120000",
            "value": 2.5,
            "status": "VALID",
        }

        value = ApiValue.from_dict(data, tariff="HT")

        assert value.timestamp == "20260813120000"
        assert value.value == 2.5
        assert value.status == "VALID"
        assert value.date == "2026-08-13"
        assert value.tariff == "HT"
        assert value.origin is ValueOrigin.API

    def test_from_dict_iso_format(self):
        """Test creating ApiValue from ISO timestamp."""
        data = {
            "timestamp": "2026-08-13 12:00:00",
            "value": 1.5,
            "status": "VALID",
        }

        value = ApiValue.from_dict(data)

        assert value.timestamp == "20260813120000"
        assert value.date == "2026-08-13"
        assert value.tariff == "TOTAL"

    @pytest.mark.parametrize(
        "timestamp",
        ["2026-03-15T23:00:00Z", "2026-03-16T00:00:00+01:00", "20260315230000"],
    )
    def test_timestamp_is_normalized_to_utc_while_preserving_api_date(self, timestamp):
        value = ApiValue.from_dict({"timestamp": timestamp, "value": 6.0, "date": "2026-03-16"})

        assert value.timestamp == "20260315230000"
        assert value.date == "2026-03-16"


class TestSeriesData:
    """Tests for SeriesData dataclass."""

    def test_from_dict_filters_invalid_values(self):
        """Test that NOT_AVAILABLE and MISSING values are filtered."""
        data = {
            "values": [
                {"timestamp": "20260813120000", "value": 2.5, "status": "VALID"},
                {"timestamp": "20260813130000", "value": 0.0, "status": "NOT_AVAILABLE"},
                {"timestamp": "20260813140000", "value": 3.0, "status": "MISSING"},
                {"timestamp": "20260813150000", "value": 1.5, "status": "VALID"},
            ],
            "level": "QUARTER_HOUR",
        }

        series = SeriesData.from_dict(data, tariff="NT")

        assert len(series.values) == 2
        assert series.values[0].value == 2.5
        assert series.values[1].value == 1.5
        assert series.level == "QUARTER_HOUR"

    def test_inactive_tariff_coverage_is_normalized_without_creating_measurements(self):
        series = SeriesData.from_dict(
            {
                "values": [
                    {
                        "timestamp": "2026-10-27T00:00:00+01:00",
                        "value": 0,
                        "status": "NOT_AVAILABLE",
                    },
                    {"timestamp": "20261027000000", "value": 0, "status": "MISSING"},
                    {"timestamp": "20261027010000", "value": 1, "status": "NOT_AVAILABLE"},
                ]
            }
        )
        assert series.values == []
        assert series.inactive_timestamps == {"20261026230000"}


class TestConsumptionData:
    """Tests for ConsumptionData dataclass."""

    @pytest.mark.parametrize("series_key", ["series", "seriesHt", "seriesNt"])
    def test_import_values_preserve_single_series(self, series_key):
        data = ConsumptionData.from_dict(
            {series_key: {"values": [{"timestamp": "20260715000000", "value": 0.0}]}}
        )
        assert data.get_import_values() == data.get_all_values()
        assert len(data.get_import_values()) == 1

    @pytest.mark.parametrize("level", ["DAY", "QUARTER_HOUR"])
    @pytest.mark.parametrize("missing_status", ["MISSING", "NOT_AVAILABLE"])
    def test_import_values_require_valid_values_from_both_tariffs(self, level, missing_status):
        data = ConsumptionData.from_dict(
            {
                "level": level,
                "seriesHt": {
                    "values": [
                        {"timestamp": "20260715000000", "value": 1.0},
                        {"timestamp": "20260715001500", "value": 2.0},
                    ]
                },
                "seriesNt": {
                    "values": [
                        {"timestamp": "20260715000000", "value": 0.0},
                        {"timestamp": "20260715001500", "value": 3.0, "status": missing_status},
                    ]
                },
            }
        )
        imported = data.get_import_values()
        assert len(imported) == 2
        assert {value.timestamp for value in imported} == {"20260715000000"}
        assert sum(value.value for value in imported) == 1.0
        assert len(data.get_all_values()) == 3

    @pytest.mark.parametrize("level", ["DAY", "QUARTER_HOUR"])
    @pytest.mark.parametrize("active_tariff", ["seriesHt", "seriesNt"])
    @pytest.mark.parametrize(
        ("status", "value", "accepted"),
        [("NOT_AVAILABLE", 0.0, True), ("MISSING", 0.0, False), ("NOT_AVAILABLE", 1.0, False)],
    )
    def test_import_values_distinguish_inactive_and_missing_tariffs(
        self, level, active_tariff, status, value, accepted
    ):
        inactive_tariff = "seriesNt" if active_tariff == "seriesHt" else "seriesHt"
        data = ConsumptionData.from_dict(
            {
                "level": level,
                active_tariff: {"values": [{"timestamp": "20261027060000", "value": 0.04}]},
                inactive_tariff: {
                    "values": [{"timestamp": "20261027060000", "value": value, "status": status}]
                },
            }
        )
        imported = data.get_import_values()
        assert len(imported) == int(accepted)
        if accepted:
            assert imported[0].value == 0.04

    def test_two_inactive_tariffs_do_not_create_zero_measurements(self):
        inactive = {
            "values": [{"timestamp": "20261027060000", "value": 0, "status": "NOT_AVAILABLE"}]
        }
        data = ConsumptionData.from_dict({"seriesHt": inactive, "seriesNt": inactive})
        assert data.get_all_values() == []
        assert data.get_import_values() == []

    def test_import_values_do_not_use_total_to_mask_empty_split_tariff(self):
        data = ConsumptionData.from_dict(
            {
                "seriesHt": {"values": [{"timestamp": "20260715000000", "value": 1.0}]},
                "seriesNt": {"values": []},
                "series": {"values": [{"timestamp": "20260715000000", "value": 99.0}]},
            }
        )
        assert data.get_import_values() == []

    def test_from_dict_with_ht_nt_series(self):
        """Test parsing response with HT and NT series."""
        data = {
            "level": "QUARTER_HOUR",
            "seriesHt": {
                "values": [
                    {"timestamp": "20260813120000", "value": 1.0, "status": "VALID"},
                ],
            },
            "seriesNt": {
                "values": [
                    {"timestamp": "20260813120000", "value": 0.5, "status": "VALID"},
                ],
            },
        }

        consumption = ConsumptionData.from_dict(data)

        assert consumption.level == "QUARTER_HOUR"
        assert consumption.series_ht is not None
        assert consumption.series_nt is not None
        assert len(consumption.series_ht.values) == 1
        assert len(consumption.series_nt.values) == 1

    def test_get_all_values_prefers_ht_nt(self):
        """Test that HT/NT values are preferred over total series."""
        data = {
            "level": "QUARTER_HOUR",
            "seriesHt": {
                "values": [
                    {"timestamp": "20260813120000", "value": 1.0, "status": "VALID"},
                ],
            },
            "series": {
                "values": [
                    {"timestamp": "20260813120000", "value": 99.0, "status": "VALID"},
                ],
            },
        }

        consumption = ConsumptionData.from_dict(data)
        values = consumption.get_all_values()

        # Should use HT value, not series
        assert len(values) == 1
        assert values[0].value == 1.0

    def test_is_empty_with_no_data(self):
        """Test is_empty returns True for empty response."""
        consumption = ConsumptionData.from_dict({})
        assert consumption.is_empty()

    def test_is_empty_with_data(self):
        """Test is_empty returns False with data."""
        data = {
            "series": {
                "values": [
                    {"timestamp": "20260813120000", "value": 1.0, "status": "VALID"},
                ],
            },
        }

        consumption = ConsumptionData.from_dict(data)
        assert not consumption.is_empty()
