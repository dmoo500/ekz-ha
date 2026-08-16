"""Tests for API models."""

from custom_components.ekz_ha.api.models import (
    ApiValue,
    ConsumptionData,
    SeriesData,
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

    def test_from_dict_iso_format(self):
        """Test creating ApiValue from ISO timestamp."""
        data = {
            "timestamp": "2026-08-13 12:00:00",
            "value": 1.5,
            "status": "VALID",
        }

        value = ApiValue.from_dict(data)

        assert value.date == "2026-08-13"
        assert value.tariff == "TOTAL"


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


class TestConsumptionData:
    """Tests for ConsumptionData dataclass."""

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
