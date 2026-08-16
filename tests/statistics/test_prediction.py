"""Tests for prediction service."""

from datetime import UTC, datetime

import pytest

from custom_components.ekz_ha.api.models import ApiValue
from custom_components.ekz_ha.statistics.prediction import PredictionService


@pytest.fixture
def prediction_service():
    """Create a prediction service instance."""
    return PredictionService()


@pytest.fixture
def sample_values():
    """Create sample 15-minute values for testing."""
    return [
        ApiValue(
            timestamp=20260301100000, value=0.5, status="VALID", date="2026-03-01", tariff="HT"
        ),  # March, 10:00 UTC
        ApiValue(
            timestamp=20260301101500, value=0.6, status="VALID", date="2026-03-01", tariff="HT"
        ),  # March, 10:15 UTC
        ApiValue(
            timestamp=20260301103000, value=0.7, status="VALID", date="2026-03-01", tariff="HT"
        ),  # March, 10:30 UTC
        ApiValue(
            timestamp=20260301104500, value=0.8, status="VALID", date="2026-03-01", tariff="HT"
        ),  # March, 10:45 UTC
        ApiValue(
            timestamp=20260301110000, value=0.9, status="VALID", date="2026-03-01", tariff="HT"
        ),  # March, 11:00 UTC
        ApiValue(
            timestamp=20260401100000, value=1.0, status="VALID", date="2026-04-01", tariff="HT"
        ),  # April, 10:00 UTC
        ApiValue(
            timestamp=20260401101500, value=1.1, status="VALID", date="2026-04-01", tariff="HT"
        ),  # April, 10:15 UTC
    ]


class TestPredictionService:
    """Test prediction service functionality."""

    def test_initialization(self, prediction_service):
        """Test service initializes with empty state."""
        assert len(prediction_service.get_averages()) == 0
        assert len(prediction_service.get_raw_accumulator()) == 0

    def test_accumulate_values(self, prediction_service, sample_values):
        """Test accumulation of 15-minute slot data."""
        prediction_service.accumulate_values(sample_values)
        raw = prediction_service.get_raw_accumulator()

        # Bucket for March (03), hour 10 UTC: 3*100 + 10 = 310
        assert 310 in raw
        # 4 slots for March 10:00 (0.5 + 0.6 + 0.7 + 0.8 = 2.6 kWh, 4 slots)
        assert raw[310] == (2.6, 4)

        # Bucket for March (03), hour 11 UTC: 3*100 + 11 = 311
        assert 311 in raw
        # 1 slot for March 11:00 (0.9 kWh, 1 slot)
        assert raw[311] == (0.9, 1)

        # Bucket for April (04), hour 10 UTC: 4*100 + 10 = 410
        assert 410 in raw
        # 2 slots for April 10:00 (1.0 + 1.1 = 2.1 kWh, 2 slots)
        assert raw[410] == (2.1, 2)

    def test_calculate_averages(self, prediction_service, sample_values):
        """Test average calculation from accumulated data."""
        prediction_service.accumulate_values(sample_values)
        averages = prediction_service.calculate_averages()

        # March 10:00 UTC: 2.6 kWh total / (4 slots / 4 per hour) = 2.6 kWh/hour
        assert 310 in averages
        assert averages[310] == pytest.approx(2.6)

        # March 11:00 UTC: 0.9 kWh / (1 slot / 4 per hour) = 3.6 kWh/hour
        # But only has 1 slot (< 4), so should NOT be in averages
        assert 311 not in averages

        # April 10:00 UTC: 2.1 kWh / (2 slots / 4 per hour) = 4.2 kWh/hour
        # Only has 2 slots (< 4), so should NOT be in averages
        assert 410 not in averages

    def test_merge_raw_accumulator(self, prediction_service):
        """Test merging new raw data into existing accumulator."""
        # Initial accumulation
        initial_values = [
            ApiValue(
                timestamp=20260301100000, value=1.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301101500, value=1.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
        ]
        prediction_service.accumulate_values(initial_values)

        # Merge additional data
        new_raw = {310: (2.0, 2)}  # March 10:00: 2.0 kWh, 2 slots
        prediction_service.merge_raw_accumulator(new_raw)

        raw = prediction_service.get_raw_accumulator()
        # Should have merged: (1.0 + 1.0) + 2.0 = 4.0 kWh, 2 + 2 = 4 slots
        assert raw[310] == (4.0, 4)

    def test_generate_predictions(self, prediction_service, sample_values):
        """Test prediction generation from averages."""
        # Accumulate and calculate averages
        prediction_service.accumulate_values(sample_values)
        prediction_service.calculate_averages()

        # Generate predictions from March 1, 2026 10:00 UTC to 13:00 UTC
        last_real = datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)
        target = datetime(2026, 3, 1, 13, 0, 0, tzinfo=UTC)

        predictions = prediction_service.generate_predictions(last_real, target)

        # Should generate 2 hours: 11:00, 12:00 (stops at 13:00, not inclusive)
        assert len(predictions) == 2

        # First prediction: 11:00 UTC (bucket 311 - but we don't have enough data for this)
        assert predictions[0]["start"] == datetime(2026, 3, 1, 11, 0, 0, tzinfo=UTC)
        assert predictions[0]["state"] == 0.0  # No average available

        # Second prediction: 12:00 UTC (bucket 312 - no data)
        assert predictions[1]["start"] == datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
        assert predictions[1]["state"] == 0.0  # No average available

    def test_generate_predictions_with_averages(self, prediction_service):
        """Test predictions use historical averages correctly."""
        # Provide enough data for averages (4+ slots per bucket)
        values = [
            # March 10:00 UTC (bucket 310): 4 slots = 4 kWh
            ApiValue(
                timestamp=20260301100000, value=1.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301101500, value=1.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301103000, value=1.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301104500, value=1.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            # March 11:00 UTC (bucket 311): 4 slots = 8 kWh
            ApiValue(
                timestamp=20260301110000, value=2.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301111500, value=2.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301113000, value=2.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
            ApiValue(
                timestamp=20260301114500, value=2.0, status="VALID", date="2026-03-01", tariff="HT"
            ),
        ]

        prediction_service.accumulate_values(values)
        averages = prediction_service.calculate_averages()

        # Verify averages
        assert averages[310] == pytest.approx(4.0)  # 4 kWh / 1 hour
        assert averages[311] == pytest.approx(8.0)  # 8 kWh / 1 hour

        # Generate predictions
        last_real = datetime(2026, 3, 1, 9, 0, 0, tzinfo=UTC)
        target = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)

        predictions = prediction_service.generate_predictions(last_real, target)

        # Should generate 2 predictions: 10:00, 11:00
        assert len(predictions) == 2

        # 10:00 UTC prediction (bucket 310)
        assert predictions[0]["start"] == datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)
        assert predictions[0]["state"] == pytest.approx(4.0)
        assert predictions[0]["sum"] == pytest.approx(4.0)

        # 11:00 UTC prediction (bucket 311)
        assert predictions[1]["start"] == datetime(2026, 3, 1, 11, 0, 0, tzinfo=UTC)
        assert predictions[1]["state"] == pytest.approx(8.0)
        assert predictions[1]["sum"] == pytest.approx(12.0)  # Running sum: 4.0 + 8.0

    def test_generate_predictions_empty_when_no_averages(self, prediction_service):
        """Test predictions return empty list when no averages available."""
        last_real = datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)
        target = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)

        predictions = prediction_service.generate_predictions(last_real, target)
        assert len(predictions) == 0

    def test_generate_predictions_empty_when_target_before_start(
        self, prediction_service, sample_values
    ):
        """Test predictions return empty when target is before start time."""
        prediction_service.accumulate_values(sample_values)
        prediction_service.calculate_averages()

        last_real = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
        target = datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)  # Before last_real

        predictions = prediction_service.generate_predictions(last_real, target)
        assert len(predictions) == 0

    def test_reset(self, prediction_service, sample_values):
        """Test reset clears all data."""
        prediction_service.accumulate_values(sample_values)
        prediction_service.calculate_averages()

        assert len(prediction_service.get_averages()) > 0
        assert len(prediction_service.get_raw_accumulator()) > 0

        prediction_service.reset()

        assert len(prediction_service.get_averages()) == 0
        assert len(prediction_service.get_raw_accumulator()) == 0
