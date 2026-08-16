"""Common test fixtures for EKZ integration tests."""


import pytest

from custom_components.ekz_ha.api.models import ApiValue


@pytest.fixture
def sample_api_values():
    """Provide sample API values for testing."""
    return [
        ApiValue(
            timestamp="20260813120000", value=1.5, status="VALID", date="2026-08-13", tariff="HT"
        ),
        ApiValue(
            timestamp="20260813130000", value=2.0, status="VALID", date="2026-08-13", tariff="HT"
        ),
    ]


@pytest.fixture
def sample_15min_values():
    """Provide 15-minute resolution sample data."""
    values = []
    for hour in [12, 13]:
        for minute in [0, 15, 30, 45]:
            values.append(
                ApiValue(
                    timestamp=f"202608{hour}{str(minute).zfill(2)}00",
                    value=0.25,
                    status="VALID",
                    date="2026-08-13",
                    tariff="TOTAL",
                )
            )
    return values


@pytest.fixture
def sample_ht_nt_values():
    """Provide HT and NT values for same timestamps."""
    return [
        ApiValue(
            timestamp="20260813120000", value=1.0, status="VALID", date="2026-08-13", tariff="HT"
        ),
        ApiValue(
            timestamp="20260813120000", value=0.5, status="VALID", date="2026-08-13", tariff="NT"
        ),
        ApiValue(
            timestamp="20260813130000", value=1.5, status="VALID", date="2026-08-13", tariff="HT"
        ),
        ApiValue(
            timestamp="20260813130000", value=0.8, status="VALID", date="2026-08-13", tariff="NT"
        ),
    ]


@pytest.fixture
def sample_consumption_response():
    """Provide sample consumption API response."""
    return {
        "level": "QUARTER_HOUR",
        "seriesHt": {
            "values": [
                {"timestamp": "20260813120000", "value": 1.0, "status": "VALID"},
                {"timestamp": "20260813130000", "value": 1.5, "status": "VALID"},
            ],
        },
        "seriesNt": {
            "values": [
                {"timestamp": "20260813120000", "value": 0.5, "status": "VALID"},
                {"timestamp": "20260813130000", "value": 0.8, "status": "VALID"},
            ],
        },
    }
