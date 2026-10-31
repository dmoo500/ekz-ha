"""Common test fixtures for EKZ integration tests."""

from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers import entity_registry as er

from custom_components.ekz_ha.api.models import ApiValue


@pytest.fixture
def register_energy_entities(hass):
    """Supply entity registration omitted by lightweight coordinator test doubles."""

    def register(coordinator):
        coordinator.hass = hass
        coordinator._statistic_ids = {}
        coordinator._statistic_id_store = Mock(async_save=AsyncMock())
        coordinator._external_migrations = {}
        coordinator._external_migration_store = Mock(async_save=AsyncMock())
        for attribute in ("_import_checkpoints", "_daily_statistics", "_sum_offsets"):
            if not isinstance(getattr(coordinator, attribute, None), dict):
                setattr(coordinator, attribute, {})
        for kind, installations in (
            ("consumption", coordinator.installations),
            ("production", coordinator.production_installations),
        ):
            for key in installations:
                er.async_get(hass).async_get_or_create(
                    "sensor",
                    "ekz_ha",
                    f"ekz_electricity_{kind}_{key}",
                    suggested_object_id=f"electricity_{kind}_ekz_{key}",
                )
                target = f"ekz_ha:electricity_{kind}_{key}"
                coordinator._external_migrations[target] = {"complete": True, "sources": []}
                if kind == "consumption":
                    coordinator._external_migrations[f"{target}_prediction"] = {
                        "complete": True,
                        "sources": [],
                    }

    return register


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
                    timestamp=f"20260813{hour:02d}{minute:02d}00",
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
