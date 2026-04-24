import sys
from unittest.mock import MagicMock, AsyncMock

def mock_homeassistant():
    """Mock Home Assistant and other external dependencies with proper hierarchy and unique base classes."""
    
    def inject_mock(module_path):
        parts = module_path.split('.')
        current = ""
        for part in parts:
            current = f"{current}.{part}" if current else part
            if current not in sys.modules:
                m = MagicMock()
                sys.modules[current] = m
    
    inject_mock("homeassistant.core")
    inject_mock("homeassistant.config_entries")
    inject_mock("homeassistant.const")
    inject_mock("homeassistant.exceptions")
    inject_mock("homeassistant.helpers.update_coordinator")
    inject_mock("homeassistant.helpers.entity")
    inject_mock("homeassistant.helpers.typing")
    inject_mock("homeassistant.components.sensor")
    inject_mock("homeassistant.components.recorder")
    inject_mock("homeassistant.components.recorder.models")
    inject_mock("homeassistant.components.recorder.statistics")

    class MockGeneric:
        def __getitem__(self, _): return self
        def __class_getitem__(cls, _): return cls

    class MockConfigFlow(MockGeneric):
        def __init_subclass__(cls, **kwargs): pass
        async def async_set_unique_id(self, *args, **kwargs): pass
        def _abort_if_unique_id_configured(self, *args, **kwargs): pass

    class MockDataUpdateCoordinator(MockGeneric):
        def __init__(self, *args, **kwargs): pass

    class MockCoordinatorEntity:
        def __init__(self, *args, **kwargs): pass

    class MockSensorEntity:
        def __init__(self, *args, **kwargs): pass

    sys.modules["homeassistant.config_entries"].ConfigFlow = MockConfigFlow
    sys.modules["homeassistant.helpers.update_coordinator"].DataUpdateCoordinator = MockDataUpdateCoordinator
    sys.modules["homeassistant.helpers.update_coordinator"].CoordinatorEntity = MockCoordinatorEntity
    sys.modules["homeassistant.components.sensor"].SensorEntity = MockSensorEntity

    sys.modules["aiohttp"] = MagicMock()
    sys.modules["bs4"] = MagicMock()
    sys.modules["pyotp"] = MagicMock()
    sys.modules["pyotp"].TOTP = MagicMock

mock_homeassistant()
