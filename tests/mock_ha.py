import sys
from unittest.mock import MagicMock, AsyncMock

def mock_homeassistant():
    """Mock Home Assistant and other external dependencies to avoid import errors during tests."""
    mock_modules = [
        "homeassistant",
        "homeassistant.core",
        "homeassistant.components",
        "homeassistant.components.recorder",
        "homeassistant.components.recorder.models",
        "homeassistant.components.recorder.statistics",
        "homeassistant.config_entries",
        "homeassistant.const",
        "homeassistant.helpers",
        "homeassistant.helpers.update_coordinator",
        "homeassistant.exceptions",
    ]
    
    # We want to be sure these are NOT the real ones if they cause trouble
    for module in mock_modules:
        sys.modules[module] = MagicMock()

    # Provide a real base class for ConfigFlow that accepts any kwargs in class definition
    class MockConfigFlow:
        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__()
        
        async def async_set_unique_id(self, *args, **kwargs): pass
        def _abort_if_unique_id_configured(self, *args, **kwargs): pass
        def async_show_form(self, **kwargs): return {"type": "form", **kwargs}
        def async_create_entry(self, **kwargs): return {"type": "create_entry", **kwargs}
        def async_abort(self, **kwargs): return {"type": "abort", **kwargs}
    
    sys.modules["homeassistant.config_entries"].ConfigFlow = MockConfigFlow

    # Special case for DataUpdateCoordinator
    class MockDataUpdateCoordinator:
        def __init__(self, *args, **kwargs):
            pass
    sys.modules["homeassistant.helpers.update_coordinator"].DataUpdateCoordinator = MockDataUpdateCoordinator

    # Also mock aiohttp, bs4, pyotp
    sys.modules["aiohttp"] = MagicMock()
    sys.modules["bs4"] = MagicMock()
    sys.modules["pyotp"] = MagicMock()

mock_homeassistant()
