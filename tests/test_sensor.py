import tests.mock_ha
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, date
from custom_components.ekz_ha.sensor import EkzEntity, EkzMetaEntity
from custom_components.ekz_ha.const import DOMAIN

class TestEkzSensors(unittest.TestCase):
    def setUp(self):
        self.coordinator = MagicMock()
        self.installation_id = "12345"
        
    def test_ekz_entity_init(self):
        entity = EkzEntity(self.coordinator, self.installation_id)
        self.assertEqual(entity.installation_id, self.installation_id)
        self.assertEqual(entity._attr_native_unit_of_measurement, "kWh")
        self.assertEqual(entity._attr_unique_id, f"ekz_electricity_consumption_{self.installation_id}")

    def test_ekz_meta_entity_native_value(self):
        entity = EkzMetaEntity(self.coordinator, self.installation_id)
        # Test None
        self.assertIsNone(entity.native_value)
        
        # Test date
        entity.set_last_import(date(2024, 3, 24))
        val = entity.native_value
        self.assertIsInstance(val, datetime)
        self.assertEqual(val.year, 2024)
        self.assertEqual(val.month, 3)
        self.assertEqual(val.day, 24)

if __name__ == "__main__":
    unittest.main()
