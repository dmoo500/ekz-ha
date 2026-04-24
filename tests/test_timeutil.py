import unittest
from custom_components.ekz_ha.timeutil import format_api_date, parse_api_timestamp
from datetime import date, datetime

class TestTimeUtil(unittest.TestCase):
    def test_format_api_date(self):
        self.assertEqual(format_api_date(date(2024, 3, 24)), "2024-03-24")
        
    def test_parse_api_timestamp(self):
        dt = parse_api_timestamp("20240324101500")
        self.assertEqual(dt, datetime(2024, 3, 24, 10, 15, 0))

if __name__ == "__main__":
    unittest.main()
