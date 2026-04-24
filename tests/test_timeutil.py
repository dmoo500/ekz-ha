import tests.mock_ha
import unittest
from datetime import datetime, date
from custom_components.ekz_ha.timeutil import format_api_date, parse_api_timestamp

class TestTimeUtil(unittest.TestCase):
    def test_format_api_date(self):
        d = date(2024, 3, 24)
        self.assertEqual(format_api_date(d), "2024-03-24")

    def test_parse_api_timestamp(self):
        ts = 20240324101530
        expected = datetime(2024, 3, 24, 10, 15, 30)
        self.assertEqual(parse_api_timestamp(ts), expected)

    def test_parse_api_timestamp_leading_zeros(self):
        # Test a timestamp with a month/day/hour that has leading zeros but is passed as int
        ts = 20240102030405
        expected = datetime(2024, 1, 2, 3, 4, 5)
        self.assertEqual(parse_api_timestamp(ts), expected)
