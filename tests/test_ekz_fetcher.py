import tests.mock_ha
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timedelta, date
import itertools
import math
import zoneinfo
import sys

from custom_components.ekz_ha.EkzFetcher import EkzFetcher, ZRH, UTC

class MockMetaEntity:
    def __init__(self, last_import=None, contract_start=None):
        self._last_import = last_import
        self._contract_start = contract_start
        self._last_run_date = None
        self._pending_from = None
        self._pending_sum_offset = None

    def set_last_import(self, val):
        self._last_import = val

    def set_contract_start(self, val):
        self._contract_start = val

    def set_last_run_date(self, val):
        self._last_run_date = val

    def set_pending(self, date_val, offset):
        self._pending_from = date_val
        self._pending_sum_offset = offset

class TestEkzFetcher(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fetcher = EkzFetcher("user", "pass")
        self.fetcher.session = MagicMock()
        
    def test_normalize_timestamp(self):
        self.assertEqual(self.fetcher._normalize_timestamp("2024-03-24T10:15:00"), "20240324101500")
        self.assertEqual(self.fetcher._normalize_timestamp("20240324101500"), "20240324101500")
        self.assertEqual(self.fetcher._normalize_timestamp("2024-03-24 10:15:00"), "20240324101500")
        
    def test_get_level(self):
        self.assertEqual(self.fetcher._get_level({"seriesNt": {"level": "QUARTER_HOUR"}}), "QUARTER_HOUR")
        self.assertEqual(self.fetcher._get_level({"level": "DAY"}), "DAY")
        self.assertEqual(self.fetcher._get_level({}), "QUARTER_HOUR")

    def test_sort_and_filter_values(self):
        data = {
            "seriesNt": {
                "values": [
                    {"timestamp": "20240324100000", "value": 1.0, "status": "VALID", "date": "2024-03-24"},
                    {"timestamp": "20240324101500", "value": 1.5, "status": "VALID", "date": "2024-03-24"},
                ]
            },
            "seriesHt": {
                "values": [
                    {"timestamp": "20240324100000", "value": 0.5, "status": "VALID", "date": "2024-03-24"},
                ]
            }
        }
        values = self.fetcher._sort_and_filter_values(data)
        self.assertEqual(len(values), 2)
        # Sum of 1.0 + 0.5 for matching timestamp
        self.assertEqual(values[0]["value"], 1.5)
        self.assertEqual(values[0]["timestamp"], "20240324100000")
        self.assertEqual(values[1]["value"], 1.5)
        self.assertEqual(values[1]["timestamp"], "20240324101500")

    def test_aggregate_hourly(self):
        values = [
            {"timestamp": "20240324100000", "value": 1.0, "date": "2024-03-24"},
            {"timestamp": "20240324101500", "value": 1.0, "date": "2024-03-24"},
            {"timestamp": "20240324103000", "value": 1.0, "date": "2024-03-24"},
            {"timestamp": "20240324104500", "value": 1.0, "date": "2024-03-24"},
            {"timestamp": "20240324110000", "value": 2.0, "date": "2024-03-24"},
        ]
        hourly = self.fetcher._aggregate_hourly(values)
        self.assertEqual(len(hourly), 2)
        self.assertEqual(hourly[0]["value"], 4.0)
        self.assertEqual(hourly[0]["timestamp"], "20240324100000")
        self.assertEqual(hourly[1]["value"], 2.0)
        self.assertEqual(hourly[1]["timestamp"], "20240324110000")

    def test_determine_date_range(self):
        meta = MockMetaEntity(last_import=date(2024, 3, 1))
        from_d, to_d = self.fetcher._determine_date_range(meta, "2024-01-01")
        self.assertEqual(from_d.date(), date(2024, 3, 2))
        
        # Test contract start fallback
        meta = MockMetaEntity()
        from_d, to_d = self.fetcher._determine_date_range(meta, "2024-01-01")
        self.assertEqual(from_d.date(), date(2024, 1, 1))
        self.assertEqual(meta._contract_start, date(2024, 1, 1))

    def test_get_expected_slots(self):
        # Normal day
        self.assertEqual(self.fetcher._get_expected_slots(datetime(2024, 1, 1)), 96)
        # Normal day
        self.assertEqual(self.fetcher._get_expected_slots(datetime(2024, 3, 30)), 96)
        # DST spring forward in Zurich (usually last Sunday of March)
        # 2024-03-31 02:00 -> 03:00
        self.assertEqual(self.fetcher._get_expected_slots(datetime(2024, 3, 31)), 92)
        # Normal day
        self.assertEqual(self.fetcher._get_expected_slots(datetime(2024, 4, 1)), 96)
        # DST fall back
        # 2024-10-27 03:00 -> 02:00
        self.assertEqual(self.fetcher._get_expected_slots(datetime(2024, 10, 27)), 100)

    async def test_import_full_history_to_statistics_mocked(self):
        # Provide a full day (96 slots) to trigger max_date update
        values = []
        for h in range(24):
            for m in [0, 15, 30, 45]:
                values.append({
                    "timestamp": f"20240324{h:02d}{m:02d}00",
                    "value": 0.1,
                    "status": "VALID",
                    "date": "2024-03-24"
                })
        
        self.fetcher.session.get_consumption_data = AsyncMock(return_value={
            "seriesNt": {
                "values": values,
                "level": "QUARTER_HOUR"
            }
        })
        meta = MockMetaEntity(last_import=date(2024, 3, 23))
        result = await self.fetcher.import_full_history_to_statistics(None, "inst1", "2024-01-01", meta)
        
        self.assertEqual(len(result["statistics"]), 24) # Aggregated to hourly
        self.assertEqual(result["last_full_day"].date(), date(2024, 3, 24))
        self.assertEqual(meta._last_import, date(2024, 3, 24))

    def test_detect_pending_from(self):
        # Case: Incomplete day (e.g., 10 slots)
        slot_counts = {"2024-03-24": 10}
        values = [{"date": "2024-03-24", "value": 1.0}]
        # We need to mock datetime.now(tz=ZRH) if we want deterministic age check, 
        # but let's assume it's "recent" enough for now or adjust logic.
        # Actually PENDING_MAX_AGE_DAYS is 14. 2024-03-24 is old, so it should NOT be pending.
        
        pending_date, offset = self.fetcher._detect_pending_from(slot_counts, values, 0.0, False)
        self.assertIsNone(pending_date)
        
        # Case: Recent incomplete day (mocking today relative to date)
        # Instead of mocking datetime.now, I'll use a date close to "today"
        recent_date = (datetime.now(tz=ZRH) - timedelta(days=2)).strftime("%Y-%m-%d")
        slot_counts = {recent_date: 10}
        values = [{"date": recent_date, "value": 1.0}]
        pending_date, offset = self.fetcher._detect_pending_from(slot_counts, values, 10.0, False)
        self.assertIsNotNone(pending_date)
        self.assertEqual(pending_date.strftime("%Y-%m-%d"), recent_date)
        self.assertEqual(offset, 10.0)

    async def test_import_production_history_to_statistics_mocked(self):
        self.fetcher.session.get_consumption_data = AsyncMock(return_value={
            "series": {
                "values": [
                    {"timestamp": "20240324100000", "value": 5.0, "status": "VALID", "date": "2024-03-24"},
                ],
                "level": "QUARTER_HOUR"
            }
        })
        meta = MockMetaEntity(last_import=date(2024, 3, 23))
        result = await self.fetcher.import_production_history_to_statistics(None, "inst1", "2024-01-01", meta)
        
        self.assertEqual(len(result["statistics"]), 1)
        self.assertEqual(result["statistics"][0]["state"], 5.0)
        self.assertEqual(meta._last_import, date(2024, 3, 24))

    def test_get_next_fetch_range(self):
        from custom_components.ekz_ha.__init__ import EkzCoordinator
        # Initialize coordinator with mocked dependencies
        coordinator = EkzCoordinator(MagicMock(), MagicMock(), timedelta(days=1), {})
        
        contract_start = date(2024, 1, 1)
        
        # No stretches
        start, end = coordinator._get_next_fetch_range([], contract_start)
        self.assertEqual(start, contract_start)
        self.assertIsNone(end)
        
        # One stretch, no gap at start
        stretches = [{"start": "2024-01-01", "end": "2024-01-05", "end_sum": 10.0}]
        start, end = coordinator._get_next_fetch_range(stretches, contract_start)
        self.assertEqual(start, date(2024, 1, 6))
        self.assertIsNone(end)
        
        # Gap at start
        stretches = [{"start": "2024-01-10", "end": "2024-01-15", "end_sum": 10.0}]
        start, end = coordinator._get_next_fetch_range(stretches, contract_start)
        self.assertEqual(start, contract_start)
        self.assertEqual(end, date(2024, 1, 9))
        
        # Gap between stretches
        stretches = [
            {"start": "2024-01-01", "end": "2024-01-05", "end_sum": 10.0},
            {"start": "2024-01-10", "end": "2024-01-15", "end_sum": 20.0}
        ]
        start, end = coordinator._get_next_fetch_range(stretches, contract_start)
        self.assertEqual(start, date(2024, 1, 6))
        self.assertEqual(end, date(2024, 1, 9))

    def test_update_stretches(self):
        from custom_components.ekz_ha.__init__ import EkzCoordinator
        coordinator = EkzCoordinator(MagicMock(), MagicMock(), timedelta(days=1), {})
        
        # New stretch separate from existing
        stretches = [{"start": "2024-01-01", "end": "2024-01-05", "end_sum": 10.0}]
        new_start = date(2024, 1, 10)
        new_end = date(2024, 1, 15)
        updated = coordinator._update_stretches(stretches, new_start, new_end, 20.0)
        self.assertEqual(len(updated), 2)
        self.assertEqual(updated[1]["start"], new_start.isoformat())
        
        # New stretch merging with preceding
        new_start = date(2024, 1, 6)
        new_end = date(2024, 1, 8)
        updated = coordinator._update_stretches(stretches, new_start, new_end, 15.0)
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["start"], "2024-01-01")
        self.assertEqual(updated[0]["end"], new_end.isoformat())
        self.assertEqual(updated[0]["end_sum"], 15.0)

if __name__ == "__main__":
    unittest.main()
