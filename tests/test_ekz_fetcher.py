import tests.mock_ha
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timedelta, date
import itertools
import math
import zoneinfo

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
        
    async def test_getInstallations(self):
        self.fetcher.session.installation_selection_data = AsyncMock(return_value={
            "contracts": [
                {"anlage": "123", "einzdat": "2024-01-01", "auszdat": None},
            ]
        })
        res = await self.fetcher.getInstallations()
        self.assertIn("123", res)

    async def test_getProductionInstallations(self):
        self.fetcher.session.production_installation_selection_data = AsyncMock(return_value={
            "contracts": [
                {"anlage": "prod1", "einzdat": "2024-01-01", "auszdat": None},
            ]
        })
        res = await self.fetcher.getProductionInstallations()
        self.assertIn("prod1", res)

    async def test_import_full_history_to_statistics_success(self):
        # Mock consumption data with 1 full day (96 slots)
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
        result = await self.fetcher.import_full_history_to_statistics(
            None, "inst1", "2024-01-01", meta
        )
        
        self.assertEqual(len(result["statistics"]), 24) # Aggregated to hourly
        self.assertEqual(result["last_full_day"].date(), date(2024, 3, 24))
        self.assertEqual(meta._last_import, date(2024, 3, 24))

    async def test_import_production_history_to_statistics_success(self):
        # Mock production data with multiple values spanning two hours
        self.fetcher.session.get_consumption_data = AsyncMock(return_value={
            "series": {
                "values": [
                    {"timestamp": "20240324100000", "value": 1.0, "status": "VALID", "date": "2024-03-24"},
                    {"timestamp": "20240324101500", "value": 1.5, "status": "VALID", "date": "2024-03-24"},
                    {"timestamp": "20240324110000", "value": 2.0, "status": "VALID", "date": "2024-03-24"},
                ],
                "level": "QUARTER_HOUR"
            }
        })
        meta = MockMetaEntity(last_import=date(2024, 3, 23))
        result = await self.fetcher.import_production_history_to_statistics(
            None, "inst1", "2024-01-01", meta, running_sum_offset=10.0
        )
        
        # Should be aggregated into 2 hourly statistics (10:00 and 11:00)
        self.assertEqual(len(result["statistics"]), 2)
        
        # First hour (10:00): 1.0 + 1.5 = 2.5
        self.assertEqual(result["statistics"][0]["state"], 2.5)
        self.assertEqual(result["statistics"][0]["sum"], 12.5) # 10.0 + 2.5
        
        # Second hour (11:00): 2.0
        self.assertEqual(result["statistics"][1]["state"], 2.0)
        self.assertEqual(result["statistics"][1]["sum"], 14.5) # 12.5 + 2.0

    def test_normalize_timestamp(self):
        self.assertEqual(self.fetcher._normalize_timestamp("2024-03-24T10:00:00"), "20240324100000")
        self.assertEqual(self.fetcher._normalize_timestamp(2024032410), "20240324100000")

    def test_get_level(self):
        self.assertEqual(self.fetcher._get_level({"series": {"level": "DAY"}}), "DAY")
        self.assertEqual(self.fetcher._get_level({"level": "QUARTER_HOUR"}), "QUARTER_HOUR")

    def test_determine_date_range(self):
        meta = MockMetaEntity(last_import=date(2024, 3, 24))
        from_date, to_date = self.fetcher._determine_date_range(meta, "2024-01-01")
        self.assertEqual(from_date.date(), date(2024, 3, 25))
        
        from_date, to_date = self.fetcher._determine_date_range(None, "2024-01-01")
        self.assertEqual(from_date.date(), date(2024, 1, 1))

    def test_merge_tariffs(self):
        data = {
            "seriesNt": {"values": [{"timestamp": "20240324100000", "value": 1.0, "status": "VALID", "date": "2024-03-24"}]},
            "seriesHt": {"values": [{"timestamp": "20240324100000", "value": 2.0, "status": "VALID", "date": "2024-03-24"}]}
        }
        merged = self.fetcher._merge_tariffs(data)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["value"], 3.0)

    def test_aggregate_data_hourly(self):
        values = [
            {"timestamp": "20240324100000", "value": 1.0, "date": "2024-03-24"},
            {"timestamp": "20240324101500", "value": 1.5, "date": "2024-03-24"},
        ]
        aggregated = self.fetcher._aggregate_data(values, "QUARTER_HOUR")
        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0]["value"], 2.5)
        self.assertEqual(aggregated[0]["timestamp"], "20240324100000")

    def test_prepare_statistics(self):
        values = [
            {"timestamp": "20240324100000", "value": 1.0, "date": "2024-03-24"},
            {"timestamp": "20240324110000", "value": 2.0, "date": "2024-03-24"},
        ]
        stats, last_full_day_sum, last_import_dt = self.fetcher._prepare_statistics(values, 10.0, datetime(2024, 3, 24))
        self.assertEqual(len(stats), 2)
        self.assertEqual(stats[0]["sum"], 11.0)
        self.assertEqual(stats[1]["sum"], 13.0)
        self.assertEqual(last_full_day_sum, 10.0)
        # Actually last_full_day_sum is min(running_sum, last_full_day_sum) in the loop
        # Loop 1: running_sum = 10.0, value = 1.0. date_obj == max_date? Yes. last_full_day_sum = min(inf, 10.0) = 10.0.
        # stats.append(11.0).
        # Loop 2: running_sum = 11.0, value = 2.0. date_obj == max_date? Yes. last_full_day_sum = min(10.0, 11.0) = 10.0.
        # stats.append(13.0).
        self.assertEqual(last_full_day_sum, 10.0)

if __name__ == "__main__":
    unittest.main()
