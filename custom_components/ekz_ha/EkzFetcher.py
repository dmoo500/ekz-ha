"""Interaction with the EKZ API."""

from datetime import datetime, timedelta, date
import itertools
import math
import zoneinfo
import logging

from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict, Union
from .session import Session
from .timeutil import format_api_date
from .apitypes import ConsumptionData, HistoryImportResult, ProductionImportResult

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")


def is_dst(dt: datetime, timeZone: zoneinfo.ZoneInfo) -> bool:
    """Determine whether the given date is during daylight savings or not."""
    aware_dt = dt.replace(tzinfo=timeZone)
    return aware_dt.dst() != timedelta(0, 0)


def is_dst_switchover_date(dt: datetime, timeZone: zoneinfo.ZoneInfo) -> bool:
    """Determine whether a day is the day on which daylight savings starts/ends."""
    day_after = dt + timedelta(days=1)
    return is_dst(day_after, timeZone) != is_dst(dt, timeZone)


class EkzFetcher:
    @staticmethod
    def _normalize_timestamp(ts: str | int) -> str:
        """Convert any timestamp format to a 14-digit string YYYYMMDDHHMMSS."""
        s = str(ts).replace("-", "").replace("T", "").replace(":", "").replace(" ", "")
        return s[:14].ljust(14, "0")

    def _get_level(self, d: ConsumptionData) -> str:
        """Extract the response level (DAY or QUARTER_HOUR) from a consumption data response."""
        for series_key in ("seriesNt", "seriesHt", "series"):
            s = d.get(series_key)
            if s and isinstance(s, dict) and "level" in s:
                return s["level"]
        return d.get("level", "QUARTER_HOUR")

    def _determine_date_range(
        self,
        from_date_source: Any,
        contract_start: str | date,
        force_from_date: date | datetime | None = None,
    ) -> tuple[datetime, datetime]:
        """Determine from_date and to_date for the import."""
        if from_date_source is not None and getattr(from_date_source, "_last_import", None):
            li = from_date_source._last_import
            if isinstance(li, datetime):
                from_date = li + timedelta(days=1)
            else:
                from_date = datetime.combine(li, datetime.min.time()) + timedelta(days=1)
        else:
            if isinstance(contract_start, str):
                from_date = datetime.strptime(contract_start, "%Y-%m-%d")
            else:
                from_date = datetime.combine(contract_start, datetime.min.time())
                
        if force_from_date is not None:
            from_date = datetime.combine(force_from_date, datetime.min.time()) if not isinstance(force_from_date, datetime) else force_from_date
            
        tomorrow_naive = datetime.combine(datetime.now(tz=ZRH).date() + timedelta(days=1), datetime.min.time())
        to_date = min(from_date + timedelta(days=30), tomorrow_naive)
        return from_date, to_date

    def _merge_tariffs(self, d: ConsumptionData) -> list[dict[str, Any]]:
        """Filter and merge entries from different series (NT, HT, TOTAL)."""
        collected = []
        for key, tariff_name in [("seriesNt", "NT"), ("seriesHt", "HT"), ("series", "TOTAL")]:
            s = d.get(key)
            if s and isinstance(s, dict):
                collected += [
                    dict(x, tariff=tariff_name)
                    for x in s.get("values", [])
                    if x.get("status") not in ("NOT_AVAILABLE", "MISSING")
                ]
            if key in ("seriesNt", "seriesHt") and collected:
                # If we found NT/HT, don't fall back to TOTAL unless we collected nothing
                pass

        values = sorted(collected, key=lambda x: x["timestamp"])
        merged = []
        for _, g in itertools.groupby(values, lambda v: v["timestamp"]):
            group = list(g)
            merged.append({**group[0], "value": sum(x["value"] for x in group)})
        return merged

    def _calculate_slot_counts(self, values: list[dict[str, Any]]) -> tuple[dict[str, int], dict[int, tuple[float, int]]]:
        """Count 15-min slots per date and accumulate sums per hour-month for prediction."""
        slot_counts: dict[str, int] = {}
        hourly_raw: dict[int, tuple[float, int]] = {}
        for v in values:
            v_date = v["date"]
            slot_counts[v_date] = slot_counts.get(v_date, 0) + 1
            ts = str(v["timestamp"])
            hour_utc = int(ts[8:10]) if len(ts) >= 10 else 0
            month = int(v_date[5:7])
            mh_key = month * 100 + hour_utc
            s, c = hourly_raw.get(mh_key, (0.0, 0))
            hourly_raw[mh_key] = (s + v["value"], c + 1)
        return slot_counts, hourly_raw

    def _aggregate_data(self, values: list[dict[str, Any]], level: str) -> list[dict[str, Any]]:
        """Aggregate slot data to hourly or daily buckets."""
        if level == "DAY":
            # Aggregate per calendar day
            def total_day(group: Any) -> dict[str, Any]:
                group = list(group)
                return {
                    "value": sum(x["value"] for x in group),
                    "date": min(x["date"] for x in group),
                    "time": "00:00",
                    "timestamp": self._normalize_timestamp(min(x["date"] for x in group) + "000000"),
                }
            it = itertools.groupby(sorted(values, key=lambda x: x["date"]), lambda v: v["date"])
            return sorted([total_day(g) for _, g in it], key=lambda x: x["timestamp"])
        else:
            # Aggregate 4x15-min slots into hourly buckets
            def total_hour(group: Any) -> dict[str, Any]:
                group = list(group)
                hour_ts = self._normalize_timestamp(str(group[0]["timestamp"])[:10] + "0000")
                return {
                    **group[0],
                    "value": sum(x["value"] for x in group),
                    "timestamp": hour_ts,
                }
            it = itertools.groupby(
                sorted(values, key=lambda v: str(v["timestamp"])[:10]),
                lambda v: str(v["timestamp"])[:10],
            )
            return [total_hour(g) for _, g in it]

    def _detect_pending_day(
        self,
        values: list[dict[str, Any]],
        slot_counts: dict[str, int],
        running_sum_offset: float,
    ) -> tuple[date | None, float]:
        """Identify the first incomplete day for lookback."""
        PENDING_MAX_AGE_DAYS = 14
        pending_from = None
        pending_sum_offset = running_sum_offset
        
        # Build per-date value sums for offset calculation
        date_sums: dict[str, float] = {}
        for v in values:
            date_sums[v["date"]] = date_sums.get(v["date"], 0.0) + v["value"]
            
        today = datetime.now(tz=ZRH).date()
        today_str = today.strftime("%Y-%m-%d")
        running_for_pending = running_sum_offset
        
        for date_str in sorted(slot_counts.keys()):
            if date_str >= today_str:
                break
            d = datetime.strptime(date_str, "%Y-%m-%d")
            expected = (
                92 if is_dst_switchover_date(d, ZRH) and d.month < 6
                else 100 if is_dst_switchover_date(d, ZRH)
                else 96
            )
            count = slot_counts.get(date_str, 0)
            if 0 < count < expected:
                if (today - d.date()).days <= PENDING_MAX_AGE_DAYS:
                    pending_from = d.date()
                    pending_sum_offset = running_for_pending
                break
            running_for_pending += date_sums.get(date_str, 0.0)
            
        return pending_from, pending_sum_offset

    def _prepare_statistics(
        self,
        values: list[dict[str, Any]],
        running_sum_offset: float,
        max_date: datetime | None,
    ) -> tuple[list[dict[str, Any]], float, datetime | None]:
        """Convert processed values into HA statistics format."""
        running_sum = running_sum_offset
        last_full_day_sum = math.inf
        statistics = []
        last_import = None
        
        for value in values:
            date_obj = datetime.strptime(value["date"], "%Y-%m-%d")
            if date_obj == max_date:
                last_full_day_sum = min(running_sum, last_full_day_sum)
                
            stat_dt = datetime.strptime(value["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            running_sum += value["value"]
            statistics.append({
                "start": stat_dt,
                "sum": running_sum,
                "state": value["value"],
            })
            if last_import is None or stat_dt > last_import:
                last_import = stat_dt
                
        return statistics, last_full_day_sum, last_import




    async def import_full_history_to_statistics(
        self,
        hass: Any,
        installationId: str,
        contract_start: str | date,
        meta_entity: Any = None,
        running_sum_offset: float = 0.0,
        force_from_date: date | datetime | None = None,
    ) -> HistoryImportResult:
        """Import data and return as dict for further processing, do not write to statistics directly."""
        _LOGGER = logging.getLogger(__name__)
        from_date, to_date = self._determine_date_range(meta_entity, contract_start, force_from_date)
        
        _LOGGER.debug(f"[import_full_history_to_statistics] Fetching consumption data: {installationId}, period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId, "PK_VERB_15MIN", from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        )

        if data is None:
            if meta_entity: meta_entity.set_last_run_date(datetime.now())
            return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf, "pending_from": None, "pending_sum_offset": running_sum_offset, "averages_raw": {}}

        level = self._get_level(data)
        values = self._merge_tariffs(data)
        
        if not values:
            recent_threshold = datetime.now() - timedelta(days=30)
            if from_date >= recent_threshold:
                if meta_entity: meta_entity.set_last_run_date(datetime.now())
                return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf, "pending_from": None, "pending_sum_offset": running_sum_offset, "averages_raw": {}}
            
            data = await self.session.get_consumption_data(
                installationId, "PK_VERB_TAG_METER", from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
            )
            if not data:
                if meta_entity: meta_entity.set_last_run_date(datetime.now())
                return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf, "pending_from": None, "pending_sum_offset": running_sum_offset, "averages_raw": {}}
            level = self._get_level(data)
            values = self._merge_tariffs(data)
        
        is_day_level = (level == "DAY")
        slot_counts, hourly_raw = self._calculate_slot_counts(values) if not is_day_level else ({}, {})
        
        # Aggregate data if needed
        processed_values = self._aggregate_data(values, level)
        
        # Determine last complete day
        max_date = None
        today_str = datetime.now(tz=ZRH).strftime("%Y-%m-%d")
        if is_day_level:
            for v in sorted(processed_values, key=lambda x: x["date"], reverse=True):
                if v["date"] < today_str:
                    max_date = datetime.strptime(v["date"], "%Y-%m-%d")
                    break
        else:
            for date_str, count in sorted(slot_counts.items(), reverse=True):
                if date_str >= today_str: continue
                d = datetime.strptime(date_str, "%Y-%m-%d")
                expected = (92 if is_dst_switchover_date(d, ZRH) and d.month < 6 else 100 if is_dst_switchover_date(d, ZRH) else 96)
                if count == expected:
                    max_date = d
                    break

        # Prepare statistics
        statistics, last_full_day_sum, last_import_dt = self._prepare_statistics(processed_values, running_sum_offset, max_date)
        
        # Detect pending day
        pending_from, pending_sum_offset = self._detect_pending_day(values, slot_counts, running_sum_offset) if not is_day_level else (None, running_sum_offset)

        if meta_entity:
            if max_date: meta_entity.set_last_import(max_date.date())
            elif not statistics and to_date.date() < datetime.now(tz=ZRH).date():
                meta_entity.set_last_import(to_date.date())
            meta_entity.set_last_run_date(datetime.now())
            if hasattr(meta_entity, "set_pending"):
                meta_entity.set_pending(pending_from, pending_sum_offset)

        return {
            "statistics": statistics,
            "last_import": last_import_dt.date() if last_import_dt else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
            "last_full_day": max_date,
            "last_full_day_sum": last_full_day_sum,
            "pending_from": pending_from,
            "pending_sum_offset": pending_sum_offset,
            "averages_raw": hourly_raw,
        }

    def __init__(self, user: str, password: str, totp_secret: str | None = None, device_name: str | None = None) -> None:
        """Construct an instance of EkzFetcher."""
        self.user = user
        self.password = password
        self.totp_secret = totp_secret
        self.device_name = device_name
        self.session = Session(self.user, self.password, self.totp_secret, self.device_name)

    async def getInstallations(self) -> dict[str, dict[str, str | None]]:
        """Return a dict of installation IDs for current contracts (auszdat == None) with contract_start (einzdat)."""
        _LOGGER = logging.getLogger(__name__)
        data = await self.session.installation_selection_data()
        contracts = data.get("contracts") if isinstance(data, dict) else None
        if not contracts:
            _LOGGER.warning(
                "[getInstallations] No contracts found in API response. "
                "This may indicate a login failure or an empty account. Response: %s",
                data,
            )
            raise ValueError("No contracts returned from EKZ API. Check credentials and that 2FA is disabled.")
        result = {}
        for c in contracts:
            if c.get("auszdat") is None:
                result[c["anlage"]] = {
                    "contract_start": c.get("einzdat")
                }
        _LOGGER.debug("[getInstallations] Found installations: %s", list(result.keys()))
        return result

    async def getProductionInstallations(self) -> dict[str, dict[str, str | None]]:
        """Return a dict of production installation IDs (solar/feed-in) with contract_start."""
        _LOGGER = logging.getLogger(__name__)
        data = await self.session.production_installation_selection_data()
        contracts = data.get("contracts") if isinstance(data, dict) else None
        if not contracts:
            _LOGGER.debug("[getProductionInstallations] No production installations found.")
            return {}
        result = {}
        for c in contracts:
            if c.get("auszdat") is None:
                result[c["anlage"]] = {
                    "contract_start": c.get("einzdat")
                }
        _LOGGER.debug("[getProductionInstallations] Found production installations: %s", list(result.keys()))
        return result

    async def import_production_history_to_statistics(
        self,
        hass: Any,
        installationId: str,
        contract_start: str | date,
        meta_entity: Any = None,
        running_sum_offset: float = 0.0,
    ) -> ProductionImportResult:
        """Import solar feed-in (production) data. Values from WIRK_NEG_15MIN are negated (positive = kWh exported)."""
        _LOGGER = logging.getLogger(__name__)
        from_date, to_date = self._determine_date_range(meta_entity, contract_start)
        
        _LOGGER.debug(f"[import_production_history_to_statistics] Fetching production data: {installationId}, period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId, "WIRK_NEG_15MIN", from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        )
        if data is None or data == {}:
            if meta_entity:
                if to_date.date() < datetime.now(tz=ZRH).date():
                    meta_entity.set_last_import(to_date.date())
                meta_entity.set_last_run_date(datetime.now())
            return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date()}

        values = self._merge_tariffs(data)
        level = self._get_level(data)
        
        # Hourly aggregation for production
        processed_values = self._aggregate_data(values, level)
        
        # Prepare statistics
        statistics, _, last_import = self._prepare_statistics(processed_values, running_sum_offset, None)

        if meta_entity:
            if last_import:
                meta_entity.set_last_import(last_import.astimezone(ZRH).date())
            elif to_date.date() < datetime.now(tz=ZRH).date():
                meta_entity.set_last_import(to_date.date())
            meta_entity.set_last_run_date(datetime.now())
            
        return {
            "statistics": statistics,
            "last_import": last_import.date() if last_import else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
        }
