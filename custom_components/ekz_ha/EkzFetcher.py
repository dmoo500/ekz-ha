"""Interaction with the EKZ API."""

from datetime import date, datetime, timedelta
import itertools
import math
import zoneinfo
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from .session import Session
from .timeutil import format_api_date

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
    """Fetcher for EKZ consumption and production data."""

    def _get_level(self, data: dict) -> str:
        """Extract the response level (DAY or QUARTER_HOUR) from a consumption data response."""
        if not isinstance(data, dict):
            return "QUARTER_HOUR"
        for series_key in ("seriesNt", "seriesHt", "series"):
            s = data.get(series_key)
            if s and isinstance(s, dict) and "level" in s:
                return s["level"]
        return data.get("level", "QUARTER_HOUR")

    def _normalize_timestamp(self, ts: Any) -> str:
        """Convert any timestamp format to a 14-digit string YYYYMMDDHHMMSS."""
        s = str(ts).replace("-", "").replace("T", "").replace(":", "").replace(" ", "")
        return s[:14].ljust(14, "0")

    def _sort_and_filter_values(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Merge NT+HT per slot, then sort and filter."""
        collected = []
        if "seriesNt" in data and data["seriesNt"] is not None:
            collected += [
                dict(x, tariff="NT")
                for x in data["seriesNt"].get("values", [])
                if x.get("status") not in ("NOT_AVAILABLE", "MISSING")
            ]
        if "seriesHt" in data and data["seriesHt"] is not None:
            collected += [
                dict(x, tariff="HT")
                for x in data["seriesHt"].get("values", [])
                if x.get("status") not in ("NOT_AVAILABLE", "MISSING")
            ]
        # Single-tariff customers may have data only in 'series' (no HT/NT split)
        if not collected and "series" in data and data["series"] is not None:
            collected += [
                dict(x, tariff="TOTAL")
                for x in data["series"].get("values", [])
                if x.get("status") not in ("NOT_AVAILABLE", "MISSING")
            ]
        
        values = sorted(collected, key=lambda x: x["timestamp"])
        
        # Sum NT + HT values for the same timestamp into a single slot entry.
        merged = []
        for _ts, g in itertools.groupby(values, lambda v: v["timestamp"]):
            group = list(g)
            merged.append({**group[0], "value": sum(x["value"] for x in group)})
        return merged

    def _determine_date_range(self, meta_entity: Any, contract_start: Union[str, date], force_from_date: Optional[Union[datetime, date]] = None, force_to_date: Optional[Union[datetime, date]] = None) -> Tuple[datetime, datetime]:
        """Determine from_date and to_date."""
        _LOGGER = logging.getLogger(__name__)
        # Determine start date
        if meta_entity is not None and meta_entity._last_import:
            li = meta_entity._last_import
            if isinstance(li, datetime):
                from_date = li + timedelta(days=1)
            else:
                from_date = datetime.combine(li, datetime.min.time()) + timedelta(days=1)
        else:
            if isinstance(contract_start, str):
                from_date = datetime.strptime(contract_start, "%Y-%m-%d")
            else:
                from_date = datetime.combine(contract_start, datetime.min.time())
            if meta_entity is not None and meta_entity._contract_start is None:
                meta_entity.set_contract_start(from_date.date())
        
        # Allow caller to override from_date (e.g. for pending day lookback)
        if force_from_date is not None:
            from_date = datetime.combine(force_from_date, datetime.min.time()) if not isinstance(force_from_date, datetime) else force_from_date
            _LOGGER.info(f"[EkzFetcher] Lookback/Gap: overriding from_date to {from_date}")

        tomorrow_naive = datetime.combine(datetime.now(tz=ZRH).date() + timedelta(days=1), datetime.min.time())
        to_date = min(from_date + timedelta(days=30), tomorrow_naive)
        
        # Allow caller to override to_date (e.g. for gap filling)
        if force_to_date is not None:
            ftd = datetime.combine(force_to_date, datetime.min.time()) if not isinstance(force_to_date, datetime) else force_to_date
            to_date = min(to_date, ftd)
            _LOGGER.info(f"[EkzFetcher] Gap filling: overriding to_date to {to_date}")
            
        return from_date, to_date

    def _get_expected_slots(self, date: datetime) -> int:
        """Return expected 15-min slots for a date, considering DST switches."""
        if is_dst_switchover_date(date, ZRH):
            return 92 if date.month < 6 else 100
        return 96



    def _get_slot_counts_and_raw_averages(self, values: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[int, Tuple[float, int]]]:
        """Count 15-min slots per date and calculate hourly raw sums/counts."""
        slot_counts: dict[str, int] = {}
        hourly_raw: dict[int, tuple[float, int]] = {}  # month*100+hour_utc -> (sum_kwh, count_slots)
        for v in values:
            slot_counts[v["date"]] = slot_counts.get(v["date"], 0) + 1
            ts = str(v["timestamp"])
            hour_utc = int(ts[8:10]) if len(ts) >= 10 else 0
            month = int(v["date"][5:7])
            mh_key = month * 100 + hour_utc
            s, c = hourly_raw.get(mh_key, (0.0, 0))
            hourly_raw[mh_key] = (s + v["value"], c + 1)
        return slot_counts, hourly_raw

    def _aggregate_hourly(self, values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aggregate 15-min slots into hourly buckets."""
        def total_hour(group: Any) -> Dict[str, Any]:
            group = list(group)
            hour_ts = self._normalize_timestamp(str(group[0]["timestamp"])[:10] + "0000")
            return {
                **group[0],
                "value": sum(x["value"] for x in group),
                "timestamp": hour_ts,
            }
        return [
            total_hour(g)
            for _, g in itertools.groupby(
                sorted(values, key=lambda v: str(v["timestamp"])[:10]),
                lambda v: str(v["timestamp"])[:10],
            )
        ]

    def _aggregate_daily(self, values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aggregate data per calendar day."""
        def total_day(group: Any) -> Dict[str, Any]:
            group = list(group)
            return {
                "value": sum(x["value"] for x in group),
                "date": min(x["date"] for x in group),
                "time": "00:00",
                "timestamp": self._normalize_timestamp(min(x["date"] for x in group) + "000000"),
            }
        values = [total_day(g) for _, g in itertools.groupby(values, lambda v: v["date"])]
        return sorted(values, key=lambda x: x["timestamp"])

    def _detect_pending_from(self, slot_counts: Dict[str, int], values: List[Dict[str, Any]], running_sum_offset: float, is_day_level: bool) -> Tuple[Optional[datetime], float]:
        """Detect the first incomplete day for next-cycle lookback."""
        _LOGGER = logging.getLogger(__name__)
        PENDING_MAX_AGE_DAYS = 14
        if is_day_level or not slot_counts:
            return None, running_sum_offset

        date_sums: dict[str, float] = {}
        for v in values:
            date_sums[v["date"]] = date_sums.get(v["date"], 0.0) + v["value"]
        
        today = datetime.now(tz=ZRH).date()
        today_str_cest = today.strftime("%Y-%m-%d")
        running_for_pending = running_sum_offset
        
        for date_str in sorted(slot_counts.keys()):
            if date_str >= today_str_cest:
                break
            date = datetime.strptime(date_str, "%Y-%m-%d")
            expected_slots = self._get_expected_slots(date)
            count = slot_counts.get(date_str, 0)
            if 0 < count < expected_slots:
                age_days = (today - date.date()).days
                if age_days <= PENDING_MAX_AGE_DAYS:
                    _LOGGER.info(f"[EkzFetcher] Pending day: {date_str} ({count}/{expected_slots} slots, {age_days}d ago)")
                    return date, running_for_pending
                else:
                    _LOGGER.debug(f"[EkzFetcher] Incomplete day {date_str} ({count}/{expected_slots} slots) is {age_days}d old — skipping lookback")
                break
            running_for_pending += date_sums.get(date_str, 0.0)
        return None, running_sum_offset

    async def import_full_history_to_statistics(self, hass: Any, installationId: str, contract_start: str, meta_entity: Any = None, running_sum_offset: float = 0.0, force_from_date: Optional[Union[datetime, date]] = None, force_to_date: Optional[Union[datetime, date]] = None) -> Dict[str, Any]:
        """Import data and return as dict for further processing."""
        _LOGGER = logging.getLogger(__name__)
        _LOGGER.debug(f"[import_full_history_to_statistics] Start: installationId={installationId}, contract_start={contract_start}")
        
        from_date, to_date = self._determine_date_range(meta_entity, contract_start, force_from_date, force_to_date)
        
        _LOGGER.debug(f"[import_full_history_to_statistics] Fetching consumption data period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId, "PK_VERB_15MIN", from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        )

        if data is None:
            _LOGGER.info(f"[import_full_history_to_statistics] No data (PK_VERB_15MIN) available")
            if meta_entity is not None:
                meta_entity.set_last_run_date(datetime.now())
            return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf}

        level = self._get_level(data)
        values = self._sort_and_filter_values(data)
        
        _LOGGER.debug(f"[import_full_history_to_statistics] Total values after deduplication (level={level}): {len(values)}")
        
        if not values:
            recent_threshold = datetime.now() - timedelta(days=30)
            if from_date >= recent_threshold:
                _LOGGER.info(f"[import_full_history_to_statistics] No 15-min data for recent period — skipping DAY-level fallback")
                if meta_entity is not None:
                    meta_entity.set_last_run_date(datetime.now())
                return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf}
            
            _LOGGER.info(f"[import_full_history_to_statistics] No PK_VERB_15MIN data — trying PK_VERB_TAG_METER")
            data = await self.session.get_consumption_data(
                installationId, "PK_VERB_TAG_METER", from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
            )
            if data is None:
                if meta_entity is not None:
                    meta_entity.set_last_run_date(datetime.now())
                return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf}
            level = self._get_level(data)
            values = self._sort_and_filter_values(data)
        
        is_day_level = (level == "DAY")
        slot_counts, hourly_raw = ({}, {}) if is_day_level else self._get_slot_counts_and_raw_averages(values)

        if is_day_level:
            values = self._aggregate_daily(values)
        else:
            values = self._aggregate_hourly(values)

        # Detect max complete date
        max_date = None
        today_str_zrh = datetime.now(tz=ZRH).strftime("%Y-%m-%d")
        if is_day_level:
            for value in values:
                if value["date"] < today_str_zrh:
                    d = datetime.strptime(value["date"], "%Y-%m-%d")
                    if max_date is None or d > max_date:
                        max_date = d
        else:
            for date_str, count in slot_counts.items():
                if date_str >= today_str_zrh:
                    continue
                date = datetime.strptime(date_str, "%Y-%m-%d")
                if count == self._get_expected_slots(date):
                    if max_date is None or date > max_date:
                        max_date = date

        running_sum = running_sum_offset
        last_full_day_sum = math.inf
        statistics = []
        last_import = None
        for i, value in enumerate(values):
            date_obj = datetime.strptime(value["date"], "%Y-%m-%d")
            if date_obj == max_date:
                last_full_day_sum = min(running_sum, last_full_day_sum)
            
            stat_dt = datetime.strptime(value["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            statistics.append({
                "start": stat_dt,
                "sum": (running_sum := running_sum + value["value"]),
                "state": value["value"],
            })
            if last_import is None or stat_dt > last_import:
                last_import = stat_dt

        pending_from, pending_sum_offset = self._detect_pending_from(slot_counts, values, running_sum_offset, is_day_level)

        if meta_entity is not None:
            if max_date is not None:
                meta_entity.set_last_import(max_date.date())
            elif not statistics and to_date.date() < datetime.now(tz=ZRH).date():
                meta_entity.set_last_import(to_date.date())
            meta_entity.set_last_run_date(datetime.now())
            if hasattr(meta_entity, "set_pending"):
                meta_entity.set_pending(pending_from.date() if pending_from else None, pending_sum_offset)
        
        return {
            "statistics": statistics,
            "last_import": last_import.date() if last_import else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
            "last_full_day": max_date,
            "last_full_day_sum": last_full_day_sum,
            "pending_from": pending_from.date() if pending_from else None,
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

    async def getInstallations(self) -> Dict[str, Any]:
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

    async def getProductionInstallations(self) -> Dict[str, Any]:
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

    async def import_production_history_to_statistics(self, hass: Any, installationId: str, contract_start: str, meta_entity: Any = None, running_sum_offset: float = 0.0, force_from_date: Optional[Union[datetime, date]] = None, force_to_date: Optional[Union[datetime, date]] = None) -> Dict[str, Any]:
        """Import solar feed-in (production) data. Values from WIRK_NEG_15MIN are negated (positive = kWh exported)."""
        _LOGGER = logging.getLogger(__name__)
        _LOGGER.debug(f"[import_production_history_to_statistics] Start: installationId={installationId}, contract_start={contract_start}")
        
        from_date, to_date = self._determine_date_range(meta_entity, contract_start, force_from_date, force_to_date)
        
        _LOGGER.debug(f"[import_production_history_to_statistics] Fetching production data period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId, "WIRK_NEG_15MIN", from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        )
        if data is None or data == {}:
            _LOGGER.info(f"[import_production_history_to_statistics] No data for {installationId}, period {from_date} to {to_date}")
            if meta_entity is not None:
                if to_date.date() < datetime.now(tz=ZRH).date():
                    meta_entity.set_last_import(to_date.date())
                meta_entity.set_last_run_date(datetime.now())
            return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date()}

        values = self._sort_and_filter_values(data)
        _LOGGER.debug(f"[import_production_history_to_statistics] Values after filter: {len(values)}")

        values = self._aggregate_hourly(values)
        _LOGGER.debug(f"[import_production_history_to_statistics] Values after hourly aggregation: {len(values)}")

        running_sum = running_sum_offset
        statistics = []
        last_import = None
        for value in values:
            production_kwh = value["value"]
            stat_dt = datetime.strptime(value["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            statistics.append({
                "start": stat_dt,
                "sum": (running_sum := running_sum + production_kwh),
                "state": production_kwh,
            })
            if last_import is None or stat_dt > last_import:
                last_import = stat_dt

        _LOGGER.debug(f"[import_production_history_to_statistics] {len(statistics)} statistics entries prepared")
        if meta_entity is not None:
            if last_import is not None:
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
