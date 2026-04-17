"""Interaction with the EKZ API."""

from datetime import datetime, timedelta
import itertools
import math
import zoneinfo
import logging

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

    async def import_full_history_to_statistics(self, hass, installationId: str, contract_start: str, meta_entity=None, running_sum_offset: float = 0.0, force_from_date=None):
        """Import data and return as dict for further processing, do not write to statistics directly."""
        _LOGGER = logging.getLogger(__name__)
        _LOGGER.debug(f"[import_full_history_to_statistics] Start: installationId={installationId}, contract_start={contract_start}")
        # Determine start date
        if meta_entity is not None and meta_entity._last_import:
            li = meta_entity._last_import
            # _last_import may be a date or datetime – always normalise to datetime
            if isinstance(li, datetime):
                from_date = li + timedelta(days=1)
            else:
                from_date = datetime.combine(li, datetime.min.time()) + timedelta(days=1)
            _LOGGER.debug(f"[import_full_history_to_statistics] Start import from last import: from_date={from_date}")
        else:
            if isinstance(contract_start, str):
                from_date = datetime.strptime(contract_start, "%Y-%m-%d")
            else:
                from_date = datetime.combine(contract_start, datetime.min.time())
            _LOGGER.debug(f"[import_full_history_to_statistics] Start import from contract start: from_date={from_date}")
            # Set contract_start in meta_entity if not set
            if meta_entity is not None and meta_entity._contract_start is None:
                meta_entity.set_contract_start(from_date.date())
        # Allow caller to override from_date (e.g. for pending day lookback)
        if force_from_date is not None:
            from_date = datetime.combine(force_from_date, datetime.min.time()) if not isinstance(force_from_date, datetime) else force_from_date
            _LOGGER.info(f"[import_full_history_to_statistics] Lookback for pending days: overriding from_date to {from_date}")
        # Import exactly one month from from_date
        to_date = from_date + timedelta(days=30)
        _LOGGER.debug(f"[import_full_history_to_statistics] Fetching consumption data: installationId={installationId}, period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId,
            "PK_VERB_15MIN",
            from_date.strftime("%Y-%m-%d"),
            to_date.strftime("%Y-%m-%d"),
        )

        if data is None:
            _LOGGER.info(f"[import_full_history_to_statistics] API returned None for installationId={installationId}, period {from_date} to {to_date} (PK_VERB_15MIN) — no data available")
            if meta_entity is not None:
                meta_entity.set_last_run_date(datetime.now())
            return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf}

        _LOGGER.debug(f"[import_full_history_to_statistics] Consumption data loaded: {len((data.get('seriesNt') or {}).get('values', [])) + len((data.get('seriesHt') or {}).get('values', []))} values")
        # --- Daily aggregation and cumulative sum calculation ---
        def get_level(d):
            """Extract the response level (DAY or QUARTER_HOUR) from a consumption data response."""
            for series_key in ("seriesNt", "seriesHt", "series"):
                s = d.get(series_key)
                if s and isinstance(s, dict) and "level" in s:
                    return s["level"]
            return d.get("level", "QUARTER_HOUR")

        def normalize_timestamp(ts):
            """Convert any timestamp format to a 14-digit string YYYYMMDDHHMMSS."""
            s = str(ts).replace("-", "").replace("T", "").replace(":", "").replace(" ", "")
            return s[:14].ljust(14, "0")

        def sortAndFilter(d):
            collected = []
            if "seriesNt" in d and d["seriesNt"] is not None:
                collected += [
                    dict(x, tariff="NT")
                    for x in d["seriesNt"].get("values", [])
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                ]
            if "seriesHt" in d and d["seriesHt"] is not None:
                collected += [
                    dict(x, tariff="HT")
                    for x in d["seriesHt"].get("values", [])
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                ]
            # Single-tariff customers may have data only in 'series' (no HT/NT split)
            if not collected and "series" in d and d["series"] is not None:
                collected += [
                    dict(x, tariff="TOTAL")
                    for x in d["series"].get("values", [])
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                ]
            values = sorted(collected, key=lambda x: x["timestamp"])
            values = [
                list(g)[0] for _, g in itertools.groupby(values, lambda v: v["timestamp"])
            ]  # deduplicate
            values = sorted(values, key=lambda x: x["timestamp"])
            return values

        level = get_level(data)
        values = sortAndFilter(data)
        
        _LOGGER.debug(f"[import_full_history_to_statistics] Total values after deduplication (level={level}): {len(values)}")
        if values is None or values == {} or len(values) == 0:
            _LOGGER.info(f"[import_full_history_to_statistics] No data returned from get_consumption_data for installationId={installationId}, period {from_date} to {to_date} by type PK_VERB_15MIN - try with PK_VERB_TAG_METER")
            data = await self.session.get_consumption_data(
                installationId,
                "PK_VERB_TAG_METER",
                from_date.strftime("%Y-%m-%d"),
                to_date.strftime("%Y-%m-%d"),
            )
            if data is None:
                _LOGGER.info(f"[import_full_history_to_statistics] API returned None for installationId={installationId}, period {from_date} to {to_date} (PK_VERB_TAG_METER) — no data available")
                if meta_entity is not None:
                    meta_entity.set_last_run_date(datetime.now())
                return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date(), "last_full_day": None, "last_full_day_sum": math.inf}
            level = get_level(data)
            _LOGGER.info(f"[import_full_history_to_statistics] PK_VERB_TAG_METER response level={level}, keys: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}")
            values = sortAndFilter(data)
            _LOGGER.debug(f"[import_full_history_to_statistics] Total values after deduplication (TAG_METER, level={level}): {len(values)}")
        
        is_day_level = (level == "DAY")

        # Count 15-min slots per date BEFORE aggregation — used for full-day detection and pending tracking.
        slot_counts: dict[str, int] = {}
        hourly_raw: dict[int, tuple[float, int]] = {}  # month*100+hour_utc -> (sum_kwh, count_slots)
        if not is_day_level:
            for v in values:
                slot_counts[v["date"]] = slot_counts.get(v["date"], 0) + 1
                ts = str(v["timestamp"])
                hour_utc = int(ts[8:10]) if len(ts) >= 10 else 0
                month = int(v["date"][5:7])
                mh_key = month * 100 + hour_utc
                s, c = hourly_raw.get(mh_key, (0.0, 0))
                hourly_raw[mh_key] = (s + v["value"], c + 1)
            _LOGGER.debug(f"[import_full_history_to_statistics] Slot counts (first 5): {dict(list(sorted(slot_counts.items()))[:5])}")

        # Aggregate per day (for QUARTER_HOUR: sum multiple slots; for DAY: passthrough with 1 entry per day)
        def total(group):
            group = list(group)
            first_item = group[0]
            return {
                "value": sum([x["value"] for x in group]),
                "date": min([x["date"] for x in group]),
                "time": min([x.get("time", "00:00") for x in group]),
                # For DAY level data: use the date field + 00:00:00 instead of the original timestamp
                "timestamp": normalize_timestamp(min([x["date"] for x in group]) + "000000") if is_day_level else normalize_timestamp(min([str(x["timestamp"])[0:10] for x in group])),
            }

        values = [
            total(g)
            for _, g in itertools.groupby(values, lambda v: v["date"] if not is_day_level else v["date"])
        ]
        values = sorted(values, key=lambda x: x["timestamp"])
        _LOGGER.debug(f"[import_full_history_to_statistics] Total values after daily aggregation: {len(values)}")

        if is_day_level:
            # For daily data each entry IS a full day — accept all days except today (may be partial)
            today_str = datetime.now(tz=ZRH).strftime("%Y-%m-%d")
            max_date = None
            for value in values:
                if value["date"] < today_str:
                    d = datetime.strptime(value["date"], "%Y-%m-%d")
                    if max_date is None or d > max_date:
                        max_date = d
        else:
            # For 15-min data: check slot_counts counted BEFORE aggregation.
            # A full day has 96 slots (4 per hour × 24h), 92 for DST spring forward, 100 for DST fall back.
            max_date = None
            today_str_cest = datetime.now(tz=ZRH).strftime("%Y-%m-%d")
            for date_str, count in slot_counts.items():
                if date_str >= today_str_cest:
                    continue  # today may be partial
                date = datetime.strptime(date_str, "%Y-%m-%d")
                expected_slots = (
                    92
                    if is_dst_switchover_date(date, ZRH) and date.month < 6
                    else 100
                    if is_dst_switchover_date(date, ZRH)
                    else 96
                )
                if count == expected_slots:
                    if max_date is None or date > max_date:
                        max_date = date

        running_sum = running_sum_offset
        last_full_day_sum = math.inf
        statistics = []
        last_import = None
        for i, value in enumerate(values):
            if i < 3:  # Log first 3 entries for debugging
                _LOGGER.debug(f"[import_full_history_to_statistics] Sample value {i}: raw_timestamp={value['timestamp']}, date={value['date']}, value={value['value']}")
            date_obj = datetime.strptime(value["date"], "%Y-%m-%d")
            if date_obj == max_date:
                last_full_day_sum = min(running_sum, last_full_day_sum)
            # EKZ API timestamps are in UTC — treat them directly as UTC
            normalized_ts = value["timestamp"]
            if i < 3:
                _LOGGER.debug(f"[import_full_history_to_statistics] Normalized timestamp {i}: {normalized_ts}")
            stat_dt_naive = datetime.strptime(normalized_ts, "%Y%m%d%H%M%S")
            stat_dt = stat_dt_naive.replace(tzinfo=UTC)
            if i < 3:
                _LOGGER.debug(f"[import_full_history_to_statistics] Converted datetime {i}: naive={stat_dt_naive}, utc={stat_dt}")
            statistics.append(
                {
                    "start": stat_dt,
                    "sum": (running_sum := running_sum + value["value"]),
                    "state": value["value"],
                }
            )
            if last_import is None or stat_dt > last_import:
                last_import = stat_dt
        if statistics:
            _LOGGER.debug(f"[import_full_history_to_statistics] Statistics range: first={statistics[0]['start']}, last={statistics[-1]['start']}")
        _LOGGER.debug(f"[import_full_history_to_statistics] Total statistics entries prepared: {len(statistics)}")
        last_full_day = max_date

        # --- Option B: detect first incomplete day for next-cycle lookback ---
        # A pending day has some VALID slots but not a full complement — EKZ may deliver the rest later.
        # Only consider days within the last PENDING_MAX_AGE_DAYS days: EKZ back-fills at most ~14 days.
        # Older incomplete days (e.g. the very first partial day of a contract) are accepted as-is.
        PENDING_MAX_AGE_DAYS = 14
        pending_from = None
        pending_sum_offset = running_sum_offset
        if not is_day_level and slot_counts:
            today = datetime.now(tz=ZRH).date()
            today_str_cest = today.strftime("%Y-%m-%d")
            running_for_pending = running_sum_offset
            for value in values:  # one entry per day, sorted by timestamp
                date_str = value["date"]
                if date_str >= today_str_cest:
                    break  # skip today and future
                date = datetime.strptime(date_str, "%Y-%m-%d")
                expected_slots = (
                    92 if is_dst_switchover_date(date, ZRH) and date.month < 6
                    else 100 if is_dst_switchover_date(date, ZRH)
                    else 96
                )
                count = slot_counts.get(date_str, 0)
                if 0 < count < expected_slots:
                    age_days = (today - date.date()).days
                    if age_days <= PENDING_MAX_AGE_DAYS:
                        pending_from = date
                        pending_sum_offset = running_for_pending
                        _LOGGER.info(
                            f"[import_full_history_to_statistics] Pending day: {date_str} "
                            f"({count}/{expected_slots} slots, {age_days}d ago) — will re-check next cycle"
                        )
                    else:
                        _LOGGER.debug(
                            f"[import_full_history_to_statistics] Incomplete day {date_str} "
                            f"({count}/{expected_slots} slots) is {age_days}d old — accepting as permanent, skipping lookback"
                        )
                    break
                running_for_pending += value["value"]

        # Set last_import and last_run_date in meta_entity if provided.
        # Use max_date (last complete day) instead of the latest timestamp to avoid
        # skipping partial-day data for today on the next import cycle.
        if meta_entity is not None:
            if max_date is not None:
                meta_entity.set_last_import(max_date.date())
            elif not statistics and to_date.date() < datetime.now(tz=ZRH).date():
                # No data at all for a past period — advance to prevent an infinite retry loop.
                _LOGGER.info(f"[import_full_history_to_statistics] No importable data for {from_date.date()} to {to_date.date()} — advancing last_import to {to_date.date()} to avoid retry loop")
                meta_entity.set_last_import(to_date.date())
            # If statistics exist but max_date is None (only partial/today data), do not advance last_import.
            meta_entity.set_last_run_date(datetime.now())
            if hasattr(meta_entity, "set_pending"):
                meta_entity.set_pending(
                    pending_from.date() if pending_from else None,
                    pending_sum_offset,
                )
        _LOGGER.debug(f"[import_full_history_to_statistics] Import finished: {len(statistics)} statistics entries, last_import={last_import}, last_full_day={last_full_day}, pending_from={pending_from}")
        # Return the data for further processing
        return {
            "statistics": statistics,
            "last_import": last_import.date() if last_import else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
            "last_full_day": last_full_day,
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

    async def getInstallations(self) -> dict:
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

    async def getProductionInstallations(self) -> dict:
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

    async def import_production_history_to_statistics(self, hass, installationId: str, contract_start: str, meta_entity=None, running_sum_offset: float = 0.0):
        """Import solar feed-in (production) data. Values from WIRK_NEG_15MIN are negated (positive = kWh exported)."""
        _LOGGER = logging.getLogger(__name__)
        _LOGGER.debug(f"[import_production_history_to_statistics] Start: installationId={installationId}, contract_start={contract_start}")
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
        to_date = from_date + timedelta(days=30)
        _LOGGER.debug(f"[import_production_history_to_statistics] Fetching production data: installationId={installationId}, period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId,
            "WIRK_NEG_15MIN",
            from_date.strftime("%Y-%m-%d"),
            to_date.strftime("%Y-%m-%d"),
        )
        if data is None or data == {}:
            _LOGGER.info(f"[import_production_history_to_statistics] No data for {installationId}, period {from_date} to {to_date}")
            if meta_entity is not None:
                if to_date.date() < datetime.now(tz=ZRH).date():
                    meta_entity.set_last_import(to_date.date())
                meta_entity.set_last_run_date(datetime.now())
            return {"statistics": [], "last_import": None, "from_date": from_date.date(), "to_date": to_date.date()}

        def normalize_timestamp(ts):
            s = str(ts).replace("-", "").replace("T", "").replace(":", "").replace(" ", "")
            return s[:14].ljust(14, "0")

        def get_values(d):
            collected = []
            for key in ("seriesNt", "seriesHt"):
                s = d.get(key)
                if s and isinstance(s, dict):
                    collected += [
                        dict(x, tariff=key)
                        for x in s.get("values", [])
                        if x.get("status") not in ("NOT_AVAILABLE", "MISSING")
                    ]
            # Fallback to 'series' if neither HT nor NT has data
            if not collected:
                s = d.get("series")
                if s and isinstance(s, dict):
                    collected += [
                        dict(x, tariff="series")
                        for x in s.get("values", [])
                        if x.get("status") not in ("NOT_AVAILABLE", "MISSING")
                    ]
            values = sorted(collected, key=lambda x: x["timestamp"])
            values = [list(g)[0] for _, g in itertools.groupby(values, lambda v: v["timestamp"])]
            return sorted(values, key=lambda x: x["timestamp"])

        values = get_values(data)
        _LOGGER.debug(f"[import_production_history_to_statistics] Values after filter: {len(values)}")

        def total(group):
            group = list(group)
            return {
                "value": sum(x["value"] for x in group),
                "date": min(x["date"] for x in group),
                "timestamp": normalize_timestamp(min(str(x["timestamp"])[:10] for x in group)),
            }

        values = [
            total(g)
            for _, g in itertools.groupby(values, lambda v: str(v["timestamp"])[:10])
        ]
        values = sorted(values, key=lambda x: x["timestamp"])

        running_sum = running_sum_offset
        statistics = []
        last_import = None
        for value in values:
            # API returns positive values for energy fed into the grid (WIRK_NEG_15MIN)
            # EKZ API timestamps are in UTC
            production_kwh = value["value"]
            stat_dt_naive = datetime.strptime(value["timestamp"], "%Y%m%d%H%M%S")
            stat_dt = stat_dt_naive.replace(tzinfo=UTC)
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
