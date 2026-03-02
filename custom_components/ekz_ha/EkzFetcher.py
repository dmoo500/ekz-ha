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

    async def import_full_history_to_statistics(self, hass, installationId: str, contract_start: str, meta_entity=None, running_sum_offset: float = 0.0):
        """Import data and return as dict for further processing, do not write to statistics directly."""
        _LOGGER = logging.getLogger(__name__)
        _LOGGER.debug(f"[import_full_history_to_statistics] Start: installationId={installationId}, contract_start={contract_start}")
        # Determine start date
        if meta_entity is not None and meta_entity._last_import:
            from_date = meta_entity._last_import + timedelta(days=1)
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

        _LOGGER.debug(f"[import_full_history_to_statistics] Consumption data loaded: {len(data.get('seriesNt', {}).get('values', [])) + len(data.get('seriesHt', {}).get('values', []))} values")
        # --- Tagesaggregation und Summenberechnung wie in fetchNewInstallationData ---
        def get_level(d):
            """Extract the response level (DAY or QUARTER_HOUR) from a consumption data response."""
            for series_key in ("seriesNt", "seriesHt"):
                s = d.get(series_key)
                if s and isinstance(s, dict) and "level" in s:
                    return s["level"]
            return d.get("level", "QUARTER_HOUR")

        def normalize_timestamp(ts):
            """Convert any timestamp format to a 14-digit string YYYYMMDDHHMMSS."""
            s = str(ts).replace("-", "").replace("T", "").replace(":", "").replace(" ", "")
            return s[:14].ljust(14, "0")

        def sortAndFilter(d):
            all_fetched_data = [[]]
            if "seriesNt" in d and d["seriesNt"] is not None:
                all_fetched_data.append(
                    dict(x, tariff="NT")
                    for x in d["seriesNt"]["values"]
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                )
            if "seriesHt" in d and d["seriesHt"] is not None:
                all_fetched_data.append(
                    dict(x, tariff="HT")
                    for x in d["seriesHt"]["values"]
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                )
            values = sorted(
                itertools.chain(*all_fetched_data), key=lambda x: x["timestamp"]
            )
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
            for _, g in itertools.groupby(values, lambda v: str(v["timestamp"])[0:10] if not is_day_level else v["date"])
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
            # For 15-min data find the last day that has 24 entries (or 23/25 for DST switchover)
            max_date = None
            for k, v in itertools.groupby(values, lambda v: v["date"]):
                date = datetime.strptime(k, "%Y-%m-%d")
                expected_hours_per_day = (
                    23
                    if is_dst_switchover_date(date, ZRH) and date.month < 6
                    else 25
                    if is_dst_switchover_date(date, ZRH)
                    else 24
                )
                if len(list(v)) == expected_hours_per_day:
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
            # Fix timezone handling: EKZ timestamps are in Zurich time, need proper conversion to UTC
            normalized_ts = value["timestamp"]
            if i < 3:
                _LOGGER.debug(f"[import_full_history_to_statistics] Normalized timestamp {i}: {normalized_ts}")
            # Parse as naive datetime, then set Zurich timezone, then convert to UTC
            stat_dt_naive = datetime.strptime(normalized_ts, "%Y%m%d%H%M%S")
            stat_dt_zurich = stat_dt_naive.replace(tzinfo=ZRH)
            stat_dt = stat_dt_zurich.astimezone(UTC)
            if i < 3:
                _LOGGER.debug(f"[import_full_history_to_statistics] Converted datetime {i}: naive={stat_dt_naive}, zurich={stat_dt_zurich}, utc={stat_dt}")
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

        # Set last_import and last_run_date in meta_entity if provided
        if meta_entity is not None:
            if last_import is not None:
                meta_entity.set_last_import(last_import)
            meta_entity.set_last_run_date(datetime.now())
        _LOGGER.debug(f"[import_full_history_to_statistics] Import finished: {len(statistics)} statistics entries, last_import={last_import}, last_full_day={last_full_day}")
        # Return the data for further processing
        return {
            "statistics": statistics,
            "last_import": last_import.date() if last_import else None,
            "from_date": from_date.date(),
            "to_date": to_date.date(),
            "last_full_day": last_full_day,
            "last_full_day_sum": last_full_day_sum,
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
