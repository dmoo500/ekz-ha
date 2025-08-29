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

    async def import_full_history_to_statistics(self, hass, installationId: str, statistic_id: str, contract_start: str, meta_entity=None):
        """Import data and return as dict for further processing, do not write to statistics directly."""
        _LOGGER = logging.getLogger(__name__)
        _LOGGER.debug(f"[import_full_history_to_statistics] Start: installationId={installationId}, statistic_id={statistic_id}, contract_start={contract_start}")
        # Determine start date
        if meta_entity is not None and meta_entity._last_import:
            from_date = meta_entity._last_import + timedelta(days=1)
            _LOGGER.debug(f"[import_full_history_to_statistics] Start import from last import: from_date={from_date}")
        else:
            from_date = datetime.strptime(contract_start, "%Y-%m-%d")
            _LOGGER.debug(f"[import_full_history_to_statistics] Start import from contract start: from_date={from_date}")
            # Set contract_start in meta_entity if not set
            if meta_entity is not None and meta_entity._contract_start is None:
                meta_entity.set_contract_start(from_date)
        # Import exactly one month from from_date
        to_date = from_date + timedelta(days=30)
        _LOGGER.debug(f"[import_full_history_to_statistics] Fetching consumption data: installationId={installationId}, period {from_date} to {to_date}")
        data = await self.session.get_consumption_data(
            installationId,
            "PK_VERB_15MIN",
            from_date.strftime("%Y-%m-%d"),
            to_date.strftime("%Y-%m-%d"),
        )
        
            
        _LOGGER.debug(f"[import_full_history_to_statistics] Consumption data loaded: {len(data.get('seriesNt', {}).get('values', [])) + len(data.get('seriesHt', {}).get('values', []))} values")
        # --- Tagesaggregation und Summenberechnung wie in fetchNewInstallationData ---
        def sortAndFilter(data):
            all_fetched_data = [[]]
            if "seriesNt" in data and data["seriesNt"] is not None:
                all_fetched_data.append(
                    dict(x, tariff="NT")
                    for x in data["seriesNt"]["values"]
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                )
            if "seriesHt" in data and data["seriesHt"] is not None:
                all_fetched_data.append(
                    dict(x, tariff="HT")
                    for x in data["seriesHt"]["values"]
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                )
            _LOGGER.debug(f"[import_full_history_to_statistics] Total fetched values before aggregation: {all_fetched_data}")
            values = sorted(
                itertools.chain(*all_fetched_data), key=lambda x: x["timestamp"]
            )
            values = [
                list(g)[0] for _, g in itertools.groupby(values, lambda v: v["timestamp"])
            ]  # deduplicate
            values = sorted(values, key=lambda x: x["timestamp"])
            return values
        values = sortAndFilter(data)
        
        _LOGGER.debug(f"[import_full_history_to_statistics] Total values after deduplication: {len(values)}")
        if values is None or values == {} or len(values) == 0:
            _LOGGER.warning(f"[import_full_history_to_statistics] No data returned from get_consumption_data for installationId={installationId}, period {from_date} to {to_date} by type PK_VERB_15MIN - try with PK_VERB_TAG_METER")
            data = await self.session.get_consumption_data(
                installationId,
                "PK_VERB_TAG_METER",
                from_date.strftime("%Y-%m-%d"),
                to_date.strftime("%Y-%m-%d"),
            )
            values = sortAndFilter(data)
            _LOGGER.debug(f"[import_full_history_to_statistics] Total values after deduplication: {len(values)}")
        
        # Aggregate per day
        def total(group):
            group = list(group)
            return {
                "value": sum([x["value"] for x in group]),
                "date": min([x["date"] for x in group]),
                "time": min([x["time"] for x in group]),
                "timestamp": min([str(x["timestamp"])[0:10] + "0000" for x in group]),
            }

        values = [
            total(g)
            for _, g in itertools.groupby(values, lambda v: str(v["timestamp"])[0:10])
        ]
        values = sorted(values, key=lambda x: x["timestamp"])
        _LOGGER.debug(f"[import_full_history_to_statistics] Total values after daily aggregation: {len(values)}")

        # Find the last day that has 24 entries. Or 23 or 25, if it's daylight savings switchover...
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

        running_sum = 0
        last_full_day_sum = math.inf
        statistics = []
        last_import = None
        for value in values:
            date_obj = datetime.strptime(value["date"], "%Y-%m-%d")
            if date_obj == max_date:
                last_full_day_sum = min(running_sum, last_full_day_sum)
            stat_dt = datetime.strptime(str(value["timestamp"]), "%Y%m%d%H%M%S").astimezone(tz=UTC)
            statistics.append(
                {
                    "start": stat_dt,
                    "sum": (running_sum := running_sum + value["value"]),
                    "state": value["value"],
                }
            )
            if last_import is None or stat_dt > last_import:
                last_import = stat_dt
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

    def __init__(self, user: str, password: str) -> None:
        """Construct an instance of EkzFetcher."""
        self.user = user
        self.password = password
        self.session = Session(self.user, self.password)

    async def getInstallations(self) -> dict:
        """Return a dict of installation IDs for current contracts (auszdat == None) with contract_start (einzdat)."""
        installations = await self.session.installation_selection_data()
        result = {}
        for c in installations["contracts"]:
            if c.get("auszdat") is None:
                result[c["anlage"]] = {
                    "contract_start": c.get("einzdat")
                }
        return result
