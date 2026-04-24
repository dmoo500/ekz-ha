"""Entrypoint."""

import asyncio
from datetime import date, datetime, timedelta
import logging
import zoneinfo

from homeassistant import core
from homeassistant.components.recorder import get_instance as get_recorder_instance
from homeassistant.components.recorder.models import StatisticData, StatisticMeanType, StatisticMetaData
from homeassistant.components.recorder.statistics import async_import_statistics, get_last_statistics, statistics_during_period
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import CATCHUP_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, DOMAIN, NORMAL_SCAN_INTERVAL
from .EkzFetcher import EkzFetcher

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")
UTC = zoneinfo.ZoneInfo("UTC")
_LOGGER = logging.getLogger(__name__)


def _make_stat_meta(statistic_id: str) -> StatisticMetaData:
    """Build StatisticMetaData, adding unit_class='energy' when supported (HA 2024.3+)."""
    kwargs = {
        "has_sum": True,
        "mean_type": StatisticMeanType.NONE,
        "source": "recorder",
        "statistic_id": statistic_id,
        "name": None,
        "unit_of_measurement": "kWh",
    }
    try:
        return StatisticMetaData(**kwargs, unit_class="energy")
    except TypeError:
        return StatisticMetaData(**kwargs)


class EkzCoordinator(DataUpdateCoordinator):
    """Coordinates data fetching from EKZ."""

    def __init__(
        self,
        hass: HomeAssistant,
        ekz_fetcher: EkzFetcher,
        update_interval: timedelta,
        config: ConfigEntry,
    ) -> None:
        """Initialize EKZ coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            # Name of the data. For logging purposes.
            name=DOMAIN,
            # Polling interval. Will only be polled if there are subscribers.
            update_interval=update_interval,
            # Set always_update to `False` if the data returned from the
            # api can be compared via `__eq__` to avoid duplicate updates
            # being dispatched to listeners
            always_update=True,
        )
        self.ekz_fetcher = ekz_fetcher
        self.config = config
        self.installations = []
        self.production_installations = {}
        self.consumption_averages = {}
        self.last_sums: dict[str, float] = {}
        self.last_production_sums: dict[str, float] = {}
        self.last_prediction_sums: dict[str, float] = {}
        self.catching_up: dict[str, bool] = {}
        self._normal_interval = update_interval  # remember configured interval for later restore
        self._reset_lock = asyncio.Lock()
        self.consumption_averages_raw: dict[str, dict] = {}  # accumulated slot sums for prediction
        self.next_update_time: datetime | None = None
        self.stretches: dict[str, list[dict]] = {}  # installation_id -> list of DataStretch
        self.production_stretches: dict[str, list[dict]] = {}

    async def _async_setup(self) -> None:
        """Load installations on first start."""
        self.installations = await self.ekz_fetcher.getInstallations()
        self.production_installations = await self.ekz_fetcher.getProductionInstallations()
        _LOGGER.debug(f"Production installations found: {list(self.production_installations.keys())}")

    def _get_next_fetch_range(self, stretches: list[dict], contract_start: date) -> tuple[date, date | None]:
        """Determine the next date range to fetch (either a gap or from the end of everything)."""
        if not stretches:
            return contract_start, None
            
        # Check for gap between contract_start and first stretch
        first_start = date.fromisoformat(stretches[0]["start"])
        if first_start > contract_start:
            return contract_start, first_start - timedelta(days=1)
            
        # Check for gaps between stretches
        for i in range(len(stretches) - 1):
            curr_end = date.fromisoformat(stretches[i]["end"])
            next_start = date.fromisoformat(stretches[i+1]["start"])
            if (next_start - curr_end).days > 1:
                return curr_end + timedelta(days=1), next_start - timedelta(days=1)
                
        # No gaps, continue from the end of the last stretch
        last_end = date.fromisoformat(stretches[-1]["end"])
        return last_end + timedelta(days=1), None

    def _update_stretches(self, stretches: list[dict], new_start: date, new_end: date, new_end_sum: float) -> list[dict]:
        """Merge new data into stretches and return updated list."""
        new_start_iso = new_start.isoformat()
        new_end_iso = new_end.isoformat()
        
        new_stretch = {"start": new_start_iso, "end": new_end_iso, "end_sum": new_end_sum}
        
        all_candidate_stretches = stretches + [new_stretch]
        # Sort by start time
        all_candidate_stretches.sort(key=lambda x: x["start"])
        
        temp_stretches = []
        if all_candidate_stretches:
            curr = all_candidate_stretches[0]
            for i in range(1, len(all_candidate_stretches)):
                nxt = all_candidate_stretches[i]
                curr_end_date = date.fromisoformat(curr["end"])
                nxt_start_date = date.fromisoformat(nxt["start"])
                
                # If they are contiguous or overlap
                if (nxt_start_date - curr_end_date).days <= 1:
                    # Merge them
                    curr["end"] = max(curr["end"], nxt["end"])
                    curr["end_sum"] = nxt["end_sum"] # Assumes nxt is later
                else:
                    temp_stretches.append(curr)
                    curr = nxt
            temp_stretches.append(curr)
            
        return temp_stretches

    async def _shift_statistics(self, statistic_id: str, from_dt: datetime, shift: float) -> None:
        """Fetch statistics after from_dt and re-import them with shifted sums."""
        _LOGGER.info(f"Shifting statistics for {statistic_id} from {from_dt} by {shift:.3f} kWh")
        
        # Get all stats from from_dt until now
        recorder = get_recorder_instance(self.hass)
        stats = await recorder.async_add_executor_job(
            statistics_during_period,
            self.hass,
            from_dt,
            datetime.now(tz=UTC) + timedelta(days=1),
            {statistic_id},
            "hour",
            None,
            {"sum"},
        )
        
        if statistic_id in stats:
            data = []
            for s in stats[statistic_id]:
                data.append(StatisticData(
                    start=s["start"],
                    sum=s["sum"] + shift,
                    state=s["state"]
                ))
            
            if data:
                async_import_statistics(self.hass, _make_stat_meta(statistic_id), data)

    async def _async_update_data(self) -> None:
        """Acquire reset lock then delegate to _do_update_data."""
        async with self._reset_lock:
            await self._do_update_data()

    async def _do_update_data(self) -> None:
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up their data.
        """
        if self.installations is None or self.installations == []:
            self.installations = await self.ekz_fetcher.getInstallations()
        if not self.production_installations:
            self.production_installations = await self.ekz_fetcher.getProductionInstallations()
        meta_entities = getattr(self, "meta_entities", None)
        if not meta_entities:
            _LOGGER.debug("meta_entities not yet available during update — entities not initialized yet.")
        for key in self.installations:
            meta_entity = meta_entities.get(key) if meta_entities else None
            if meta_entity is not None:
                _LOGGER.debug(f"Meta entity for {key}: unique_id={getattr(meta_entity, 'unique_id', None)}, last_import={getattr(meta_entity, '_last_import', None)}, contract_start={getattr(meta_entity, '_contract_start', None)}")
            else:
                _LOGGER.debug(f"Meta entity for {key}: None")
            if meta_entity is None:
                continue  # meta entity required for tracking import state
            # Determine contract_start
            contract_start = meta_entity._contract_start if meta_entity is not None else None
            if contract_start is None:
                contract_start = self.installations[key]["contract_start"]
                if meta_entity is not None and meta_entity._contract_start is None:
                    meta_entity.set_contract_start(
                        datetime.strptime(contract_start, "%Y-%m-%d").date()
                    )
                    _LOGGER.debug(f"Meta entity for {key}: unique_id={meta_entity.unique_id}, last_import={getattr(meta_entity, '_last_import', None)}, contract_start={getattr(meta_entity, '_contract_start', None)}")

            # Query DB only on first cycle after (re)start to restore last import date.
            # In-memory state (set by EkzFetcher after each import) is trusted for all subsequent cycles.
            # Querying every cycle risks overwriting with a stale DB result if the recorder is not yet ready.
            statistic_id = f"sensor.electricity_consumption_ekz_{key}"
            if meta_entity._last_import is None:
                try:
                    last_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                    )
                    if last_stats and statistic_id in last_stats:
                        last_stat_data = last_stats[statistic_id]
                        if last_stat_data:
                            raw_start = last_stat_data[0]["start"]
                            # HA returns start as float (Unix timestamp) or datetime depending on version
                            if isinstance(raw_start, (int, float)):
                                import_dt = datetime.fromtimestamp(float(raw_start), tz=ZRH)
                            elif hasattr(raw_start, "tzinfo") and raw_start.tzinfo is not None:
                                import_dt = raw_start.astimezone(ZRH)
                            else:
                                import_dt = raw_start.replace(tzinfo=ZRH)
                            # Go back 1 day from the last DB entry: the last imported day may have
                            # been partial (EKZ has a ~2-day delay), so we always re-fetch it on
                            # restart to pick up any slots that were added later by EKZ.
                            import_date = import_dt.date() - timedelta(days=1)
                            _LOGGER.info(f"Restored last import for {key} from DB: {import_dt.date()} → rewinding to {import_date} to re-check last day")
                            # Set last_import one day BEFORE the rewind date so the fetcher
                            # starts from import_date (last_import + 1 = import_date).
                            meta_entity.set_last_import(import_date - timedelta(days=1))
                            # Pre-initialise catching_up flag so sensor shows correct value immediately
                            # (The running offset will be queried from DB just before the fetch below.)
                            today_date = datetime.now(tz=ZRH).date()
                            if (today_date - import_date).days <= 1:
                                self.catching_up[key] = False
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing statistics for {key}: {e}")

            # One-time migration: clear data stored under the old statistic_id from early integration versions
            old_statistic_id = f"sensor.ekz_electricity_consumption_{key}"
            try:
                old_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, old_statistic_id, True, {"sum"}
                )
                if old_stats and old_statistic_id in old_stats:
                    _LOGGER.info(f"Migrating: clearing old statistics under {old_statistic_id}")
                    try:
                        from homeassistant.components.recorder.statistics import async_clear_statistics
                        await async_clear_statistics(self.hass, [old_statistic_id])
                    except ImportError:
                        _LOGGER.info(
                            f"async_clear_statistics not available in this HA version — "
                            f"old statistics under {old_statistic_id} will remain but won't affect functionality"
                        )
            except Exception as e:
                _LOGGER.debug(f"Migration check failed for {key}: {e}")

            # Fall back to contract_start when no statistics exist yet (first-ever import)
            if meta_entity._last_import is None and contract_start is not None:
                start = contract_start if not isinstance(contract_start, str) else datetime.strptime(contract_start, "%Y-%m-%d")
                meta_entity.set_last_import(start)
                _LOGGER.info(f"No existing statistics for {key}, starting import from contract start {start}")

            # Import exactly one 30-day chunk per update cycle.
            # If pending days were detected in the previous cycle, re-fetch from that earlier date
            # so EKZ can fill in previously incomplete days.
            pending_from = getattr(meta_entity, "_pending_from", None)
            if pending_from is not None:
                _LOGGER.info(f"[{key}] Pending day lookback: re-fetching from {pending_from}")

            # Always query the DB for the correct running offset at the start of from_date.
            # from_date = (last_import + 1 day) midnight CEST = last_import 22:00 UTC.
            # Querying the DB here is the only reliable way to avoid double-counting when
            # the same slots are re-imported across cycles (e.g. pending days or restarts).
            _last_import_date = meta_entity._last_import
            if _last_import_date is not None:
                if isinstance(_last_import_date, datetime):
                    _last_import_date = _last_import_date.date()
                offset_boundary = datetime.combine(
                    _last_import_date + timedelta(days=1), datetime.min.time()
                ).replace(tzinfo=ZRH).astimezone(UTC)
                try:
                    pre_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        statistics_during_period,
                        self.hass,
                        offset_boundary - timedelta(hours=26),
                        offset_boundary,
                        {statistic_id},
                        "hour",
                        None,
                        {"sum"},
                    )
                    if pre_stats and statistic_id in pre_stats and pre_stats[statistic_id]:
                        running_sum = pre_stats[statistic_id][-1]["sum"]
                        _LOGGER.info(f"DB offset for {key}: {running_sum:.3f} kWh (boundary {offset_boundary})"
                        )
                    else:
                        running_sum = 0.0
                        _LOGGER.debug(f"No DB stats found before {offset_boundary} for {key}, using 0")
                except Exception as e_offset:
                    _LOGGER.debug(f"Could not query DB offset for {key}: {e_offset}")
                    running_sum = self.last_sums.get(key, 0.0)
            else:
                running_sum = 0.0

            # --- New Stretch-based logic ---
            stretches = meta_entity.get_stretches()
            fetch_start, fetch_end = self._get_next_fetch_range(stretches, contract_start)
            
            _LOGGER.info(f"[{key}] Next fetch range: {fetch_start} to {fetch_end or 'now'}")
            
            # Determine correct running_sum for fetch_start
            if not stretches or fetch_start == contract_start:
                running_sum = 0.0
            else:
                # Find the stretch just before fetch_start
                prev_stretch = None
                for s in sorted(stretches, key=lambda x: x["end"]):
                    if date.fromisoformat(s["end"]) < fetch_start:
                        prev_stretch = s
                    else:
                        break
                running_sum = prev_stretch["end_sum"] if prev_stretch else 0.0

            result = await self.ekz_fetcher.import_full_history_to_statistics(
                self.hass, key, contract_start, meta_entity,
                running_sum_offset=running_sum,
                force_from_date=fetch_start,
                force_to_date=fetch_end,
            )
            
            _LOGGER.debug(f"Chunk result for {key}: from={result.get('from_date')} to={result.get('to_date')}, entries={len(result.get('statistics', []))}")
            
            if result.get("statistics"):
                # Check if we filled a gap
                gap_filled = fetch_end is not None
                added_consumption = sum(s["state"] for s in result["statistics"])
                
                # If we filled a gap, we must shift all statistics in all subsequent stretches
                if gap_filled:
                    _LOGGER.info(f"[{key}] Gap filled, shifting subsequent statistics by {added_consumption:.3f} kWh")
                    # All stretches starting after this new one need to be shifted
                    new_stretches = []
                    new_end_date = result["to_date"]
                    
                    for s in stretches:
                        if date.fromisoformat(s["start"]) > new_end_date:
                            s["end_sum"] += added_consumption
                            # Perform DB shift
                            await self._shift_statistics(statistic_id, datetime.fromisoformat(s["start"]) if "T" in s["start"] else datetime.combine(date.fromisoformat(s["start"]), datetime.min.time()).replace(tzinfo=ZRH).astimezone(UTC), added_consumption)
                        new_stretches.append(s)
                    stretches = new_stretches

                # Update stretches with the new data
                new_start_date = result["from_date"]
                new_end_date = result["to_date"]
                new_end_sum = result["statistics"][-1]["sum"]
                
                stretches = self._update_stretches(stretches, new_start_date, new_end_date, new_end_sum)
                meta_entity.set_stretches(stretches)

                # Proceed with regular import
                try:
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(f"sensor.electricity_consumption_ekz_{key}"),
                        [StatisticData(start=s["start"], sum=s["sum"], state=s["state"]) for s in result["statistics"]],
                    )
                except Exception as e:
                    _LOGGER.error(f"Failed to import statistics chunk for {key}: {e}")
            
            # Update last_import for compatibility (point to the end of the last stretch)
            if stretches:
                last_stretch_end = datetime.fromisoformat(stretches[-1]["end"]).astimezone(ZRH).date()
                meta_entity.set_last_import(last_stretch_end)
            # Adjust polling interval based on catch-up status
            today = datetime.now(tz=ZRH).date()
            to_date = result.get("to_date")
            still_catching_up = to_date is not None and (today - to_date).days > 1
            self.catching_up[key] = still_catching_up
            if still_catching_up:
                if self.update_interval != CATCHUP_SCAN_INTERVAL:
                    _LOGGER.info(f"Catch-up mode for {key}: imported up to {to_date}, switching poll interval to {CATCHUP_SCAN_INTERVAL}")
                    self.update_interval = CATCHUP_SCAN_INTERVAL
            else:
                if self.update_interval != NORMAL_SCAN_INTERVAL:
                    _LOGGER.info(f"Catch-up complete for {key}, switching to daily poll interval")
                    self.update_interval = NORMAL_SCAN_INTERVAL

            # Accumulate hourly averages across all imported chunks for prediction.
            # averages_raw: {month*100+hour_utc: (sum_kwh, count_slots)} from EkzFetcher.
            averages = None
            if result.get("averages_raw"):
                raw = result["averages_raw"]
                existing_raw = self.consumption_averages_raw.get(key, {})
                for mh_key, (new_sum, new_count) in raw.items():
                    ex_sum, ex_count = existing_raw.get(mh_key, (0.0, 0))
                    existing_raw[mh_key] = (ex_sum + new_sum, ex_count + new_count)
                self.consumption_averages_raw[key] = existing_raw
                # 4 slots per hour → avg kWh/h = total_kwh / (count_slots / 4)
                averages = {
                    k: v[0] / (v[1] / 4)
                    for k, v in existing_raw.items()
                    if v[1] >= 4  # require at least 1 complete hour of data
                }
                self.consumption_averages[key] = averages
                _LOGGER.debug(f"Updated hourly averages for {key}: {len(averages)} month-hour buckets")
            elif key in self.consumption_averages:
                averages = self.consumption_averages[key]

            # Only run predictions when fully caught up (gap fills the recent EKZ delay)
            if averages and len(result["statistics"]) > 0 and not still_catching_up:
                # Zero out predictions for all periods already covered by real data in this chunk,
                # then extrapolate forward using historical averages.
                predictions = [
                    {"start": x["start"], "sum": 0, "state": 0}
                    for x in result["statistics"]
                ]
                last_actual_start = result["statistics"][-1]["start"]
                # Start predictions at the next full hour after the last real data entry
                pred_start = last_actual_start + timedelta(hours=1)
                running_total = 0.0
                now_utc = datetime.now(tz=UTC)
                while pred_start < now_utc:
                    mh_key = pred_start.month * 100 + pred_start.hour
                    hourly_kwh = averages.get(mh_key, 0.0)
                    running_total += hourly_kwh
                    predictions.append(
                        {
                            "start": pred_start,
                            "sum": running_total,
                            "state": hourly_kwh,
                        }
                    )
                    pred_start = pred_start + timedelta(hours=1)

                if len(predictions) > 1:
                    _LOGGER.info(
                        f"Predictions for {key}: {len(predictions)} entries, "
                        f"gap coverage {result['statistics'][-1]['start'].date()} → {pred_start.date()}"
                    )
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(f"sensor.electricity_consumption_ekz_{key}_prediction"),
                        [StatisticData(start=s["start"], sum=s["sum"], state=s["state"]) for s in predictions],
                    )

        # --- Production (solar feed-in) import loop ---
        production_meta_entities = getattr(self, "production_meta_entities", {})
        for key, info in self.production_installations.items():
            prod_meta = production_meta_entities.get(key) if production_meta_entities else None
            if prod_meta is None:
                continue
            contract_start = info.get("contract_start")
            if prod_meta._contract_start is None and contract_start:
                prod_meta.set_contract_start(datetime.strptime(contract_start, "%Y-%m-%d").date())
            statistic_id = f"sensor.electricity_production_ekz_{key}"
            if prod_meta._last_import is None:
                try:
                    last_stats = await get_recorder_instance(self.hass).async_add_executor_job(
                        get_last_statistics, self.hass, 1, statistic_id, True, {"sum"}
                    )
                    if last_stats and statistic_id in last_stats:
                        last_stat_data = last_stats[statistic_id]
                        if last_stat_data:
                            raw_start = last_stat_data[0]["start"]
                            if isinstance(raw_start, (int, float)):
                                import_dt = datetime.fromtimestamp(float(raw_start), tz=ZRH)
                            elif hasattr(raw_start, "tzinfo") and raw_start.tzinfo is not None:
                                import_dt = raw_start.astimezone(ZRH)
                            else:
                                import_dt = raw_start.replace(tzinfo=ZRH)
                            prod_meta.set_last_import(import_dt.date())
                except Exception as e:
                    _LOGGER.debug(f"Could not query existing production statistics for {key}: {e}")
            
            if prod_meta._last_import is None and contract_start:
                prod_meta.set_last_import(datetime.strptime(contract_start, "%Y-%m-%d"))

            # --- Production Stretch Logic ---
            p_stretches = prod_meta.get_stretches()
            p_contract_start = prod_meta._contract_start
            p_fetch_start, p_fetch_end = self._get_next_fetch_range(p_stretches, p_contract_start)
            
            # Determine running sum offset for production
            p_running_sum = 0.0
            if p_stretches and p_fetch_start != p_contract_start:
                p_prev_stretch = None
                for s in sorted(p_stretches, key=lambda x: x["end"]):
                    if date.fromisoformat(s["end"]) < p_fetch_start:
                        p_prev_stretch = s
                    else:
                        break
                p_running_sum = p_prev_stretch["end_sum"] if p_prev_stretch else 0.0

            result = await self.ekz_fetcher.import_production_history_to_statistics(
                self.hass, key, contract_start, prod_meta,
                running_sum_offset=p_running_sum,
                force_from_date=p_fetch_start,
                force_to_date=p_fetch_end,
            )
            if result.get("statistics"):
                p_gap_filled = p_fetch_end is not None
                p_added_consumption = sum(s["state"] for s in result["statistics"])
                
                if p_gap_filled:
                    _LOGGER.info(f"[{key}] Production gap filled, shifting subsequent statistics by {p_added_consumption:.3f} kWh")
                    p_new_stretches = []
                    p_new_end_date = result["to_date"]
                    for s in p_stretches:
                        if date.fromisoformat(s["start"]) > p_new_end_date:
                            s["end_sum"] += p_added_consumption
                            await self._shift_statistics(statistic_id, datetime.fromisoformat(s["start"]) if "T" in s["start"] else datetime.combine(date.fromisoformat(s["start"]), datetime.min.time()).replace(tzinfo=ZRH).astimezone(UTC), p_added_consumption)
                        p_new_stretches.append(s)
                    p_stretches = p_new_stretches

                p_new_start_date = result["from_date"]
                p_new_end_date = result["to_date"]
                p_new_end_sum = result["statistics"][-1]["sum"]
                
                p_stretches = self._update_stretches(p_stretches, p_new_start_date, p_new_end_date, p_new_end_sum)
                prod_meta.set_stretches(p_stretches)

                _LOGGER.info(f"Importing {len(result['statistics'])} production statistics for {key}")
                try:
                    async_import_statistics(
                        self.hass,
                        _make_stat_meta(statistic_id),
                        [StatisticData(start=s["start"], sum=s["sum"], state=s["state"]) for s in result["statistics"]],
                    )
                except Exception as e:
                    _LOGGER.error(f"Failed to import production statistics for {key}: {e}")
            
            if p_stretches:
                p_last_stretch_end = date.fromisoformat(p_stretches[-1]["end"])
                prod_meta.set_last_import(p_last_stretch_end)

        # Track when the next update is scheduled so the next-sync sensor can display it
        self.next_update_time = datetime.now(tz=UTC) + self.update_interval


async def async_setup_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up integration entry."""
    scan_interval = entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    ekz_fetcher = EkzFetcher(entry.data["user"], entry.data["password"], entry.data.get("totp_secret"), entry.data.get("device_name"))
    coordinator = EkzCoordinator(hass, ekz_fetcher, scan_interval, entry)

    hass.data[DOMAIN] = {
        "conf": entry,
        "coordinator": coordinator,
    }
    await coordinator.async_config_entry_first_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])

    async def handle_reset_statistics(call: core.ServiceCall) -> None:
        """Delete all EKZ statistics from the DB and reset in-memory state so a full re-import starts."""
        import inspect

        async def _clear_statistics(statistic_ids: list[str]) -> bool:
            """Try to clear statistics, handling different HA versions robustly."""
            # Approach 1: module-level function (most HA versions)
            try:
                from homeassistant.components.recorder.statistics import async_clear_statistics
                result = async_clear_statistics(hass, statistic_ids)
                if inspect.isawaitable(result):
                    await result
                return True
            except (ImportError, Exception) as err:
                _LOGGER.debug("Module-level async_clear_statistics failed: %s", err)

            # Approach 2: method on recorder instance (some HA versions)
            try:
                recorder = get_recorder_instance(hass)
                clear_fn = getattr(recorder, "async_clear_statistics", None)
                if clear_fn is not None:
                    result = clear_fn(statistic_ids)
                    if inspect.isawaitable(result):
                        await result
                    return True
            except Exception as err:
                _LOGGER.debug("Recorder instance async_clear_statistics failed: %s", err)

            return False

        statistic_ids = []
        for key in coordinator.installations:
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}")
            statistic_ids.append(f"sensor.electricity_consumption_ekz_{key}_prediction")
        for key in coordinator.production_installations:
            statistic_ids.append(f"sensor.electricity_production_ekz_{key}")

        _LOGGER.info("Resetting EKZ statistics for: %s", statistic_ids)

        # Hold the reset lock so any in-progress _async_update_data finishes first,
        # and new updates are blocked until the state is fully cleared.
        async with coordinator._reset_lock:
            if not await _clear_statistics(statistic_ids):
                _LOGGER.error("Cannot clear statistics: no compatible API found in this HA version")
                return

            # Reset in-memory tracking so the next poll starts from contract_start
            coordinator.last_sums = {}
            coordinator.last_production_sums = {}
            coordinator.last_prediction_sums = {}
            coordinator.catching_up = {}
            for meta in (getattr(coordinator, "meta_entities", None) or {}).values():
                meta.set_last_import(None)
            for meta in (getattr(coordinator, "production_meta_entities", None) or {}).values():
                meta.set_last_import(None)

        # Lock released — now it's safe to schedule the re-import
        _LOGGER.info("EKZ statistics reset complete — re-import will start on next poll")
        await coordinator.async_request_refresh()

    hass.services.async_register(DOMAIN, "reset_statistics", handle_reset_statistics)
    return True

async def async_unload_entry(hass: core.HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if unload_ok:
        hass.data.pop(DOMAIN, None)
        hass.services.async_remove(DOMAIN, "reset_statistics")
    return unload_ok
