"""Tests for shared importer behavior."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.ekz_ha import EkzCoordinator, statistics_during_period
from custom_components.ekz_ha.api.models import ApiValue, ConsumptionData, SeriesData, ValueOrigin
from custom_components.ekz_ha.const import NORMAL_SCAN_INTERVAL
from custom_components.ekz_ha.statistics.consumption import ConsumptionImporter
from custom_components.ekz_ha.statistics.importer import ZRH, BaseImporter
from custom_components.ekz_ha.statistics.prediction import PredictionService
from custom_components.ekz_ha.statistics.production import ProductionImporter


class StubImporter(BaseImporter):
    async def fetch_data(self, installation_id, date_from, date_to):
        raise NotImplementedError

    def get_data_type_name(self):
        return "Stub"


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
@pytest.mark.parametrize("missing", ["one", "empty", "complementary"])
async def test_incomplete_tariff_series_cannot_replace_daily_fallback(
    monkeypatch, freezer, importer_type, local_day, missing
):
    from custom_components.ekz_ha.data.validator import day_slots
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH)
    freezer.move_to(start + timedelta(days=2))
    slots = day_slots(local_day.isoformat())
    nt_slots = slots[1:] if missing == "one" else [] if missing == "empty" else slots[1::2]
    ht_slots = slots if missing != "complementary" else slots[::2]

    def split_data(ht, nt):
        return ConsumptionData.from_dict(
            {
                "level": "QUARTER_HOUR",
                "seriesHt": {
                    "values": [{"timestamp": slot.isoformat(), "value": 0.1} for slot in ht]
                },
                "seriesNt": {
                    "values": [{"timestamp": slot.isoformat(), "value": 0.2} for slot in nt]
                },
            }
        )

    partial = split_data(ht_slots, nt_slots)
    api = Mock(
        get_consumption_15min=AsyncMock(return_value=partial),
        get_consumption_daily=AsyncMock(
            return_value=api_series("DAY", [(start.isoformat(), 40.0, "VALID")])
        ),
        get_production_15min=AsyncMock(return_value=partial),
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(local_day)
    meta._daily_statistics = {local_day.isoformat()}
    meta._recent_statistics = [{"start": start.astimezone(UTC), "state": 40.0, "sum": 140.0}]
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    importer = (
        ConsumptionImporter(api, daily_fallback_days=meta._daily_statistics)
        if importer_type is ConsumptionImporter
        else ProductionImporter(api)
    )

    result = await importer.import_statistics(Mock(), "825821", start, meta, 100.0)

    assert sum(row["state"] for row in result["statistics"]) == pytest.approx(40.0)
    assert result["statistics"][-1]["sum"] == pytest.approx(140.0)
    assert meta._pending_from == local_day
    assert result["raw_values"] == []

    complete = split_data(slots, slots)
    api.get_consumption_15min.return_value = complete
    api.get_production_15min.return_value = complete
    result = await importer.import_statistics(Mock(), "825821", start, meta, 100.0)
    assert sum(row["state"] for row in result["statistics"]) == pytest.approx(len(slots) * 0.3)
    assert len(result["raw_values"]) == len(slots)
    assert all(row["source_resolution"] == "hour" for row in result["statistics"])
    assert meta._pending_from is None


def test_calculate_date_range_rechecks_last_imported_day():
    importer = StubImporter(None)
    last_import = date(2026, 9, 30)

    from_date, _ = importer.calculate_date_range(last_import, date(2026, 9, 1), max_days=1)

    assert from_date.date() == last_import


def test_calculate_date_range_preserves_contract_start_before_ten_years():
    importer = StubImporter(None)
    contract_start = date(2000, 1, 1)

    from_date, to_date = importer.calculate_date_range(contract_start, contract_start)

    assert from_date.date() == contract_start
    assert to_date.date() == contract_start + timedelta(days=30)


async def test_empty_initial_windows_expand_and_import_first_available_chunk(freezer, monkeypatch):
    freezer.move_to("2026-10-26 12:00:00+00:00")
    contract_start = date(2000, 1, 1)
    first_available = date(2007, 1, 1)
    importer = StubImporter(None)
    metadata = SimpleNamespace(
        _last_import=contract_start,
        _history_cursor=None,
        _empty_search_days=None,
        _pending_from=None,
        _pending_sum_offset=0.0,
        set_last_import=lambda value: setattr(metadata, "_last_import", value),
        set_history_cursor=lambda value: setattr(metadata, "_history_cursor", value),
        set_last_run_date=lambda value: None,
        set_pending=lambda pending, offset=0.0: setattr(metadata, "_pending_from", pending),
    )
    requests = []

    async def fetch_data(_installation_id, date_from, date_to):
        requests.append((date_from.date(), date_to.date()))
        if date_from.date() == contract_start and date_to.date() <= first_available:
            return ConsumptionData()
        timestamp = datetime.combine(first_available, datetime.min.time(), tzinfo=ZRH)
        return api_series("DAY", [(timestamp.strftime("%Y%m%d%H%M%S"), 8.0, "VALID")])

    monkeypatch.setattr(importer, "fetch_data", fetch_data)

    result = None
    while metadata._empty_search_days is None or metadata._last_import == contract_start:
        result = await importer.import_statistics(
            None, "825821", datetime.combine(contract_start, datetime.min.time()), metadata
        )
        if result["statistics"]:
            break

    assert result is not None
    assert result["statistics"]
    assert result["from_date"] == first_available
    assert result["to_date"] == first_available + timedelta(days=30)
    assert requests[0] == (contract_start, contract_start + timedelta(days=30))
    assert requests[1][0] == contract_start
    assert requests[1][1] == contract_start + timedelta(days=60)
    assert requests[-1] == (first_available, first_available + timedelta(days=30))
    assert metadata._empty_search_days is None


def api_series(level, entries):
    return ConsumptionData.from_dict(
        {
            "level": level,
            "series": {
                "level": level,
                "values": [
                    {"timestamp": timestamp, "value": value, "status": status}
                    for timestamp, value, status in entries
                ],
            },
        }
    )


@pytest.mark.parametrize("recent", [False, True])
async def test_issue20_daily_fallback_fills_days_missing_quarter_hours(freezer, recent):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    start_day = date(2026, 10, 15) if recent else date(2026, 8, 21)
    first_start = datetime.combine(start_day, datetime.min.time(), tzinfo=ZRH)
    next_start = first_start + timedelta(days=1)
    first_timestamp = first_start.astimezone(UTC).strftime("%Y%m%d%H%M%S")
    next_timestamp = next_start.astimezone(UTC).strftime("%Y%m%d%H%M%S")
    quarter_hour = api_series(
        "QUARTER_HOUR",
        [(first_timestamp, 0.25, "VALID"), (next_timestamp, 0.0, "NOT_AVAILABLE")],
    )
    daily = api_series("DAY", [(next_timestamp, 6.0, "VALID")])
    api = Mock(
        get_consumption_15min=AsyncMock(return_value=quarter_hour),
        get_consumption_daily=AsyncMock(return_value=daily),
    )

    result = await ConsumptionImporter(api).fetch_data(
        "825821", first_start, first_start + timedelta(days=2)
    )

    assert sum(value.value for value in result.get_all_values()) == pytest.approx(6.25)
    api.get_consumption_daily.assert_awaited_once()


async def test_issue20_recent_daily_only_response_is_imported(freezer):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    api = Mock(
        get_consumption_15min=AsyncMock(
            return_value=api_series("QUARTER_HOUR", [("20261016220000", 0, "MISSING")])
        ),
        get_consumption_daily=AsyncMock(
            return_value=api_series("DAY", [("20261016220000", 6.0, "VALID")])
        ),
    )

    result = await ConsumptionImporter(api).fetch_data(
        "825821", datetime(2026, 10, 17, tzinfo=ZRH), datetime(2026, 10, 18, tzinfo=ZRH)
    )

    assert not result.is_empty()
    api.get_consumption_daily.assert_awaited_once()


@pytest.mark.parametrize("missing_day", [False, True])
@pytest.mark.parametrize("flushed", [False, True])
@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
async def test_corrected_daily_fallback_with_partial_quarter_hours(
    monkeypatch, freezer, missing_day, flushed, local_day
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH)
    end = start + timedelta(days=1)
    freezer.move_to(end + timedelta(days=2))
    existing = [{"start": start.astimezone(UTC), "state": 6.0, "sum": 106.0}]
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(local_day)
    meta._daily_statistics = {local_day.isoformat()}
    meta._recent_statistics = [] if flushed else existing
    statistic_id = "sensor.electricity_consumption_ekz_825821"
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={statistic_id: existing} if flushed else {})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    api = Mock(
        get_consumption_15min=AsyncMock(
            return_value=api_series(
                "QUARTER_HOUR",
                [((start + timedelta(hours=12)).isoformat(), 0.25, "VALID")]
                + [
                    ((start + timedelta(days=index)).isoformat(), 0.25, "VALID")
                    for index in range(1, 4)
                    if not missing_day or index != 1
                ],
            )
        ),
        get_consumption_daily=AsyncMock(
            return_value=api_series("DAY", [(start.isoformat(), 10.0, "VALID")])
        ),
    )
    importer = ConsumptionImporter(api, daily_fallback_days=meta._daily_statistics)
    result = await importer.import_statistics(Mock(), "825821", start, meta, 100.0)

    daily_rows = [row for row in result["statistics"] if row["start"] < end]
    assert daily_rows[-1]["sum"] == pytest.approx(110.0)
    assert sum(row["state"] for row in daily_rows) == pytest.approx(10.0)
    assert all(value.date != local_day.isoformat() for value in result["raw_values"])
    assert meta._pending_from == local_day
    api.get_consumption_daily.assert_awaited_once()


@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
async def test_complete_quarter_hour_fallback_day_does_not_fetch_daily(freezer, local_day):
    from custom_components.ekz_ha.data.validator import day_slots

    start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH)
    freezer.move_to(start + timedelta(days=2))
    quarter_hour = api_series(
        "QUARTER_HOUR",
        [(slot.isoformat(), 0.25, "VALID") for slot in day_slots(local_day.isoformat())],
    )
    api = Mock(
        get_consumption_15min=AsyncMock(return_value=quarter_hour),
        get_consumption_daily=AsyncMock(),
    )
    importer = ConsumptionImporter(api, daily_fallback_days={local_day.isoformat()})

    result = await importer.fetch_data("825821", start, start + timedelta(days=1))

    assert result is quarter_hour
    api.get_consumption_daily.assert_not_awaited()


async def test_missing_daily_correction_keeps_partial_quarter_hours(freezer):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    start = datetime(2026, 10, 15, tzinfo=ZRH)
    quarter_hour = api_series(
        "QUARTER_HOUR", [((start + timedelta(hours=12)).isoformat(), 0.25, "VALID")]
    )
    api = Mock(
        get_consumption_15min=AsyncMock(return_value=quarter_hour),
        get_consumption_daily=AsyncMock(return_value=ConsumptionData()),
    )
    importer = ConsumptionImporter(api, daily_fallback_days={start.date().isoformat()})

    result = await importer.fetch_data("825821", start, start + timedelta(days=1))

    api.get_consumption_daily.assert_awaited_once()
    assert result.get_all_values() == quarter_hour.get_all_values()


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize(
    "local_day", [date(2026, 1, 15), date(2026, 7, 15), date(2026, 3, 29), date(2026, 10, 25)]
)
async def test_issue20_daily_import_preserves_local_midnight(monkeypatch, importer_type, local_day):
    expected_start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH).astimezone(UTC)
    timestamp = expected_start.strftime("%Y%m%d%H%M%S")
    daily = api_series("DAY", [(timestamp, 6.0, "VALID")])
    importer = importer_type(Mock())
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=daily))

    result = await importer.import_statistics(
        None, "825821", datetime(2026, 1, 1, tzinfo=ZRH), running_sum_offset=100.0
    )

    assert result["statistics"][0]["start"] == expected_start
    assert result["statistics"][0]["sum"] == pytest.approx(106.0)


@pytest.mark.parametrize("mixed", [False, True])
async def test_issue20_daily_values_do_not_feed_prediction(monkeypatch, mixed):
    values = [
        ApiValue.from_dict({"timestamp": f"202607{day:02d}220000", "value": 24.0})
        for day in range(14, 18)
    ]
    if mixed:
        for daily in values:
            daily.origin = ValueOrigin.DAILY_TOTAL
        values.extend(
            ApiValue.from_dict({"timestamp": f"2026071512{minute:02d}00", "value": 0.25})
            for minute in (0, 15, 30, 45)
        )
    level = "QUARTER_HOUR" if mixed else "DAY"
    data = ConsumptionData(series_total=SeriesData(values=values, level=level), level=level)
    importer = ConsumptionImporter(Mock())
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

    result = await importer.import_statistics(None, "825821", datetime(2026, 7, 15, tzinfo=ZRH))
    prediction = PredictionService()
    prediction.accumulate_values(result["raw_values"])
    averages = prediction.calculate_averages()

    assert averages == ({712: 1.0} if mixed else {})
    assert len(result["raw_values"]) == (4 if mixed else 0)


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
async def test_issue20_running_sum_survives_long_data_gap(monkeypatch, entity_type):
    statistic_id = f"sensor.electricity_{entity_type}_ekz_825821"
    import_day = date(2026, 10, 17)
    previous_start = datetime(2026, 8, 21, tzinfo=UTC)

    async def query_statistics(function, hass, start, end, *args):
        if start <= previous_start < end:
            return {statistic_id: [{"start": previous_start, "sum": 1000.0}]}
        return {}

    recorder = Mock(async_add_executor_job=AsyncMock(side_effect=query_statistics))
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    coordinator = SimpleNamespace(hass=Mock())

    offset = await EkzCoordinator._get_running_sum_before(coordinator, statistic_id, import_day)

    assert offset == pytest.approx(1000.0)


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
async def test_issue20_missing_recorder_rows_do_not_discard_known_sum(monkeypatch, entity_type):
    statistic_id = f"sensor.electricity_{entity_type}_ekz_825821"
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    coordinator = SimpleNamespace(hass=Mock())

    offset = await EkzCoordinator._get_running_sum_before(
        coordinator, statistic_id, date(2026, 10, 17), fallback=1000.0
    )

    assert offset == pytest.approx(1000.0)


async def test_issue20_real_daily_fixture_preserves_api_timestamp(monkeypatch):
    fixture_path = (
        Path(__file__).parent.parent
        / "fixtures"
        / "import_2026_03_16-2026_04_16-PK_VERB_TAG_METER.json"
    )
    with fixture_path.open() as fixture_file:
        data = ConsumptionData.from_dict(json.load(fixture_file))
    importer = ConsumptionImporter(Mock())
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

    result = await importer.import_statistics(None, "825821", datetime(2026, 3, 16, tzinfo=ZRH))

    assert result["statistics"][0]["start"] == datetime(2026, 3, 15, 23, tzinfo=UTC)


@pytest.mark.parametrize("entity_type", ["consumption", "production"])
@pytest.mark.parametrize("recorder_flushed", [False, True])
@pytest.mark.parametrize("history_gap", [False, True])
async def test_issue20_consecutive_updates_do_not_read_stale_offset(
    monkeypatch, freezer, entity_type, recorder_flushed, history_gap
):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    installation_id = "825821"
    statistic_id = f"sensor.electricity_{entity_type}_ekz_{installation_id}"
    first_day = date(2026, 8, 1) if history_gap else date(2026, 10, 15)
    meta = SimpleNamespace(_last_import=first_day, _contract_start=first_day)
    info = {installation_id: {"contract_start": first_day.isoformat()}}
    coordinator = SimpleNamespace(
        hass=Mock(),
        api_client=Mock(
            get_production_installations=AsyncMock(return_value=SimpleNamespace(contracts=[])),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
        installations=info if entity_type == "consumption" else {},
        production_installations=info if entity_type == "production" else {},
        meta_entities={installation_id: meta},
        production_meta_entities={installation_id: meta},
        last_sums={},
        last_production_sums={},
        catching_up={},
        prediction_services={},
        migrated_prediction_statistics=set(),
        update_interval=NORMAL_SCAN_INTERVAL,
        _daily_statistics={statistic_id: [first_day.isoformat()]},
    )
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    first_start = datetime.combine(first_day, datetime.min.time(), tzinfo=ZRH).astimezone(UTC)
    persisted_start = first_start - timedelta(hours=1)
    persisted_rows = [{"start": persisted_start, "sum": 1000.0}]

    async def recorder_query(function, hass, *args):
        if function is statistics_during_period:
            start, end = args[:2]
            matching = sorted(
                [row for row in persisted_rows if start <= row["start"] < end],
                key=lambda row: row["start"],
            )
            if matching:
                return {statistic_id: matching}
        return {}

    recorder = Mock(
        async_add_executor_job=AsyncMock(side_effect=recorder_query),
        async_block_till_done=AsyncMock(),
    )
    monkeypatch.setattr("custom_components.ekz_ha.get_recorder_instance", lambda hass: recorder)
    monkeypatch.setattr(
        "custom_components.ekz_ha._prediction_statistic_id",
        lambda hass, key: f"sensor.electricity_consumption_ekz_{key}_prediction",
    )
    queued_imports = []

    def enqueue_statistics(hass, metadata, statistics):
        if metadata["statistic_id"] != statistic_id:
            return
        queued_imports.append(statistics)
        if recorder_flushed:
            persisted_rows.extend(statistics)

    monkeypatch.setattr("custom_components.ekz_ha.async_import_statistics", enqueue_statistics)
    second_day_start = datetime(2026, 10, 16, tzinfo=ZRH).astimezone(UTC)
    call_count = 0

    async def import_chunk(self, hass, key, contract_start, metadata, running_sum_offset):
        nonlocal call_count
        if entity_type == "consumption":
            assert self.daily_fallback_days == metadata._daily_statistics
        if history_gap:
            reading_day = first_day if call_count == 0 else date(2026, 9, 15)
            metadata._last_import = reading_day
            metadata._history_cursor = date(2026, 8, 30) if call_count == 0 else date(2026, 9, 20)
            reading = float(call_count + 1)
            call_count += 1
            return {
                "fetched_data": True,
                "statistics": [
                    {
                        "start": datetime.combine(
                            reading_day, datetime.min.time(), tzinfo=ZRH
                        ).astimezone(UTC),
                        "state": reading,
                        "sum": running_sum_offset + reading,
                    }
                ],
                "to_date": date(2026, 10, 18),
            }
        starts = (
            [second_day_start - timedelta(hours=1), second_day_start]
            if call_count == 0
            else [second_day_start, second_day_start + timedelta(hours=1)]
        )
        call_count += 1
        metadata._last_import = date(2026, 10, 16)
        return {
            "fetched_data": True,
            "statistics": [
                {"start": start, "state": 10.0, "sum": running_sum_offset + 10.0 * index}
                for index, start in enumerate(starts, start=1)
            ],
            "to_date": date(2026, 10, 18),
        }

    importer_type = ConsumptionImporter if entity_type == "consumption" else ProductionImporter
    monkeypatch.setattr(importer_type, "import_statistics", import_chunk)

    await EkzCoordinator._do_update_data(coordinator)
    await EkzCoordinator._do_update_data(coordinator)

    assert len(queued_imports) == 2
    if history_gap:
        assert queued_imports[0][-1]["sum"] == pytest.approx(1001.0)
        assert queued_imports[1][0]["sum"] == pytest.approx(1003.0)
    else:
        assert queued_imports[0][-1]["sum"] == pytest.approx(1020.0)
        assert queued_imports[1][0]["sum"] == pytest.approx(queued_imports[0][-1]["sum"])


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("backfill", [False, True])
async def test_recent_daily_fallback_is_revisited_for_corrections(
    monkeypatch, freezer, importer_type, backfill
):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    meta = EkzMetaEntity(Mock(), "825821")
    first_day = datetime(2026, 10, 15, tzinfo=ZRH)
    next_day = first_day + timedelta(days=1)
    daily = api_series(
        "DAY",
        [
            (first_day.isoformat(), 6.0, "VALID"),
            (next_day.isoformat(), 8.0, "VALID"),
        ],
    )
    importer = importer_type(Mock())
    fetch = AsyncMock(return_value=daily)
    monkeypatch.setattr(importer, "fetch_data", fetch)

    result = await importer.import_statistics(None, "825821", first_day, meta, 100.0)

    assert meta._last_import == next_day.date()
    assert meta._pending_from == first_day.date()
    assert meta._pending_sum_offset == 100.0
    assert result["statistics"][-1]["sum"] == pytest.approx(114.0)

    if backfill:
        start = first_day.astimezone(UTC)
        fetch.return_value = api_series(
            "QUARTER_HOUR",
            [
                ((start + timedelta(minutes=15 * index)).isoformat(), 0.25, "VALID")
                for index in range(192)
            ],
        )
    else:
        fetch.return_value = api_series(
            "DAY",
            [
                (first_day.isoformat(), 10.0, "VALID"),
                (next_day.isoformat(), 8.0, "VALID"),
            ],
        )

    updated = await importer.import_statistics(
        None, "825821", first_day, meta, meta._pending_sum_offset
    )

    assert fetch.await_args.args[1] == first_day
    assert updated["statistics"][-1]["sum"] == pytest.approx(148.0 if backfill else 118.0)
    assert meta._pending_from == (None if backfill else first_day.date())


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_daily_fallback_pending_expires_after_recent_lookback(
    monkeypatch, freezer, importer_type
):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    meta = EkzMetaEntity(Mock(), "825821")
    first_day = datetime(2026, 10, 15, tzinfo=ZRH)
    next_day = first_day + timedelta(days=1)
    importer = importer_type(Mock())

    async def fetch(_installation_id, start, end):
        return api_series(
            "DAY",
            [
                (day.isoformat(), 6.0, "VALID")
                for day in (first_day, next_day)
                if start <= day < end
            ],
        )

    monkeypatch.setattr(importer, "fetch_data", fetch)
    await importer.import_statistics(None, "825821", first_day, meta)
    assert meta._pending_from == first_day.date()

    freezer.move_to("2026-11-15 12:00:00+00:00")
    await importer.import_statistics(None, "825821", first_day, meta)
    assert meta._pending_from == next_day.date()
    assert meta._pending_sum_offset == pytest.approx(6.0)

    freezer.move_to("2026-11-16 12:00:00+00:00")
    await importer.import_statistics(None, "825821", first_day, meta, meta._pending_sum_offset)
    assert meta._pending_from is None
    assert meta._history_cursor == date(2026, 10, 17)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_pending_day_is_revisited_until_complete(monkeypatch, freezer, importer_type):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 10, 15))
    values = []
    for local_day, slots in [(date(2026, 10, 15), 1), (date(2026, 10, 16), 96)]:
        start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH).astimezone(UTC)
        values.extend(
            ((start + timedelta(minutes=15 * index)).strftime("%Y%m%d%H%M%S"), 0.25, "VALID")
            for index in range(slots)
        )
    importer = importer_type(Mock())
    fetch = AsyncMock(return_value=api_series("QUARTER_HOUR", values))
    monkeypatch.setattr(importer, "fetch_data", fetch)

    await importer.import_statistics(None, "825821", datetime(2026, 10, 15), meta, 100.0)
    assert meta._last_import == date(2026, 10, 16)
    assert meta._pending_from == date(2026, 10, 15)
    assert meta._pending_sum_offset == 100.0

    start = datetime(2026, 10, 15, tzinfo=ZRH).astimezone(UTC)
    values = [
        ((start + timedelta(minutes=15 * index)).strftime("%Y%m%d%H%M%S"), 0.25, "VALID")
        for index in range(192)
    ]
    fetch.return_value = api_series("QUARTER_HOUR", values)
    result = await importer.import_statistics(None, "825821", datetime(2026, 10, 15), meta, 100.0)
    assert fetch.await_args.args[1].date() == date(2026, 10, 15)
    assert meta._pending_from is None
    assert result["statistics"][-1]["sum"] == pytest.approx(148.0)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_historical_windows_progress_across_long_reading_gap(
    monkeypatch, freezer, importer_type
):
    freezer.move_to("2026-10-20 12:00:00+00:00")
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    meta = EkzMetaEntity(Mock(), "825821")
    importer = importer_type(Mock())
    readings = [(date(2026, 8, 1), 1.0), (date(2026, 9, 15), 2.0)]

    async def fetch(installation_id, start, end):
        return api_series(
            "QUARTER_HOUR",
            [
                (
                    datetime.combine(day, datetime.min.time(), tzinfo=ZRH)
                    .astimezone(UTC)
                    .strftime("%Y%m%d%H%M%S"),
                    value,
                    "VALID",
                )
                for day, value in readings
                if start.date() <= day < end.date()
            ],
        )

    monkeypatch.setattr(importer, "fetch_data", fetch)
    first = await importer.import_statistics(None, "825821", datetime(2026, 8, 1), meta)
    assert first["statistics"][-1]["state"] == 1.0
    assert meta._last_import == date(2026, 8, 1)
    assert meta._history_cursor == date(2026, 8, 30)

    second = await importer.import_statistics(None, "825821", datetime(2026, 8, 1), meta, 1.0)
    assert second["from_date"] == date(2026, 8, 30)
    assert second["statistics"][-1]["sum"] == 3.0
    assert meta._last_import == date(2026, 9, 15)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_failed_fetch_leaves_import_cursors_unchanged(monkeypatch, importer_type):
    import aiohttp

    from custom_components.ekz_ha.sensor import EkzMetaEntity

    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 8, 1))
    importer = importer_type(Mock())
    monkeypatch.setattr(
        importer, "fetch_data", AsyncMock(side_effect=aiohttp.ClientError("HTTP 503"))
    )

    with pytest.raises(aiohttp.ClientError):
        await importer.import_statistics(None, "825821", datetime(2026, 8, 1), meta)

    assert meta._last_import == date(2026, 8, 1)
    assert meta._history_cursor is None


async def test_inclusive_end_date_does_not_hide_missing_day():
    start = datetime(2026, 10, 15, tzinfo=ZRH)
    end = start + timedelta(days=2)
    quarter_hour = api_series(
        "QUARTER_HOUR",
        [(day.astimezone(UTC).strftime("%Y%m%d%H%M%S"), 0.25, "VALID") for day in (start, end)],
    )
    missing_timestamp = (start + timedelta(days=1)).astimezone(UTC).strftime("%Y%m%d%H%M%S")
    api = Mock(
        get_consumption_15min=AsyncMock(return_value=quarter_hour),
        get_consumption_daily=AsyncMock(
            return_value=api_series("DAY", [(missing_timestamp, 6.0, "VALID")])
        ),
    )

    result = await ConsumptionImporter(api).fetch_data("825821", start, end)

    api.get_consumption_daily.assert_awaited_once()
    assert any(
        value.timestamp == missing_timestamp and value.value == 6.0
        for value in result.get_all_values()
    )


@pytest.mark.parametrize("mixed", [False, True])
@pytest.mark.parametrize("recorder_flushed", [False, True])
@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_daily_fallback_preserves_existing_hourly_rows(
    monkeypatch, mixed, recorder_flushed, importer_type
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    start = datetime(2026, 10, 15, tzinfo=ZRH).astimezone(UTC)
    existing = [
        {"start": start, "state": 1.0, "sum": 101.0},
        {"start": start + timedelta(hours=1), "state": 2.0, "sum": 103.0},
    ]
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    recorder = Mock(
        async_add_executor_job=AsyncMock(
            return_value={statistic_id: existing} if recorder_flushed else {}
        )
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 10, 15))
    meta._recent_statistics = [] if recorder_flushed else existing
    values = [ApiValue.from_dict({"timestamp": start.strftime("%Y%m%d%H%M%S"), "value": 6.0})]
    if mixed:
        values[0].origin = ValueOrigin.DAILY_TOTAL
        values.append(
            ApiValue.from_dict(
                {"timestamp": (start + timedelta(days=1)).strftime("%Y%m%d%H%M%S"), "value": 0.25}
            )
        )
    level = "QUARTER_HOUR" if mixed else "DAY"
    data = ConsumptionData(series_total=SeriesData(values=values, level=level), level=level)
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

    result = await importer.import_statistics(Mock(), "825821", datetime(2026, 10, 15), meta, 100.0)

    assert [row["sum"] for row in result["statistics"][:2]] == [101.0, 103.0]
    assert result["statistics"][-1]["sum"] == pytest.approx(106.25 if mixed else 106.0)
    covered = [row for row in result["statistics"] if row["start"] < start + timedelta(days=1)]
    assert sum(row["state"] for row in covered) == pytest.approx(6.0)
    assert all(row["source_resolution"] == "day" for row in covered)
    assert len(result["raw_values"]) == (1 if mixed else 0)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize(
    "local_day", [date(2026, 1, 15), date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)]
)
async def test_daily_statistics_cover_complete_zurich_day(
    monkeypatch, freezer, importer_type, local_day
):
    freezer.move_to("2026-11-01 12:00:00+00:00")
    start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH)
    end = start + timedelta(days=1)
    importer = importer_type(Mock())
    daily = api_series("DAY", [(start.isoformat(), 6.0, "VALID")])
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=daily))

    result = await importer.import_statistics(None, "825821", start)

    assert result["statistics"][0]["start"] == start.astimezone(UTC)
    assert result["statistics"][0]["end"] == end.astimezone(UTC)


@pytest.mark.parametrize("known_hourly", [False, True])
async def test_single_midnight_row_preserves_known_hourly_resolution(monkeypatch, known_hourly):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    start = datetime(2026, 10, 15, tzinfo=ZRH).astimezone(UTC)
    end = start + timedelta(hours=1) if known_hourly else start + timedelta(days=1)
    row = {"start": start, "end": end, "state": 1.0, "sum": 101.0}
    statistic_id = "sensor.electricity_consumption_ekz_825821"
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={statistic_id: [row]}))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 10, 15))
    importer = ConsumptionImporter(Mock())
    monkeypatch.setattr(
        importer,
        "fetch_data",
        AsyncMock(return_value=api_series("DAY", [(start.strftime("%Y%m%d%H%M%S"), 6.0, "VALID")])),
    )

    result = await importer.import_statistics(Mock(), "825821", datetime(2026, 10, 15), meta, 100.0)

    assert result["statistics"][0]["state"] == (1.0 if known_hourly else 6.0)
    assert result["statistics"][0]["end"] == end


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_persisted_daily_source_survives_recorder_round_trip(
    monkeypatch, hass, importer_type
):
    from homeassistant.helpers.storage import Store

    from custom_components.ekz_ha import _persist_source_resolution
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    local_start = datetime(2026, 10, 15, tzinfo=ZRH)
    start = local_start.astimezone(UTC)
    daily = api_series("DAY", [(start.isoformat(), 6.0, "VALID")])
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=daily))
    first = await importer.import_statistics(None, "825821", local_start)
    key = f"ekz_ha.test.{importer.get_data_type_name()}.source_resolution"
    before_restart = SimpleNamespace(_source_resolution_store=Store(hass, 1, key))
    await _persist_source_resolution(before_restart, statistic_id, first["statistics"])

    after_restart = SimpleNamespace(
        _source_resolution_store=Store(hass, 1, key),
        api_client=Mock(
            get_consumption_installations=AsyncMock(
                return_value=SimpleNamespace(
                    contracts=[SimpleNamespace(anlage="825821", einzdat="2026-10-15", auszdat=None)]
                )
            ),
            get_production_installations=AsyncMock(return_value=SimpleNamespace(contracts=[])),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
    )
    await EkzCoordinator._async_setup(after_restart)
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(local_start.date())
    meta._daily_statistics = set(after_restart._daily_statistics[statistic_id])
    recorded = {
        "start": start.timestamp(),
        "end": (start + timedelta(hours=1)).timestamp(),
        "state": 4.0,
        "sum": 104.0,
    }
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={statistic_id: [recorded]}))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )

    corrected = await importer.import_statistics(hass, "825821", local_start, meta, 100.0)

    assert meta._recent_statistics == []
    assert corrected["statistics"][0]["state"] == 6.0
    assert corrected["statistics"][0]["sum"] == 106.0
    assert corrected["statistics"][0]["end"] == (local_start + timedelta(days=1)).astimezone(UTC)
    assert corrected["statistics"][0]["source_resolution"] == "day"


@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
@pytest.mark.parametrize("daily_total", [0.0, 2.0, 6.0])
async def test_daily_reconciliation_conserves_energy_on_dst_days(
    monkeypatch, freezer, local_day, daily_total
):
    freezer.move_to("2026-11-01 12:00:00+00:00")
    local_start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH)
    start = local_start.astimezone(UTC)
    end = (local_start + timedelta(days=1)).astimezone(UTC)
    existing = [
        {"start": start, "state": 1.0, "sum": 101.0},
        {"start": start + timedelta(hours=1), "state": 2.0, "sum": 103.0},
    ]
    statistic_id = "sensor.electricity_consumption_ekz_825821"
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={statistic_id: existing}))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    importer = ConsumptionImporter(Mock())
    daily = api_series("DAY", [(start.isoformat(), daily_total, "VALID")])
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=daily))

    result = await importer.import_statistics(
        Mock(), "825821", local_start, running_sum_offset=100.0
    )

    rows = result["statistics"]
    assert len(rows) == int((end - start).total_seconds() / 3600)
    assert rows[0]["start"] == start
    assert rows[-1]["end"] == end
    assert rows[-1]["sum"] == pytest.approx(100.0 + daily_total)
    assert sum(row["state"] for row in rows) == pytest.approx(daily_total)
    assert all(row["state"] >= 0 for row in rows)
    assert result["raw_values"] == []


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
@pytest.mark.parametrize("reconciled", [False, True])
@pytest.mark.parametrize("queued", [False, True])
async def test_daily_source_waits_for_complete_quarter_hour_backfill(
    monkeypatch, freezer, importer_type, local_day, reconciled, queued
):
    from custom_components.ekz_ha import _persist_source_resolution
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    local_start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH)
    start = local_start.astimezone(UTC)
    end = (local_start + timedelta(days=1)).astimezone(UTC)
    freezer.move_to(end + timedelta(days=2))
    hours = int((end - start).total_seconds() / 3600)
    existing = [
        {
            "start": start + timedelta(hours=index),
            "state": 24.0 / hours if reconciled else 24.0,
            "sum": 100.0 + (index + 1) * (24.0 / hours if reconciled else 24.0),
            "end": start + timedelta(hours=index + 1) if reconciled else end,
            "source_resolution": "day",
        }
        for index in range(hours if reconciled else 1)
    ]
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    persisted = [
        {
            "start": row["start"].timestamp(),
            "end": (row["start"] + timedelta(hours=1)).timestamp(),
            "state": row["state"],
            "sum": row["sum"],
        }
        for row in existing
    ]
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={} if queued else {statistic_id: persisted})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(local_day)
    meta._daily_statistics = {local_day.isoformat()}
    meta._recent_statistics = existing if queued else []
    partial = api_series(
        "QUARTER_HOUR",
        [
            ((start + timedelta(hours=12, minutes=15 * index)).isoformat(), 0.5, "VALID")
            for index in range(4)
        ]
        + [(end.isoformat(), 0.5, "VALID")],
    )
    fetch = AsyncMock(return_value=partial)
    monkeypatch.setattr(importer, "fetch_data", fetch)

    result = await importer.import_statistics(Mock(), "825821", local_start, meta, 100.0)

    retained_day = [row for row in result["statistics"] if row["start"] < end]
    assert sum(row["state"] for row in retained_day) == pytest.approx(24.0)
    assert result["statistics"][0]["start"] == start
    assert retained_day[-1]["sum"] == pytest.approx(124.0)
    assert retained_day[-1]["end"] == end
    assert all(row["source_resolution"] == "day" for row in retained_day)
    assert result["statistics"][-1]["start"] == end
    assert result["statistics"][-1]["sum"] == pytest.approx(124.5)
    assert len(result["raw_values"]) == 1
    assert result["raw_values"][0].timestamp == end.strftime("%Y%m%d%H%M%S")

    source_metadata = SimpleNamespace(_daily_statistics={statistic_id: [local_day.isoformat()]})
    await _persist_source_resolution(source_metadata, statistic_id, result["statistics"])
    meta._daily_statistics = set(source_metadata._daily_statistics[statistic_id])
    meta._recent_statistics = result["statistics"] if queued else []
    if not queued:
        recorder.async_add_executor_job.return_value = {
            statistic_id: [
                {
                    "start": row["start"].timestamp(),
                    "end": (row["start"] + timedelta(hours=1)).timestamp(),
                    "state": row["state"],
                    "sum": row["sum"],
                }
                for row in result["statistics"]
            ]
        }
    assert meta._pending_from == local_day
    fetch.return_value = api_series(
        "QUARTER_HOUR",
        [
            ((start + timedelta(minutes=15 * index)).isoformat(), 0.5, "VALID")
            for index in range(hours * 4)
        ],
    )

    complete = await importer.import_statistics(Mock(), "825821", local_start, meta, 100.0)

    completed_day = [row for row in complete["statistics"] if row["start"] < end]
    assert len(completed_day) == hours
    assert complete["statistics"][0]["start"] == start
    assert completed_day[-1]["end"] == end
    assert completed_day[-1]["sum"] == pytest.approx(100.0 + hours * 2.0)
    assert complete["statistics"][-1]["start"] == end
    assert complete["statistics"][-1]["sum"] == pytest.approx(100.5 + hours * 2.0)
    assert all(row["source_resolution"] == "hour" for row in complete["statistics"])
    assert len(complete["raw_values"]) == hours * 4 + (1 if queued else 0)
    await _persist_source_resolution(source_metadata, statistic_id, complete["statistics"])
    assert source_metadata._daily_statistics[statistic_id] == []


@pytest.mark.parametrize("local_day", [date(2026, 3, 29), date(2026, 7, 15), date(2026, 10, 25)])
@pytest.mark.parametrize("shifted", [False, True])
async def test_source_resolution_requires_exact_hourly_grid(local_day, shifted):
    from custom_components.ekz_ha import _persist_source_resolution

    start = datetime.combine(local_day, datetime.min.time(), tzinfo=ZRH).astimezone(UTC)
    end = datetime.combine(
        local_day + timedelta(days=1), datetime.min.time(), tzinfo=ZRH
    ).astimezone(UTC)
    hours = int((end - start).total_seconds() / 3600)
    rows = [
        {
            "start": start + timedelta(hours=index, minutes=1 if shifted else 0),
            "source_resolution": "hour",
        }
        for index in range(hours)
    ]
    statistic_id = "sensor.electricity_consumption_ekz_test"
    coordinator = SimpleNamespace(_daily_statistics={statistic_id: [local_day.isoformat()]})

    await _persist_source_resolution(coordinator, statistic_id, rows)

    assert coordinator._daily_statistics[statistic_id] == (
        [local_day.isoformat()] if shifted else []
    )


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("queued", [False, True])
@pytest.mark.parametrize("resolution", ["day", "reconciled_day", "hour"])
@pytest.mark.parametrize("empty_response", [False, True])
async def test_missing_response_day_preserves_recorded_energy(
    monkeypatch, freezer, importer_type, queued, resolution, empty_response
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    freezer.move_to("2026-10-20 12:00:00+00:00")
    missing_start = datetime(2026, 10, 16, tzinfo=ZRH).astimezone(UTC)
    if resolution == "reconciled_day":
        existing = [
            {
                "start": missing_start + timedelta(hours=index),
                "state": 0.25,
                "sum": 101.0 + (index + 1) * 0.25,
            }
            for index in range(24)
        ]
    elif resolution == "hour":
        existing = [
            {"start": missing_start, "state": 1.0, "sum": 102.0},
            {"start": missing_start + timedelta(hours=1), "state": 5.0, "sum": 107.0},
        ]
    else:
        existing = [{"start": missing_start, "state": 6.0, "sum": 107.0}]
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={} if queued else {statistic_id: existing})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 10, 15))
    meta._daily_statistics = {"2026-10-16"} if resolution != "hour" else set()
    meta._recent_statistics = existing if queued else []
    entries = (
        []
        if empty_response
        else [
            (datetime(2026, 10, day, tzinfo=ZRH).astimezone(UTC).isoformat(), 1.0, "VALID")
            for day in (15, 17)
        ]
    )
    monkeypatch.setattr(
        importer, "fetch_data", AsyncMock(return_value=api_series("QUARTER_HOUR", entries))
    )

    result = await importer.import_statistics(Mock(), "825821", datetime(2026, 10, 15), meta, 100.0)

    assert result["statistics"]
    assert sum(row["state"] for row in result["statistics"]) == pytest.approx(
        6.0 if empty_response else 8.0
    )
    assert result["statistics"][-1]["sum"] == pytest.approx(106.0 if empty_response else 108.0)
    restored = [
        row
        for row in result["statistics"]
        if row["start"].astimezone(ZRH).date() == date(2026, 10, 16)
    ]
    assert sum(row["state"] for row in restored) == pytest.approx(6.0)
    assert all(
        row["source_resolution"] == ("hour" if resolution == "hour" else "day") for row in restored
    )
    assert len(result["raw_values"]) == (0 if empty_response else 2)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("queued", [False, True])
async def test_recorded_hour_is_replaced_only_by_complete_hour(
    monkeypatch, freezer, importer_type, queued
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    freezer.move_to("2026-10-20 12:00:00+00:00")
    start = datetime(2026, 10, 16, 12, tzinfo=ZRH).astimezone(UTC)
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    recorded = [{"start": start, "state": 6.0, "sum": 107.0}]
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={} if queued else {statistic_id: recorded})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta._recent_statistics = recorded if queued else []
    fetch = AsyncMock()
    monkeypatch.setattr(importer, "fetch_data", fetch)
    surrounding = [
        (datetime(2026, 10, day, tzinfo=ZRH).astimezone(UTC).isoformat(), 1.0, "VALID")
        for day in (15, 17)
    ]
    for slot_count in (0, 1, 4):
        meta.set_last_import(date(2026, 10, 15))
        entries = surrounding + [
            ((start + timedelta(minutes=15 * index)).isoformat(), 0.5, "VALID")
            for index in range(slot_count)
        ]
        fetch.return_value = api_series("QUARTER_HOUR", entries)

        result = await importer.import_statistics(
            Mock(), "825821", datetime(2026, 10, 15), meta, 100.0
        )

        assert result["statistics"][-1]["sum"] == pytest.approx(104.0 if slot_count == 4 else 108.0)
        assert len(result["raw_values"]) == (6 if slot_count == 4 else 2)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("queued", [False, True])
@pytest.mark.parametrize("daily_response", [False, True])
async def test_end_date_values_include_existing_statistics(
    monkeypatch, importer_type, queued, daily_response
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    date_from = datetime(2026, 10, 15, tzinfo=ZRH)
    date_to = date_from + timedelta(days=1)
    start = date_to.astimezone(UTC)
    existing = [
        {"start": start + timedelta(hours=index), "state": 1.0, "sum": 101.0 + index}
        for index in range(2 if daily_response else 24)
    ]
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"

    async def query(function, hass, begin, end, *args):
        return (
            {}
            if queued
            else {statistic_id: [row for row in existing if begin <= row["start"] < end]}
        )

    recorder = Mock(async_add_executor_job=AsyncMock(side_effect=query))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date_from.date())
    meta._recent_statistics = existing if queued else []
    meta._daily_statistics = set() if daily_response else {date_to.date().isoformat()}
    if daily_response:
        data = api_series("DAY", [(start.isoformat(), 6.0, "VALID")])
    else:
        data = api_series(
            "QUARTER_HOUR",
            [
                ((start + timedelta(hours=12, minutes=15 * index)).isoformat(), 0.5, "VALID")
                for index in range(4)
            ],
        )
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))
    monkeypatch.setattr(
        importer, "calculate_date_range", lambda *args, **kwargs: (date_from, date_to)
    )

    result = await importer.import_statistics(Mock(), "825821", date_from, meta, 100.0)

    assert len(result["statistics"]) == 24
    assert result["statistics"][-1]["sum"] == pytest.approx(106.0 if daily_response else 124.0)
    assert result["statistics"][-1]["end"] == (date_to + timedelta(days=1)).astimezone(UTC)


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("queued", [False, True])
async def test_fuller_partial_hour_adds_backfilled_slots(monkeypatch, importer_type, queued):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    start = datetime(2026, 10, 16, 12, tzinfo=ZRH).astimezone(UTC)
    hour_key = start.strftime("%Y%m%d%H%M%S")
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    existing = [{"start": start, "state": 0.5, "sum": 100.5}]
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={} if queued else {statistic_id: existing})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(start.astimezone(ZRH).date())
    meta._recent_statistics = existing if queued else []
    meta._hourly_slots = {hour_key: {hour_key: 0.5}}
    data = api_series(
        "QUARTER_HOUR",
        [((start + timedelta(minutes=15 * index)).isoformat(), 0.5, "VALID") for index in range(3)],
    )
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

    result = await importer.import_statistics(Mock(), "825821", datetime(2026, 10, 16), meta, 100.0)

    assert result["statistics"][0]["state"] == 1.5
    assert result["statistics"][0]["sum"] == 101.5
    assert len(result["statistics"][0]["slots"]) == 3
    for amounts, expected in [
        ([0.5, 0.5, 0.5], 1.5),
        ([0.25, 0.5, 0.5], 1.25),
        ([0.25], 1.25),
        ([0.5] * 4, 2.0),
    ]:
        meta._hourly_slots = {hour_key: result["statistics"][0]["slots"]}
        if queued:
            meta._recent_statistics = result["statistics"]
        else:
            recorder.async_add_executor_job.return_value = {
                statistic_id: [
                    {
                        "start": start,
                        "state": result["statistics"][0]["state"],
                        "sum": result["statistics"][0]["sum"],
                    }
                ]
            }
        data = api_series(
            "QUARTER_HOUR",
            [
                ((start + timedelta(minutes=15 * index)).isoformat(), amount, "VALID")
                for index, amount in enumerate(amounts)
            ],
        )
        monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

        result = await importer.import_statistics(
            Mock(), "825821", datetime(2026, 10, 16), meta, 100.0
        )

        assert result["statistics"][0]["state"] == expected
        assert result["statistics"][0]["sum"] == 100.0 + expected


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_checkpoint_restores_sum_across_history_gap_longer_than_recorder_lookback(
    importer_type,
):
    from custom_components.ekz_ha import _persist_import_checkpoint, _restore_import_checkpoint
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    statistic_id = (
        f"sensor.electricity_{importer_type(Mock()).get_data_type_name().lower()}_ekz_825821"
    )
    old_meta = EkzMetaEntity(Mock(), "825821")
    old_meta.set_history_cursor(date(2026, 8, 30))
    old = SimpleNamespace()
    await _persist_import_checkpoint(
        old,
        statistic_id,
        old_meta,
        [{"start": datetime(2000, 1, 1, tzinfo=UTC), "state": 2.0, "sum": 102.0}],
    )
    fresh = SimpleNamespace(_import_checkpoints=old._import_checkpoints)
    meta = EkzMetaEntity(Mock(), "825821")
    _restore_import_checkpoint(fresh, statistic_id, meta)

    assert (
        await EkzCoordinator._get_running_sum_before(fresh, statistic_id, date(2026, 8, 30))
        == 102.0
    )


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
async def test_import_checkpoint_restores_pending_and_partial_slots(
    monkeypatch, hass, freezer, importer_type
):
    from homeassistant.helpers.storage import Store

    from custom_components.ekz_ha import _persist_import_checkpoint, _restore_import_checkpoint
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    freezer.move_to("2026-10-20 12:00:00+00:00")
    start = datetime(2026, 10, 16, 12, tzinfo=ZRH).astimezone(UTC)
    hour = start.strftime("%Y%m%d%H%M%S")
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    key = f"ekz_ha.test.{importer.get_data_type_name()}.checkpoint"
    old_meta = EkzMetaEntity(Mock(), "825821")
    old_meta.set_history_cursor(date(2000, 1, 31))
    old_meta._empty_search_days = 120
    old_meta.set_pending(date(2026, 10, 15), 100.0)
    old = SimpleNamespace(_import_checkpoint_store=Store(hass, 1, key))
    rows = [{"start": start, "state": 0.5, "sum": 100.5, "slots": {hour: 0.5}}]
    await _persist_import_checkpoint(old, statistic_id, old_meta, rows)
    fresh = SimpleNamespace(
        _import_checkpoint_store=Store(hass, 1, key),
        api_client=Mock(
            get_consumption_installations=AsyncMock(
                return_value=SimpleNamespace(
                    contracts=[SimpleNamespace(anlage="825821", einzdat="2026-10-01", auszdat=None)]
                )
            ),
            get_production_installations=AsyncMock(return_value=SimpleNamespace(contracts=[])),
            get_production_15min=AsyncMock(return_value=ConsumptionData()),
        ),
    )
    await EkzCoordinator._async_setup(fresh)
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 10, 18))
    _restore_import_checkpoint(fresh, statistic_id, meta)
    assert meta._pending_from == date(2026, 10, 15)
    assert meta._pending_sum_offset == 100.0
    assert meta._hourly_slots == {hour: {hour: 0.5}}
    assert meta._history_cursor == date(2000, 1, 31)
    assert meta._empty_search_days == 120
    meta._empty_search_days = None
    assert (
        await EkzCoordinator._get_running_sum_before(fresh, statistic_id, meta._pending_from)
        == 100.0
    )
    recorder = Mock(
        async_add_executor_job=AsyncMock(
            return_value={statistic_id: [{"start": start.timestamp(), "state": 0.5, "sum": 100.5}]}
        )
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    fetch = AsyncMock(
        return_value=api_series(
            "QUARTER_HOUR",
            [
                ((start + timedelta(minutes=15 * index)).isoformat(), 0.5, "VALID")
                for index in range(3)
            ],
        )
    )
    monkeypatch.setattr(importer, "fetch_data", fetch)

    result = await importer.import_statistics(
        hass, "825821", datetime(2026, 10, 1), meta, meta._pending_sum_offset
    )

    assert fetch.await_args.args[1].date() == date(2026, 10, 15)
    assert result["statistics"][0]["sum"] == 101.5
    await _persist_import_checkpoint(fresh, statistic_id, meta, result["statistics"])
    assert len(fresh._import_checkpoints[statistic_id]["slots"][hour]) == 3


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("response_level", ["DAY", "QUARTER_HOUR", "UNKNOWN"])
@pytest.mark.parametrize("queued", [False, True])
@pytest.mark.parametrize("recorded_row", [False, True])
async def test_restored_slots_are_hourly_for_every_response_level(
    monkeypatch, freezer, importer_type, response_level, queued, recorded_row
):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    freezer.move_to("2026-10-20 12:00:00+00:00")
    start = datetime(2026, 10, 16, 12, tzinfo=ZRH).astimezone(UTC)
    hour = start.strftime("%Y%m%d%H%M%S")
    slot = (start + timedelta(minutes=15)).strftime("%Y%m%d%H%M%S")
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    rows = [{"start": start, "state": 0.5, "sum": 100.5}] if recorded_row else []
    recorder = Mock(
        async_add_executor_job=AsyncMock(return_value={} if queued else {statistic_id: rows})
    )
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(date(2026, 10, 16))
    meta._recent_statistics = rows if queued else []
    meta._hourly_slots = {hour: {slot: 0.5}}
    entries = (
        [(datetime(2026, 10, 17, tzinfo=ZRH).isoformat(), 6.0, "VALID")]
        if response_level == "DAY"
        else []
    )
    data = api_series(response_level, entries)
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

    result = await importer.import_statistics(Mock(), "825821", datetime(2026, 10, 16), meta, 100.0)

    assert all(row["start"].minute == row["start"].second == 0 for row in result["statistics"])
    assert result["statistics"][0]["start"] == start
    assert result["statistics"][0]["slots"] == {slot: 0.5}
    assert result["statistics"][-1]["sum"] == pytest.approx(
        106.5 if response_level == "DAY" else 100.5
    )
    assert [value.timestamp for value in result["raw_values"]] == [slot]


@pytest.mark.parametrize("importer_type", [ConsumptionImporter, ProductionImporter])
@pytest.mark.parametrize("daily", [False, True])
async def test_own_review_checkpoint_replays_unflushed_chunk(
    monkeypatch, hass, freezer, importer_type, daily
):
    from homeassistant.helpers.storage import Store

    from custom_components.ekz_ha import _persist_import_checkpoint, _restore_import_checkpoint
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    freezer.move_to("2026-10-20 12:00:00+00:00")
    local_start = datetime(2026, 10, 16, tzinfo=ZRH)
    start = local_start.astimezone(UTC)
    importer = importer_type(Mock())
    statistic_id = f"sensor.electricity_{importer.get_data_type_name().lower()}_ekz_825821"
    rows = [
        {
            "start": start,
            "end": (local_start + timedelta(days=1)).astimezone(UTC)
            if daily
            else start + timedelta(hours=1),
            "state": 6.0,
            "sum": 106.0,
            "source_resolution": "day" if daily else "hour",
        }
    ]
    key = f"ekz_ha.test.unflushed.{importer.get_data_type_name()}.{daily}"
    old = SimpleNamespace(_import_checkpoint_store=Store(hass, 1, key))
    old_meta = EkzMetaEntity(Mock(), "825821")
    await _persist_import_checkpoint(old, statistic_id, old_meta, rows)
    restarted = SimpleNamespace(_import_checkpoints=await Store(hass, 1, key).async_load())
    meta = EkzMetaEntity(Mock(), "825821")
    meta.set_last_import(local_start.date())
    meta._daily_statistics = {local_start.date().isoformat()} if daily else set()
    _restore_import_checkpoint(restarted, statistic_id, meta)
    recorder = Mock(async_add_executor_job=AsyncMock(return_value={}))
    monkeypatch.setattr(
        "custom_components.ekz_ha.statistics.importer.get_recorder_instance", lambda hass: recorder
    )
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=ConsumptionData()))

    result = await importer.import_statistics(hass, "825821", local_start, meta, 100.0)

    assert result["statistics"][0]["start"] == start
    assert result["statistics"][-1]["sum"] == 106.0
    assert result["statistics"][-1]["end"] == rows[0]["end"]
