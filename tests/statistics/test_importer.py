"""Tests for shared importer behavior."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.ekz_ha import EkzCoordinator, statistics_during_period
from custom_components.ekz_ha.api.models import ApiValue, ConsumptionData, SeriesData
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


def test_calculate_date_range_rechecks_last_imported_day():
    importer = StubImporter(None)
    last_import = date(2026, 9, 30)

    from_date, _ = importer.calculate_date_range(last_import, date(2026, 9, 1), max_days=1)

    assert from_date.date() == last_import


def test_calculate_date_range_limits_history_to_ten_years():
    importer = StubImporter(None)
    today = datetime.now(tz=ZRH).date()
    try:
        expected_start = today.replace(year=today.year - 10)
    except ValueError:
        expected_start = today.replace(year=today.year - 10, day=28)

    from_date, _ = importer.calculate_date_range(date(2000, 1, 1), date(2000, 1, 1))

    assert from_date.date() == expected_start


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
            daily.status = "DAILY_VALID"
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
async def test_issue20_consecutive_updates_do_not_read_stale_offset(
    monkeypatch, freezer, entity_type, recorder_flushed
):
    freezer.move_to("2026-10-18 12:00:00+00:00")
    installation_id = "825821"
    statistic_id = f"sensor.electricity_{entity_type}_ekz_{installation_id}"
    first_day = date(2026, 10, 15)
    meta = SimpleNamespace(_last_import=first_day, _contract_start=first_day)
    info = {installation_id: {"contract_start": "2026-10-15"}}
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
    )
    coordinator._get_running_sum_before = MethodType(
        EkzCoordinator._get_running_sum_before, coordinator
    )
    persisted_start = datetime(2026, 10, 14, 23, tzinfo=ZRH).astimezone(UTC)
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

    recorder = Mock(async_add_executor_job=AsyncMock(side_effect=recorder_query))
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
        starts = (
            [second_day_start - timedelta(hours=1), second_day_start]
            if call_count == 0
            else [second_day_start, second_day_start + timedelta(hours=1)]
        )
        call_count += 1
        metadata._last_import = date(2026, 10, 16)
        return {
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
    assert queued_imports[0][-1]["sum"] == pytest.approx(1020.0)
    assert queued_imports[1][0]["sum"] == pytest.approx(queued_imports[0][-1]["sum"])


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
async def test_daily_fallback_preserves_existing_hourly_rows(monkeypatch, mixed, recorder_flushed):
    from custom_components.ekz_ha.sensor import EkzMetaEntity

    start = datetime(2026, 10, 15, tzinfo=ZRH).astimezone(UTC)
    existing = [
        {"start": start, "state": 1.0, "sum": 101.0},
        {"start": start + timedelta(hours=1), "state": 2.0, "sum": 103.0},
    ]
    statistic_id = "sensor.electricity_consumption_ekz_825821"
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
        values[0].status = "DAILY_VALID"
        values.append(
            ApiValue.from_dict(
                {"timestamp": (start + timedelta(days=1)).strftime("%Y%m%d%H%M%S"), "value": 0.25}
            )
        )
    level = "QUARTER_HOUR" if mixed else "DAY"
    data = ConsumptionData(series_total=SeriesData(values=values, level=level), level=level)
    importer = ConsumptionImporter(Mock())
    monkeypatch.setattr(importer, "fetch_data", AsyncMock(return_value=data))

    result = await importer.import_statistics(Mock(), "825821", datetime(2026, 10, 15), meta, 100.0)

    assert [row["sum"] for row in result["statistics"][:2]] == [101.0, 103.0]
    assert result["statistics"][-1]["sum"] == (103.25 if mixed else 103.0)
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
