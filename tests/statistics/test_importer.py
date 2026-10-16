"""Tests for shared importer behavior."""

from datetime import date, datetime

from custom_components.ekz_ha.statistics.importer import ZRH, BaseImporter


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
