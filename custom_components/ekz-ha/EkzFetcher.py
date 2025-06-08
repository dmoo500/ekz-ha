"""Interaction with the EKZ API."""

from datetime import datetime, timedelta
import itertools
import zoneinfo

from .session import Session
from .timeutil import format_api_date

ZRH = zoneinfo.ZoneInfo("Europe/Zurich")


def is_dst(dt: datetime, timeZone: zoneinfo.ZoneInfo) -> bool:
    """Determine whether the given date is during daylight savings or not."""
    aware_dt = dt.replace(tzinfo=timeZone)
    return aware_dt.dst() != timedelta(0, 0)


def is_dst_switchover_date(dt: datetime, timeZone: zoneinfo.ZoneInfo) -> bool:
    """Determine whether a day is the day on which daylight savings starts/ends."""
    day_after = dt + timedelta(days=1)
    return is_dst(day_after, timeZone) != is_dst(dt, timeZone)


class EkzFetcher:
    """Fetches data from EKZ."""

    def __init__(self, user: str, password: str) -> None:
        """Construct an instance of EkzFetcher."""
        self.user = user
        self.password = password
        self.session = Session(self.user, self.password)

    async def getInstallations(self) -> list[str]:
        """Return the installation IDs."""
        installations = await self.session.installation_selection_data()
        return [c["anlage"] for c in installations["contracts"]]

    async def fetchNewInstallationData(
        self, installationId: str, last_full_day: str, last_full_day_sum: float
    ) -> dict:
        """Fetch data from the last_full_day onwards for a given installation."""
        from_date = datetime.strptime(last_full_day[0:10], "%Y-%m-%d")
        all_fetched_data = [[]]
        to_date = datetime.now()

        end_of_month = from_date + timedelta(
            days=(32 - from_date.day)
        )  # guaranteed to be in the next month
        end_of_month = end_of_month - timedelta(days=end_of_month.day)
        while from_date <= to_date:
            d = await self.session.get_consumption_data(
                installationId,
                "PK_VERB_15MIN",
                format_api_date(from_date),
                format_api_date(end_of_month),
            )
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
            from_date = end_of_month + timedelta(days=1)
            end_of_month = from_date + timedelta(
                days=32
            )  # guaranteed to be in the next month
            end_of_month = end_of_month - timedelta(days=end_of_month.day)
        values = sorted(
            itertools.chain(*all_fetched_data), key=lambda x: x["timestamp"]
        )
        values = [
            list(g)[0] for _, g in itertools.groupby(values, lambda v: v["timestamp"])
        ]  # deduplicate
        values = sorted(values, key=lambda x: x["timestamp"])

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

        running_sum = last_full_day_sum
        statistics = []
        for value in values:
            statistics.append(
                {
                    "start": datetime.strptime(
                        str(value["timestamp"]), "%Y%m%d%H%M%S"
                    ).astimezone(tz=ZRH),
                    "sum": (running_sum := running_sum + value["value"]),
                    "state": value["value"],
                }
            )
            date = datetime.strptime(value["date"], "%Y-%m-%d")
            if date == max_date:
                last_full_day_sum = running_sum
        last_full_day = max_date
        return {
            "statistics": statistics,
            "last_full_day": last_full_day,
            "last_full_day_sum": last_full_day_sum,
        }

    async def fetchEntireHistory(self, installationId: str) -> dict:
        """Fetch all data from EKZ for a given installation."""
        installations = await self.session.installation_selection_data()
        from_date = None
        for c in installations["contracts"]:
            if c["anlage"] == installationId:
                from_date = c["einzdat"]
        if from_date is None:
            raise ValueError("No matching installation...")
        results = await self.fetchNewInstallationData(installationId, from_date, 0)

        def average(g):
            g = list(g)
            total = sum(x["state"] for x in g)
            return total / len(g)

        averages = {
            key: average(g)
            for key, g in itertools.groupby(
                results["statistics"],
                lambda v: v["start"].month * 100 + v["start"].hour,
            )
        }
        results["averages"] = averages
        return results

    async def fetch(self) -> dict:
        """Fetch new data from EKZ."""
        all_data = {}

        for anlage, values in all_data.items():
            values = [
                v
                for v in values
                if v["status"] != "NOT_AVAILABLE" and v["status"] != "MISSING"
            ]
            values = [
                list(g)[0]
                for _, g in itertools.groupby(values, lambda v: v["timestamp"])
            ]  # deduplicate
            all_data[anlage] = values

        installations = await self.session.installation_selection_data()
        for c in installations["contracts"]:
            from_date = datetime.strptime(c["einzdat"], "%Y-%m-%d")
            to_date = (
                datetime.strptime(c["auszdat"], "%Y-%m-%d")
                if c["auszdat"] is not None
                else datetime.now()
            )

            if c["auszdat"] is not None:
                continue

            existing_values = []
            if c["anlage"] in all_data:
                existing_values = all_data[c["anlage"]]
                existing_values_by_date = {
                    k: len(list(g))
                    for k, g in itertools.groupby(
                        existing_values,
                        lambda v: datetime.strptime(v["date"], "%Y-%m-%d"),
                    )
                }

                while from_date in existing_values_by_date:
                    hours_per_day = (
                        23
                        if is_dst_switchover_date(from_date, ZRH)
                        and from_date.month < 6
                        else 25
                        if is_dst_switchover_date(from_date, ZRH)
                        else 24
                    )
                    expected_measurements = hours_per_day * 4
                    if existing_values_by_date[from_date] < expected_measurements:
                        break
                    from_date = from_date + timedelta(days=1)

            end_of_month = from_date + timedelta(
                days=(32 - from_date.day)
            )  # guaranteed to be in the next month
            end_of_month = end_of_month - timedelta(days=end_of_month.day)

            monthly_data = [existing_values]
            while from_date <= to_date:
                d = await self.session.get_consumption_data(
                    c["anlage"],
                    "PK_VERB_15MIN",
                    format_api_date(from_date),
                    format_api_date(end_of_month),
                )
                monthly_data.append(
                    dict(x, tariff="NT")
                    for x in d["seriesNt"]["values"]
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                )
                monthly_data.append(
                    dict(x, tariff="HT")
                    for x in d["seriesHt"]["values"]
                    if x["status"] != "NOT_AVAILABLE" and x["status"] != "MISSING"
                )
                from_date = end_of_month + timedelta(days=1)
                end_of_month = from_date + timedelta(
                    days=32
                )  # guaranteed to be in the next month
                end_of_month = end_of_month - timedelta(days=end_of_month.day)

            values = sorted(
                itertools.chain(*monthly_data), key=lambda x: x["timestamp"]
            )
            values = [
                list(g)[0]
                for _, g in itertools.groupby(values, lambda v: v["timestamp"])
            ]  # deduplicate
            values = sorted(values, key=lambda x: x["timestamp"])

            def total(group):
                group = list(group)
                return {
                    "value": sum([x["value"] for x in group]),
                    "date": min([x["date"] for x in group]),
                    "time": min([x["time"] for x in group]),
                    "timestamp": min([x["timestamp"] for x in group]),
                }

            values = [
                total(g)
                for _, g in itertools.groupby(
                    values, lambda v: str(v["timestamp"])[0:10]
                )
            ]
            values = sorted(values, key=lambda x: x["timestamp"])
            all_data[c["anlage"]] = values

        return all_data
