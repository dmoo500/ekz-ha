from .session import Session
import json
import itertools
from datetime import datetime, timedelta
from .timeutil import format_api_date
import pytz

ZRH = pytz.timezone("Europe/Zurich")


def is_dst(dt, timeZone) -> bool:
    aware_dt = timeZone.localize(dt)
    return aware_dt.dst() != timedelta(0, 0)


def is_dst_switchover_date(dt, timeZone) -> bool:
    day_after = dt + timedelta(days=1)
    return is_dst(day_after, timeZone) != is_dst(dt, timeZone)


class EkzFetcher:
    def __init__(self, user: str, password: str):
        """Construct an instance of EkzFetcher."""
        self.user = user
        self.password = password
        self.session = Session(self.user, self.password, login_immediately=True)

    async def getInstallations(self) -> list:
        installations = await self.session.installation_selection_data()
        return [c["anlage"] for c in installations["contracts"]]

    async def fetch(self) -> dict:
        """Fetch new data from EKZ."""
        all_data = {}
        # try:
        #     with open("data.json", "r") as file:
        #         all_data = json.load(file)
        # except:
        #     pass

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
            # address = "N/A"
            # for s in installations["evbs"]:
            #     if s["vstelle"] == c["vstelle"]:
            #         address = (
            #             f"{s['address']['street']} {s['address']['houseNumber']}, "
            #             f"{s['address']['postalCode']} {s['address']['city']}"
            #         )
            #         break
            # Get data for this Anlage
            # year = datetime.strptime(c["einzdat"], "%Y-%m-%d").year
            # month = datetime.strptime(c["einzdat"], "%Y-%m-%d").month
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

            def total(key, group):
                group = list(group)
                t = {
                    "value": sum([x["value"] for x in group]),
                    "date": min([x["date"] for x in group]),
                    "time": min([x["time"] for x in group]),
                    "timestamp": min([x["timestamp"] for x in group]),
                }
                return t

            values = [
                total(key, g)
                for key, g in itertools.groupby(
                    values, lambda v: str(v["timestamp"])[0:10]
                )
            ]
            values = sorted(values, key=lambda x: x["timestamp"])
            all_data[c["anlage"]] = values

        # with open("data.json", "w") as file:
        #     json.dump(all_data, file)
        # print(all_data)
        # pass
        return all_data
