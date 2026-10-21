"""Reconcile daily totals with recorded hourly detail."""

from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

from ..api.models import ApiValue, ValueOrigin
from .validator import day_bounds, day_slots


class DataReconciler:
    """Pure calculations for changes in measurement resolution."""

    @staticmethod
    def reconcile_hourly_values(
        values: list[ApiValue],
        rows: dict[datetime, dict[str, Any]],
        statistic_id: str,
        known_slots: dict[str, dict[str, float]] | None = None,
    ) -> list[ApiValue]:
        reconciled = {value.timestamp: value for value in values}
        for timestamp, row in sorted(rows.items()):
            slots = {
                (timestamp + timedelta(minutes=15 * index)).strftime("%Y%m%d%H%M%S")
                for index in range(4)
            }
            if slots.issubset(reconciled):
                continue
            hour_key = timestamp.strftime("%Y%m%d%H%M%S")
            previous_slots = row.get("slots", (known_slots or {}).get(hour_key))
            if previous_slots and len(previous_slots) < 4:
                for slot, value in previous_slots.items():
                    if slot in slots:
                        reconciled.setdefault(
                            slot, ApiValue.from_dict({"timestamp": slot, "value": value})
                        )
                continue
            if row.get("state") is None:
                raise ValueError(f"Cannot restore statistics without state for {statistic_id}")
            for slot in slots:
                reconciled.pop(slot, None)
            restored = ApiValue.from_dict(
                {"timestamp": timestamp.strftime("%Y%m%d%H%M%S"), "value": row["state"]}
            )
            reconciled[restored.timestamp] = replace(restored, origin=ValueOrigin.RECORDED)
        return sorted(reconciled.values(), key=lambda value: value.timestamp)

    @staticmethod
    def restore_daily_values(
        value: ApiValue,
        rows: dict[datetime, dict[str, Any]],
        statistic_id: str,
    ) -> list[ApiValue]:
        if not rows:
            raise ValueError(f"Cannot restore daily statistics for {statistic_id} on {value.date}")
        start, _ = day_bounds(value.date)
        single_daily = len(rows) == 1 and start in rows
        if not single_daily and set(rows) != set(day_slots(value.date, minutes=60)):
            raise ValueError(
                f"Incomplete stored daily statistics for {statistic_id} on {value.date}"
            )
        restored = []
        for timestamp, row in sorted(rows.items()):
            if row.get("state") is None:
                raise ValueError(
                    f"Cannot restore daily statistics without state for {statistic_id}"
                )
            restored.append(
                replace(
                    value,
                    timestamp=timestamp.strftime("%Y%m%d%H%M%S"),
                    value=float(row["state"]),
                    origin=ValueOrigin.DAILY_TOTAL
                    if single_daily
                    else ValueOrigin.DAILY_RECONCILED,
                )
            )
        return restored

    @staticmethod
    def reconcile_daily_total(
        value: ApiValue,
        rows: dict[datetime, dict[str, Any]],
        statistic_id: str,
    ) -> list[ApiValue]:
        hourly_values: dict[datetime, float] = {}
        for timestamp, row in sorted(rows.items()):
            if row.get("state") is None:
                raise ValueError(f"Cannot preserve statistics without state for {statistic_id}")
            hourly_values[timestamp] = float(row["state"])
        hours = day_slots(value.date, minutes=60)
        missing = [hour for hour in hours if hour not in hourly_values]
        recorded_total = sum(hourly_values.values())
        if missing and value.value >= recorded_total:
            remainder = (value.value - recorded_total) / len(missing)
            hourly_values.update(dict.fromkeys(missing, remainder))
        elif recorded_total:
            factor = value.value / recorded_total
            hourly_values = {hour: hourly_values.get(hour, 0.0) * factor for hour in hours}
        else:
            hourly_values = {hour: value.value / len(hours) for hour in hours}
        hourly_values[hours[-1]] += value.value - sum(hourly_values.values())
        return [
            replace(
                value,
                timestamp=timestamp.strftime("%Y%m%d%H%M%S"),
                value=hourly_values[timestamp],
                origin=ValueOrigin.DAILY_RECONCILED,
            )
            for timestamp in hours
        ]
