"""Data models for EKZ API responses."""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

_LOGGER = logging.getLogger(__name__)


class ValueOrigin(StrEnum):
    API = "api"
    RECORDED = "recorded"
    DAILY_TOTAL = "daily_total"
    DAILY_RECONCILED = "daily_reconciled"

    @property
    def is_daily(self) -> bool:
        return self in (ValueOrigin.DAILY_TOTAL, ValueOrigin.DAILY_RECONCILED)


@dataclass
class ApiValue:
    """Represents a single measurement value from the EKZ API."""

    timestamp: str
    value: float
    status: str
    date: str = ""
    tariff: str = "TOTAL"
    origin: ValueOrigin = ValueOrigin.API

    @classmethod
    def from_dict(cls, data: dict[str, Any], tariff: str = "TOTAL") -> "ApiValue":
        """Create ApiValue from API response dict."""
        ts = str(data["timestamp"])
        if "-" in ts:
            timestamp = datetime.fromisoformat(ts)
        else:
            timestamp = datetime.strptime(ts, "%Y%m%d%H%M%S")
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        date_part = timestamp.astimezone(ZoneInfo("Europe/Zurich")).date().isoformat()

        return cls(
            timestamp=timestamp.astimezone(UTC).strftime("%Y%m%d%H%M%S"),
            value=float(data["value"]),
            status=data.get("status", "VALID"),
            date=data.get("date") or date_part,
            tariff=tariff,
        )


@dataclass
class SeriesData:
    """Represents a time series (HT, NT, or combined) from the API."""

    values: list[ApiValue] = field(default_factory=list)
    level: str = "UNKNOWN"  # DAY, QUARTER_HOUR, etc.
    inactive_timestamps: set[str] = field(default_factory=set)

    @classmethod
    def from_dict(cls, data: dict[str, Any], tariff: str = "TOTAL") -> "SeriesData":
        """Create SeriesData from API response dict."""
        values = [
            ApiValue.from_dict(v, tariff=tariff)
            for v in data.get("values", [])
            if v.get("status") not in ("NOT_AVAILABLE", "MISSING")
        ]
        inactive_timestamps = {
            ApiValue.from_dict(v, tariff=tariff).timestamp
            for v in data.get("values", [])
            if v.get("status") == "NOT_AVAILABLE" and v.get("value") == 0
        }
        return cls(
            values=values,
            level=data.get("level", "UNKNOWN"),
            inactive_timestamps=inactive_timestamps,
        )


@dataclass
class ConsumptionData:
    """Represents consumption/production data from the EKZ API."""

    series_ht: SeriesData | None = None
    series_nt: SeriesData | None = None
    series_total: SeriesData | None = None
    level: str = "UNKNOWN"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConsumptionData":
        """Create ConsumptionData from API response dict."""
        if not data or data == []:
            return cls()

        # Try to get level from top-level or from any series
        level = data.get("level", "UNKNOWN")
        if level == "UNKNOWN":
            for series_key in ("seriesHt", "seriesNt", "series"):
                if series_key in data and data[series_key] and isinstance(data[series_key], dict):
                    level = data[series_key].get("level", "UNKNOWN")
                    if level != "UNKNOWN":
                        break

        series_ht = None
        series_nt = None
        series_total = None

        if "seriesHt" in data and data["seriesHt"]:
            series_ht = SeriesData.from_dict(data["seriesHt"], tariff="HT")
        if "seriesNt" in data and data["seriesNt"]:
            series_nt = SeriesData.from_dict(data["seriesNt"], tariff="NT")
        if "series" in data and data["series"]:
            series_total = SeriesData.from_dict(data["series"], tariff="TOTAL")

        return cls(
            series_ht=series_ht,
            series_nt=series_nt,
            series_total=series_total,
            level=level,
        )

    def get_all_values(self) -> list[ApiValue]:
        """Get all values from all series, sorted by timestamp."""
        values = []
        if self.series_ht:
            values.extend(self.series_ht.values)
        if self.series_nt:
            values.extend(self.series_nt.values)
        if self.series_total and not values:
            # Only use total series if HT/NT are empty
            values.extend(self.series_total.values)
        return sorted(values, key=lambda v: v.timestamp)

    def is_empty(self) -> bool:
        """Check if this response contains no data."""
        return not self.get_all_values()

    def get_import_values(self) -> list[ApiValue]:
        """Require a reading or explicit inactive zero in every supplied tariff."""
        values = self.get_all_values()
        split_series = [series for series in (self.series_ht, self.series_nt) if series is not None]
        if len(split_series) < 2 or not any(series.values for series in split_series):
            return values
        covered = set.intersection(
            *(
                {value.timestamp for value in series.values} | series.inactive_timestamps
                for series in split_series
            )
        )
        import_values = [value for value in values if value.timestamp in covered]
        if len(import_values) != len(values):
            _LOGGER.debug(
                "Excluded %d tariff readings without matching HT/NT coverage",
                len(values) - len(import_values),
            )
        return import_values


@dataclass
class InstallationContract:
    """Represents a contract for an installation."""

    gpart: str
    vkonto: str
    vertrag: str
    anlage: str
    vstelle: str
    einzdat: str | None = None
    auszdat: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InstallationContract":
        """Create InstallationContract from API response dict."""
        return cls(
            gpart=data.get("gpart", ""),
            vkonto=data.get("vkonto", ""),
            vertrag=data.get("vertrag", ""),
            anlage=data.get("anlage", ""),
            vstelle=data.get("vstelle", ""),
            einzdat=data.get("einzdat"),
            auszdat=data.get("auszdat"),
        )


@dataclass
class InstallationSelectionData:
    """Represents the installation selection response."""

    contracts: list[InstallationContract] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InstallationSelectionData":
        """Create InstallationSelectionData from API response dict."""
        if not data or not isinstance(data, dict):
            return cls()

        contracts = [InstallationContract.from_dict(c) for c in data.get("contracts", [])]
        return cls(contracts=contracts)

    def get_installation_ids(self) -> list[str]:
        """Get list of all installation IDs (anlage)."""
        return [c.anlage for c in self.contracts if c.anlage]


@dataclass
class InstallationData:
    """Represents metadata for a specific installation."""

    installation_id: str = ""
    address: str = ""
    meter_number: str = ""
    raw_data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InstallationData":
        """Create InstallationData from API response dict."""
        if not data or not isinstance(data, dict):
            return cls()

        return cls(
            installation_id=data.get("installationId", ""),
            address=data.get("address", ""),
            meter_number=data.get("meterNumber", ""),
            raw_data=data,
        )
