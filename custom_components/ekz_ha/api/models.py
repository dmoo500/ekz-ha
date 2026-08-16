"""Data models for EKZ API responses."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ApiValue:
    """Represents a single measurement value from the EKZ API."""

    timestamp: str
    value: float
    status: str
    date: str = ""
    tariff: str = "TOTAL"

    @classmethod
    def from_dict(cls, data: dict[str, Any], tariff: str = "TOTAL") -> "ApiValue":
        """Create ApiValue from API response dict."""
        # Extract date from timestamp (YYYY-MM-DD from YYYYMMDDHHmmss or YYYY-MM-DD HH:mm:ss)
        ts = str(data["timestamp"])
        if "-" in ts:
            date_part = ts.split(" ")[0] if " " in ts else ts.split("T")[0]
        else:
            # Format: YYYYMMDDHHmmss
            date_part = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"

        return cls(
            timestamp=data["timestamp"],
            value=float(data["value"]),
            status=data.get("status", "VALID"),
            date=date_part,
            tariff=tariff,
        )


@dataclass
class SeriesData:
    """Represents a time series (HT, NT, or combined) from the API."""

    values: list[ApiValue] = field(default_factory=list)
    level: str = "UNKNOWN"  # DAY, QUARTER_HOUR, etc.

    @classmethod
    def from_dict(cls, data: dict[str, Any], tariff: str = "TOTAL") -> "SeriesData":
        """Create SeriesData from API response dict."""
        values = [
            ApiValue.from_dict(v, tariff=tariff)
            for v in data.get("values", [])
            if v.get("status") not in ("NOT_AVAILABLE", "MISSING")
        ]
        return cls(values=values, level=data.get("level", "UNKNOWN"))


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
