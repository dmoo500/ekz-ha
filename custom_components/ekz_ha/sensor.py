"""Entities for EKZ installations."""

from datetime import date, datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant import core
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
)

from .const import DOMAIN

async def async_setup_entry(
    hass: core.HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: Any,
) -> None:
    """Set up EKZ sensors from a config entry."""
    coordinator = hass.data[DOMAIN]["coordinator"]
    meta_entities = {}
    sensors = (
        [EkzEntity(coordinator, installationId) for installationId in coordinator.installations]
        + [EkzPredictionEntity(coordinator, installationId) for installationId in coordinator.installations]
    )
    # Create meta entities and contract-start entities per consumption installation
    for installationId in coordinator.installations:
        meta = EkzMetaEntity(coordinator, installationId)
        meta_entities[installationId] = meta
        sensors.append(meta)
        sensors.append(EkzContractStartEntity(coordinator, installationId))
        sensors.append(EkzNextSyncEntity(coordinator, installationId))
    # Store mapping in coordinator so update loop can access entities by installation ID
    coordinator.meta_entities = meta_entities

    # Production (solar feed-in) entities
    production_meta_entities = {}
    for installationId in getattr(coordinator, "production_installations", {}):
        sensors.append(EkzProductionEntity(coordinator, installationId))
        prod_meta = EkzMetaEntity(coordinator, installationId, model="Solar Meter")
        production_meta_entities[installationId] = prod_meta
        sensors.append(prod_meta)
    coordinator.production_meta_entities = production_meta_entities

    async_add_entities(sensors, True)

class EkzEntity(CoordinatorEntity, SensorEntity):
    """Represents the electricity consumption of an EKZ installation."""
    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = f"ekz_electricity_consumption_{installationId}"
        self._attr_name = f"Electricity consumption EKZ {installationId}"

    @property
    def device_info(self) -> DeviceInfo:
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self) -> None:
        """Statistics are imported directly; sensor state is intentionally always None."""
        return None

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"


class EkzPredictionEntity(CoordinatorEntity, SensorEntity):

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = (
            f"ekz_electricity_consumption_{installationId}_prediction"
        )
        self._attr_name = f"Electricity consumption EKZ {installationId} prediction"

    @property
    def device_info(self) -> DeviceInfo:
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self) -> None:
        """Statistics are imported directly; sensor state is intentionally always None."""
        return None

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"



# Tracks import progress and shows the last successfully imported timestamp as sensor state
class EkzMetaEntity(CoordinatorEntity, SensorEntity):
    def __init__(self, coordinator: DataUpdateCoordinator[str], installationId: str, model: str = "Electricity Meter") -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._model = model
        self._attr_unique_id = f"ekz_electricity_consumption_{installationId}_meta"
        self._attr_name = f"EKZ {installationId} Last Import"
        self._attr_device_class = SensorDeviceClass.TIMESTAMP
        self._last_running_sum = None
        self._last_full_day = None
        self._last_get_all = None
        self._contract_start: date | None = None
        self._last_import: date | None = None
        self._last_run_date: datetime | None = None
        self._pending_from: date | None = None
        self._pending_sum_offset: float = 0.0
        self._received_ranges: list[dict[str, str]] = []

    @property
    def device_info(self) -> DeviceInfo:
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self) -> datetime | None:
        """Return the last successfully imported statistic timestamp."""
        if self._last_import is None:
            return None
        from datetime import timezone
        if isinstance(self._last_import, datetime):
            if self._last_import.tzinfo is None:
                return self._last_import.replace(tzinfo=timezone.utc)
            return self._last_import
        # date → datetime at midnight UTC
        return datetime(
            self._last_import.year,
            self._last_import.month,
            self._last_import.day,
            tzinfo=timezone.utc,
        )

    @property
    def icon(self) -> str:
        return "mdi:information-outline"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {
            "last_running_sum": self._last_running_sum,
            "last_full_day": self._last_full_day,
            "last_get_all": self._last_get_all,
            "contract_start": self._contract_start.isoformat() if self._contract_start else None,
            "last_import": self._last_import.isoformat() if self._last_import else None,
            "last_run_datetime": self._last_run_date.isoformat() if self._last_run_date else None,
            "pending_from": self._pending_from.isoformat() if self._pending_from else None,
            "received_ranges": self._received_ranges,
        }

    def set_last_run_date(self, value: datetime) -> None:
        self._last_run_date = value

    def set_last_running_sum(self, value: float) -> None:
        self._last_running_sum = value

    def set_last_full_day(self, value: datetime) -> None:
        self._last_full_day = value

    def set_last_get_all(self, value: datetime) -> None:
        self._last_get_all = value

    def set_contract_start(self, value: date) -> None:
        self._contract_start = value

    def set_last_import(self, value: date) -> None:
        self._last_import = value

    def set_pending(self, pending_from: date | None, sum_offset: float = 0.0) -> None:
        self._pending_from = pending_from
        self._pending_sum_offset = sum_offset

    def add_received_range(self, start: str, end: str) -> None:
        """Record a date range for which data was received."""
        if not any(r["start"] == start and r["end"] == end for r in self._received_ranges):
            self._received_ranges.append({"start": start, "end": end})
            self._received_ranges.sort(key=lambda x: x["start"])

class EkzContractStartEntity(CoordinatorEntity, SensorEntity):
    """Shows the EKZ contract start date for an installation."""

    def __init__(self, coordinator: DataUpdateCoordinator[str], installationId: str) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_unique_id = f"ekz_contract_start_{installationId}"
        self._attr_name = f"EKZ {installationId} Contract Start"
        self._attr_device_class = SensorDeviceClass.DATE
        self._attr_icon = "mdi:calendar-start"

    @property
    def device_info(self) -> DeviceInfo:
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self) -> date | None:
        """Return the contract start date."""
        meta = getattr(self.coordinator, "meta_entities", {}).get(self.installation_id)
        if meta is None:
            return None
        return meta._contract_start


class EkzProductionEntity(CoordinatorEntity, SensorEntity):
    """Represents the solar/feed-in production of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator, installationId: str
    ) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = f"ekz_electricity_production_{installationId}"
        self._attr_name = f"Electricity production EKZ {installationId}"

    @property
    def device_info(self) -> DeviceInfo:
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Solar Meter",
        }

    @property
    def native_value(self) -> None:
        """Statistics are imported directly; sensor state is intentionally always None."""
        return None

    @property
    def icon(self) -> str:
        return "mdi:solar-power"


class EkzNextSyncEntity(CoordinatorEntity, SensorEntity):
    """Shows when the next data sync with EKZ is scheduled."""

    def __init__(self, coordinator: DataUpdateCoordinator, installationId: str) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_unique_id = f"ekz_next_sync_{installationId}"
        self._attr_name = f"EKZ {installationId} Next Sync"
        self._attr_device_class = SensorDeviceClass.TIMESTAMP
        self._attr_icon = "mdi:clock-outline"

    @property
    def device_info(self) -> DeviceInfo:
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self) -> datetime | None:
        """Return the next scheduled sync time."""
        return getattr(self.coordinator, "next_update_time", None)