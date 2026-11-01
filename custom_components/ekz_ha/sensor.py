"""Entities for EKZ installations."""

from datetime import date, datetime
from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
)

from .const import DOMAIN
from .import_status import ImportStatus, get_import_status
from .statistics.checkpoint import _restore_energy_import_checkpoints


def _remove_legacy_energy_entities(hass: HomeAssistant, coordinator: Any) -> None:
    """Remove obsolete entities only after their statistic IDs have been saved."""
    registry = er.async_get(hass)
    unique_ids = {
        f"ekz_electricity_consumption_{key}{suffix}"
        for key in coordinator.installations
        for suffix in ("", "_prediction")
    } | {f"ekz_electricity_production_{key}" for key in coordinator.production_installations}
    for migration in coordinator._external_migrations.values():
        for entity_id in migration["sources"]:
            entity = registry.async_get(entity_id)
            if entity is not None and entity.platform == DOMAIN and entity.unique_id in unique_ids:
                registry.async_remove(entity_id)


def _migrate_production_meta_entities(hass: HomeAssistant, coordinator: Any) -> None:
    registry = er.async_get(hass)
    for installation_id in getattr(coordinator, "production_installations", {}):
        if installation_id in coordinator.installations:
            continue
        old_unique_id = f"ekz_electricity_consumption_{installation_id}_meta"
        new_unique_id = f"ekz_electricity_production_{installation_id}_meta"
        old_entity_id = registry.async_get_entity_id("sensor", DOMAIN, old_unique_id)
        new_entity_id = registry.async_get_entity_id("sensor", DOMAIN, new_unique_id)
        if old_entity_id is not None and new_entity_id is None:
            registry.async_update_entity(old_entity_id, new_unique_id=new_unique_id)


async def async_setup_entry(hass, entry, async_add_entities):
    """Set up EKZ sensors from a config entry."""
    coordinator = hass.data[DOMAIN]["coordinator"]
    _remove_legacy_energy_entities(hass, coordinator)
    _migrate_production_meta_entities(hass, coordinator)
    meta_entities = {}
    sensors: list[SensorEntity] = []

    # Create meta entities and contract-start entities per consumption installation
    for installationId in coordinator.installations:
        meta = EkzMetaEntity(coordinator, installationId)
        meta_entities[installationId] = meta
        sensors.append(meta)
        sensors.append(EkzImportStatusEntity(coordinator, meta, "consumption"))
        sensors.append(EkzContractStartEntity(coordinator, installationId))
        sensors.append(EkzNextSyncEntity(coordinator, installationId))
    # Store mapping in coordinator so update loop can access entities by installation ID
    coordinator.meta_entities = meta_entities

    # Production (solar feed-in) entities
    production_meta_entities = {}
    for installationId in getattr(coordinator, "production_installations", {}):
        prod_meta = EkzMetaEntity(coordinator, installationId, model="Solar Meter")
        production_meta_entities[installationId] = prod_meta
        sensors.append(prod_meta)
        sensors.append(EkzImportStatusEntity(coordinator, prod_meta, "production"))
    coordinator.production_meta_entities = production_meta_entities

    _restore_energy_import_checkpoints(coordinator)
    async_add_entities(sensors, False)


# Tracks import progress and shows the last successfully imported timestamp as sensor state
class EkzMetaEntity(CoordinatorEntity, SensorEntity):
    def __init__(
        self,
        coordinator: DataUpdateCoordinator[str],
        installationId: str,
        model: str = "Electricity Meter",
    ) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._model = model
        is_production = model == "Solar Meter"
        entity_type = "production" if is_production else "consumption"
        label = "Production" if is_production else "Consumption"
        self._attr_unique_id = f"ekz_electricity_{entity_type}_{installationId}_meta"
        self._attr_name = f"EKZ {installationId} Last Import {label}"
        self._attr_device_class = SensorDeviceClass.TIMESTAMP
        self._last_running_sum = None
        self._last_full_day = None
        self._last_get_all = None
        self._contract_start: date | None = None
        self._last_import: date | None = None
        self._last_successful_import: datetime | None = None
        self._import_status = ImportStatus()
        self._diagnostic_entities: list[SensorEntity] = [self]
        self._diagnostic_coordinator_error: object = object()
        self._history_cursor: date | None = None
        self._empty_search_days: int | None = None
        self._empty_search_from: date | None = None
        self._recent_statistics: list[dict[str, Any]] = []
        self._daily_statistics: set[str] = set()
        self._hourly_slots: dict[str, dict[str, float]] = {}
        self._checkpoint_restored = False
        self._last_run_date: datetime | None = None
        self._pending_from: date | None = None
        self._pending_sum_offset: float = 0.0

    def async_publish_import_status(self) -> None:
        # A previous cycle's exception must not hide this attempt's local progress.
        self._diagnostic_coordinator_error = self.coordinator.last_exception
        for entity in self._diagnostic_entities:
            if entity.hass is not None and entity.entity_id is not None:
                entity.async_write_ha_state()

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": self._model,
        }

    @property
    def native_value(self):
        """Return the last successfully imported statistic timestamp."""
        return self._last_successful_import

    @property
    def available(self) -> bool:
        """Retain the last successful import date when an update fails."""
        return True

    @property
    def icon(self) -> str:
        return "mdi:information-outline"

    @property
    def extra_state_attributes(self):
        return {
            "last_running_sum": self._last_running_sum,
            "last_full_day": self._last_full_day,
            "last_get_all": self._last_get_all,
            "contract_start": self._contract_start.isoformat() if self._contract_start else None,
            "last_import_date": self._last_import.isoformat() if self._last_import else None,
            "history_cursor": self._history_cursor.isoformat() if self._history_cursor else None,
            "empty_search_from": (
                self._empty_search_from.isoformat() if self._empty_search_from else None
            ),
            "last_run_datetime": self._last_run_date.isoformat() if self._last_run_date else None,
            "pending_from": self._pending_from.isoformat() if self._pending_from else None,
        }

    def set_last_run_date(self, value):
        self._last_run_date = value

    def set_last_running_sum(self, value):
        self._last_running_sum = value

    def set_last_full_day(self, value):
        self._last_full_day = value

    def set_last_get_all(self, value):
        self._last_get_all = value

    def set_contract_start(self, value: date):
        self._contract_start = value

    def set_last_import(self, value: date | None):
        self._last_import = value

    def set_history_cursor(self, value: date | None) -> None:
        self._history_cursor = value

    def set_pending(self, pending_from: "date | None", sum_offset: float = 0.0):
        self._pending_from = pending_from
        self._pending_sum_offset = sum_offset


class EkzImportStatusEntity(CoordinatorEntity, SensorEntity):
    """Expose import diagnostics independently of coordinator availability."""

    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = ["waiting", "searching", "importing", "current", "failed"]
    _attr_has_entity_name = True
    _attr_icon = "mdi:database-sync"

    def __init__(self, coordinator, meta: EkzMetaEntity, import_type: str) -> None:
        super().__init__(coordinator)
        self._meta = meta
        meta._diagnostic_entities.append(self)
        self._attr_unique_id = f"ekz_electricity_{import_type}_{meta.installation_id}_import_status"
        self._attr_translation_key = f"import_status_{import_type}"

    @property
    def device_info(self):
        return self._meta.device_info

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self) -> str:
        status = get_import_status(self._meta)
        if (
            not self.coordinator.last_update_success
            and self.coordinator.last_exception is not self._meta._diagnostic_coordinator_error
        ):
            return "failed"
        return status.state

    @property
    def extra_state_attributes(self):
        attributes = get_import_status(self._meta).as_attributes()
        next_update = getattr(self.coordinator, "next_update_time", None)
        attributes["next_update"] = (
            next_update.isoformat()
            if next_update and self.coordinator.last_update_success
            else None
        )
        if not self.coordinator.last_update_success:
            attributes["coordinator_error_type"] = type(self.coordinator.last_exception).__name__
        return attributes


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
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self):
        """Return the contract start date."""
        meta = getattr(self.coordinator, "meta_entities", {}).get(self.installation_id)
        if meta is None:
            return None
        return meta._contract_start


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
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Electricity Meter",
        }

    @property
    def native_value(self):
        """Return the next scheduled sync time."""
        return getattr(self.coordinator, "next_update_time", None)
