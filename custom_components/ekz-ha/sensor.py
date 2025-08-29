"""Entities for EKZ installations."""

from datetime import date, datetime

from homeassistant import core
from homeassistant.components.date import DateEntity
from homeassistant.components.number import NumberEntity
from homeassistant.components.number.const import NumberDeviceClass
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
)

from .const import DOMAIN

async def async_setup_entry(hass, entry, async_add_entities):
    """Set up EKZ sensors from a config entry."""
    coordinator = hass.data[DOMAIN]["coordinator"]
    meta_entities = {}
    sensors = (
        [EkzEntity(coordinator, installationId) for installationId in coordinator.installations]
        + [EkzPredictionEntity(coordinator, installationId) for installationId in coordinator.installations]
    )
    # Meta-Entities erzeugen und Mapping pflegen
    for installationId in coordinator.installations:
        meta = EkzMetaEntity(coordinator, installationId)
        meta_entities[installationId] = meta
        sensors.append(meta)
    # Mapping im Coordinator speichern
    coordinator.meta_entities = meta_entities
    async_add_entities(sensors, True)

class EkzEntity(CoordinatorEntity, NumberEntity):
    """Represents the electricity consumption of an EKZ installation."""
    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_device_class = NumberDeviceClass.ENERGY
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = f"sensor.ekz_electricity_consumption_{installationId}"
        self._attr_name = f"Electricity consumption EKZ {installationId}"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Stromzähler",
        }

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"


class EkzPredictionEntity(CoordinatorEntity, NumberEntity):

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_device_class = NumberDeviceClass.ENERGY
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = (
            f"sensor.ekz_electricity_consumption_{installationId}_prediction"
        )
        self._attr_name = f"Electricity consumption prediction EKZ {installationId}"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Stromzähler",
        }

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"



# Neue Meta-Entity für interne Statusdaten
class EkzMetaEntity(CoordinatorEntity):
    def __init__(self, coordinator: DataUpdateCoordinator[str], installationId: str) -> None:
        super().__init__(coordinator)
        self.installation_id = installationId
        self._attr_unique_id = f"sensor.ekz_electricity_consumption_{installationId}_meta"
        self._attr_name = f"EKZ {installationId} Meta"
        self._last_running_sum = None
        self._last_full_day = None
        self._last_get_all = None
        self._contract_start = None  # type: date | None
        self._last_import = None    # type: date | None
        self._last_run_date: datetime | None = None

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, f"ekz_{self.installation_id}")},
            "name": f"EKZ {self.installation_id}",
            "manufacturer": "EKZ",
            "model": "Stromzähler",
        }

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
            "last_run_datetime": self._last_run_date.isoformat() if self._last_run_date else None,
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

    def set_last_import(self, value: date):
        self._last_import = value
    