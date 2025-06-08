"""Entities for EKZ installations."""

from homeassistant import core
from homeassistant.components.number import NumberEntity
from homeassistant.components.number.const import NumberDeviceClass
from homeassistant.components.text import TextEntity
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
)

from .const import DOMAIN


async def async_setup_platform(
    hass: core.HomeAssistant, config, async_add_entities, discovery_info=None
):
    """Set up the sensor platform."""
    coordinator = hass.data[DOMAIN]["coordinator"]
    sensors = (
        [
            EkzEntity(coordinator, installationId)
            for installationId in coordinator.installations
        ]
        + [
            EkzLastUpdateEntity(coordinator, installationId)
            for installationId in coordinator.installations
        ]
        + [
            EkzLastUpdateTotalEntity(coordinator, installationId)
            for installationId in coordinator.installations
        ]
    )

    async_add_entities(sensors, True)


class EkzEntity(CoordinatorEntity, NumberEntity):
    """Represents the electricity consumption of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzEntity."""
        super().__init__(coordinator)
        self._attr_device_class = NumberDeviceClass.ENERGY
        self.installationId = installationId

    @property
    def unit_of_measurement(self) -> str:
        """Return the unit of measurement of this entity, if any."""
        return "kWh"

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"

    @property
    def entity_id(self) -> str:
        """Return the entity id of the sensor."""
        return f"input_number.electricity_consumption_ekz_{self.installationId}"

    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return f"Electricity consumption EKZ {self.installationId}"


class EkzLastUpdateTotalEntity(CoordinatorEntity, NumberEntity):
    """Represents an internal state used to archive the electricity consumption of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzLastUpdateTotalEntity."""
        super().__init__(coordinator)
        self.installationId = installationId

    @property
    def unit_of_measurement(self) -> str:
        """Return the unit of measurement of this entity, if any."""
        return "kWh"

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"

    @property
    def entity_id(self) -> str:
        """Return the entity id of the sensor."""
        return f"input_number.electricity_consumption_ekz_{self.installationId}_last_update_total"

    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return f"EKZ {self.installationId} internal state: last total"

    def set_value(self, value: float) -> None:
        """Change the value."""
        self._attr_native_value = value


class EkzLastUpdateEntity(CoordinatorEntity, TextEntity):
    """Represents the electricity consumption of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzLastUpdateEntity."""
        super().__init__(coordinator)
        self.installationId = installationId

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:calendar"

    @property
    def entity_id(self) -> str:
        """Return the entity id of the sensor."""
        return f"input_text.electricity_consumption_ekz_{self.installationId}_last_full_day_update"

    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return f"EKZ {self.installationId} Data Freshness"

    def set_value(self, value: str) -> None:
        """Change the text."""
        self._attr_native_value = value
