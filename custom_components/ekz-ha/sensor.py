"""Entities for EKZ installations."""

from homeassistant import core
from homeassistant.components.number import NumberEntity
from homeassistant.components.number.const import NumberDeviceClass
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
    sensors = [
        EkzEntity(coordinator, installationId)
        for installationId in coordinator.installations
    ] + [
        EkzPredictionEntity(coordinator, installationId)
        for installationId in coordinator.installations
    ]

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


class EkzPredictionEntity(CoordinatorEntity, NumberEntity):
    """Represents the electricity consumption prediction of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzPredictionEntity."""
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
        return (
            f"input_number.electricity_consumption_ekz_{self.installationId}_prediction"
        )

    @property
    def name(self) -> str:
        """Return the name of the sensor."""
        return f"Electricity consumption prediction EKZ {self.installationId}"
