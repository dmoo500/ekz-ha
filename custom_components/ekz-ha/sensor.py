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
            EkzPredictionEntity(coordinator, installationId)
            for installationId in coordinator.installations
        ]
        + [
            EkzLastRunningSumEntity(coordinator, installationId)
            for installationId in coordinator.installations
        ]
        + [
            EkzLastFullDayEntity(coordinator, installationId)
            for installationId in coordinator.installations
        ]
        + [
            EkzLastGetAllEntity(coordinator, installationId)
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
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = f"sensor.ekz_electricity_consumption_{installationId}"
        self._attr_name = f"Electricity consumption EKZ {installationId}"

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"


class EkzPredictionEntity(CoordinatorEntity, NumberEntity):
    """Represents the electricity consumption prediction of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzPredictionEntity."""
        super().__init__(coordinator)
        self._attr_device_class = NumberDeviceClass.ENERGY
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = (
            f"sensor.ekz_electricity_consumption_{installationId}_prediction"
        )
        self._attr_name = f"Electricity consumption prediction EKZ {installationId}"

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"


class EkzLastRunningSumEntity(CoordinatorEntity, NumberEntity):
    """Represents the electricity consumption prediction of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzLastRunningSumEntity."""
        super().__init__(coordinator)
        self._attr_device_class = NumberDeviceClass.ENERGY
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_unique_id = (
            f"sensor.ekz_electricity_consumption_{installationId}_internal_last_sum"
        )
        self._attr_name = f"Internal entity for EKZ {installationId}: last running sum"


class EkzLastFullDayEntity(CoordinatorEntity, TextEntity):
    """Represents the electricity consumption prediction of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzLastFullDayEntity."""
        super().__init__(coordinator)
        self._attr_unique_id = (
            f"sensor.ekz_electricity_consumption_{installationId}_internal_last_day"
        )
        self._attr_name = f"Internal entity for EKZ {installationId}: last full day"

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"


class EkzLastGetAllEntity(CoordinatorEntity, TextEntity):
    """Represents the electricity consumption prediction of an EKZ installation."""

    def __init__(
        self, coordinator: DataUpdateCoordinator[str], installationId: str
    ) -> None:
        """Construct an instance of EkzLastGetAllEntity."""
        super().__init__(coordinator)
        self._attr_unique_id = (
            f"sensor.ekz_electricity_consumption_{installationId}_internal_last_all"
        )
        self._attr_name = f"Internal entity for EKZ {installationId}: last get all"

    @property
    def icon(self) -> str:
        """Icon to use in the frontend."""
        return "mdi:lightning-bolt"
