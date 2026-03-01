"""Strong types for the EKZ API."""

from typing import TypedDict


class Address(TypedDict):
    """Represents the address associated with an installation."""

    addressNumber: str
    street: str
    houseNumber: str
    houseNumberDetails: str
    locationDetails: str
    floor: str
    postalCode: str
    city: str


class ISDContract(TypedDict):
    """Used for the contracts property in InstallationSelectionData."""

    gpart: str
    vkonto: str
    vertrag: str
    anlage: str
    vstelle: str
    haus: str
    einzdat: str
    auszdat: str | None
    sparte: str


class ISDAnlage(TypedDict):
    """Used for the eanl property in InstallationSelectionData."""

    anlage: str
    sparte: str
    vstelle: str
    anlart: str
    spebene: str
    zzenergietraeger: str | None
    zzevgstat: str | None
    zzevganlage: str | None
    eanlhTariftyp: str
    eanlhAbleinh: str


class ISDStelle(TypedDict):
    """Who knows what this is, but we don't explicitly need it."""

    vstelle: str
    haus: str
    eigent: str
    vbsart: str
    lgzusatz: str
    floor: str
    zzlage: str
    zzlgzusatz: str
    iflotZzanobjart: str
    iflotZzeigen: str
    iflotZzegid: str
    address: Address


class ISDFkkVkp(TypedDict):
    """Who knows what this is, but we don't explicitly need it."""

    vkont: str
    gpart: str
    opbuk: str
    stdbk: str
    abrwe: str | None
    abwra: str | None
    abwma: str | None
    ebvty: str | None
    abvty: str
    ezawe: str
    azawe: str
    vkpbz: str
    ktokl: str
    consolidatorId: str
    zzRechDet: str


class InstallationSelectionData(TypedDict):
    """Return schema for /consumption-view/v1/installation-selection-data?installationVariant=CONSUMPTION."""

    contracts: list[ISDContract]
    eanl: list[ISDAnlage]
    evbs: list[ISDStelle]
    fkkvkp: list[ISDFkkVkp]
    commonData: str | None


class IDProperty(TypedDict):
    """Who knows what this is, but we don't explicitly need it."""

    property: str
    ab: str
    bis: str


class InstallationData(TypedDict):
    """Return schema for /consumption-view/v1/installation-data?installationId=..."""

    status: list[IDProperty]


class Value(TypedDict):
    """Consumption data."""

    value: float
    timestamp: int
    date: str
    time: str
    status: str


class Series(TypedDict):
    """Consumption data during the specified time period."""

    level: str
    energyType: str | None
    sourceType: str | None
    tariffType: str
    ab: str
    bis: str
    values: list[Value]


class ConsumptionData(TypedDict):
    """Return schema for /consumption-view/v1/consumption-data?installationId=..."""

    series: Series | None
    seriesHt: Series | None
    seriesNetz: Series | None
    seriesNetzHt: Series | None
    seriesNetzHt: Series | None
    seriesNt: Series | None
