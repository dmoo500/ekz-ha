"""Authentication failures and replacement-credential flow."""

from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import ConfigEntryAuthFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ekz_ha import EkzCoordinator, _async_import_chunk
from custom_components.ekz_ha.api.models import InstallationContract, InstallationSelectionData
from custom_components.ekz_ha.api.session import EkzAuthenticationError
from custom_components.ekz_ha.const import DOMAIN
from custom_components.ekz_ha.sensor import EkzMetaEntity
from custom_components.ekz_ha.statistics.production import ProductionImporter


@pytest.mark.parametrize("stage", ["consumption", "production", "probe", "import"])
async def test_authentication_errors_trigger_ha_auth_failure(hass, monkeypatch, stage):
    error = EkzAuthenticationError("rejected")
    client = Mock(
        get_consumption_installations=AsyncMock(
            return_value=InstallationSelectionData(
                contracts=[
                    InstallationContract.from_dict({"anlage": "test", "einzdat": "2026-01-01"})
                ]
            )
        ),
        get_production_installations=AsyncMock(return_value=InstallationSelectionData()),
        get_production_15min=AsyncMock(side_effect=error),
    )
    entry = Mock(entry_id=f"auth-{stage}")
    coordinator = EkzCoordinator(hass, client, timedelta(hours=4), entry)
    if stage in ("consumption", "production"):
        getattr(client, f"get_{stage}_installations").side_effect = error
    if stage == "import":
        from datetime import UTC, datetime

        importer = ProductionImporter(client)
        meta = EkzMetaEntity(coordinator, "test")
        recorder = Mock(async_block_till_done=AsyncMock())
        monkeypatch.setattr(
            "custom_components.ekz_ha.get_recorder_instance", lambda _hass: recorder
        )
        with pytest.raises(ConfigEntryAuthFailed) as raised:
            await _async_import_chunk(
                coordinator,
                importer,
                "test",
                datetime(2026, 1, 1, tzinfo=UTC),
                meta,
                "sensor.electricity_production_ekz_test",
                0.0,
            )
        assert meta._import_status.error_type == "EkzAuthenticationError"
    else:
        with pytest.raises(ConfigEntryAuthFailed) as raised:
            await coordinator._async_setup()
    assert raised.value.__cause__ is error


async def test_reauthentication_updates_existing_entry_and_reloads(
    hass, enable_custom_integrations, monkeypatch
):
    import custom_components

    monkeypatch.setattr(
        custom_components, "__path__", [str(Path(__file__).parent.parent / "custom_components")]
    )
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=DOMAIN,
        data={
            "user": "test",
            "password": "old",
            "totp_secret": "JBSWY3DPEHPK3PXP",
            "scan_interval": 1200,
        },
    )
    entry.add_to_hass(hass)
    client = Mock(
        get_consumption_installations=AsyncMock(
            return_value=InstallationSelectionData(
                contracts=[InstallationContract.from_dict({"anlage": "test"})]
            )
        ),
        close=AsyncMock(),
    )
    with (
        patch("custom_components.ekz_ha.config_flow.EkzApiClient", return_value=client),
        patch.object(hass.config_entries, "async_reload", new_callable=AsyncMock) as reload,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": config_entries.SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=entry.data,
        )
        assert result["type"] is FlowResultType.FORM
        assert result["step_id"] == "user"
        credentials = {
            "user": "test",
            "password": "replacement",
            "totp_secret": "JBSWY3DPEHPK3PXP",
            "device_name": "",
        }
        result = await hass.config_entries.flow.async_configure(result["flow_id"], credentials)
        assert result["step_id"] == "confirm"
        client.get_consumption_installations.side_effect = EkzAuthenticationError("bad password")
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        assert result["errors"] == {"base": "invalid_auth"}
        assert entry.data["password"] == "old"
        client.get_consumption_installations.side_effect = None
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})
        await hass.async_block_till_done()
        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == "reauth_successful"
        assert entry.data == {**credentials, "scan_interval": 1200}
        assert len(hass.config_entries.async_entries(DOMAIN)) == 1
        reload.assert_awaited_once_with(entry.entry_id)
        assert client.close.await_count == 2
