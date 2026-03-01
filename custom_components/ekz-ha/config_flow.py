"""Config flow for EKZ integration."""

import logging

import voluptuous as vol

from homeassistant import config_entries

from .const import DOMAIN
from .session import Session

_LOGGER = logging.getLogger(__name__)

DATA_SCHEMA = vol.Schema(
    {
        vol.Required("user"): str,
        vol.Required("password"): str,
    }
)


class EkzConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Ekz config flow."""

    VERSION = 1
    MINOR_VERSION = 1

    async def async_step_user(self, user_input):
        """Configure EKZ login."""
        errors = {}

        if user_input is not None:
            session = Session(user_input["user"], user_input["password"], user_input.get("totp_secret"))
            try:
                data = await session.installation_selection_data()
                contracts = data.get("contracts") if isinstance(data, dict) else None
                if not contracts:
                    _LOGGER.warning(
                        "[config_flow] Login succeeded but no contracts found for user %s",
                        user_input["user"],
                    )
                    errors["base"] = "no_contracts"
                else:
                    return self.async_create_entry(title="EKZ", data=user_input)
            except ValueError as e:
                msg = str(e)
                _LOGGER.warning("[config_flow] Login failed: %s", msg)
                if "2FA" in msg:
                    errors["base"] = "totp_required"
                elif "SMS" in msg:
                    errors["base"] = "sms_2fa_not_supported"
                elif "TOTP code was rejected" in msg:
                    errors["base"] = "invalid_totp"
                elif "maintenance" in msg.lower():
                    errors["base"] = "ekz_maintenance"
                else:
                    errors["base"] = "invalid_auth"
            except Exception as e:  # noqa: BLE001
                _LOGGER.exception("[config_flow] Unexpected error during login: %s", e)
                errors["base"] = "cannot_connect"
            finally:
                await session._reset_session()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required("user", default=(user_input or {}).get("user", "")): str,
                    vol.Required("password"): str,
                    vol.Optional("totp_secret", default=(user_input or {}).get("totp_secret", "")): str,
                }
            ),
            errors=errors,
        )
