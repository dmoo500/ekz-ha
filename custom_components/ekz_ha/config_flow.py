"""Config flow for EKZ integration."""

import logging
from typing import Any

import pyotp
import voluptuous as vol

from homeassistant import config_entries

from .const import DOMAIN
from .session import Session

_LOGGER = logging.getLogger(__name__)


class EkzConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Ekz config flow."""

    VERSION = 1
    MINOR_VERSION = 1

    def __init__(self) -> None:
        self._credentials: dict[str, Any] = {}

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Step 1: collect credentials and TOTP secret, then show generated OTP."""
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()

        errors = {}

        if user_input is not None:
            totp_secret = (user_input.get("totp_secret") or "").strip().replace(" ", "")
            if not totp_secret:
                errors["totp_secret"] = "totp_secret_required"
            else:
                try:
                    pyotp.TOTP(totp_secret).now()  # validate secret format
                except Exception:
                    errors["totp_secret"] = "invalid_totp_secret_format"

            if not errors:
                self._credentials = dict(user_input)
                self._credentials["totp_secret"] = totp_secret
                return await self.async_step_confirm()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required("user", default=(user_input or {}).get("user", "")): str,
                    vol.Required("password"): str,
                    vol.Required("totp_secret", default=(user_input or {}).get("totp_secret", "")): str,
                    vol.Optional("device_name", default=(user_input or {}).get("device_name", "")): str,
                }
            ),
            errors=errors,
        )

    async def async_step_confirm(self, user_input: dict[str, Any] | None = None) -> config_entries.ConfigFlowResult:
        """Step 2: show the current TOTP code for manual EKZ registration, then validate login."""
        errors = {}

        if user_input is not None:
            session = Session(
                self._credentials["user"],
                self._credentials["password"],
                self._credentials["totp_secret"],
                self._credentials.get("device_name") or None,
            )
            try:
                data = await session.installation_selection_data()
                contracts = data.get("contracts") if isinstance(data, dict) else None
                _LOGGER.warning(
                    "[config_flow] installation_selection_data response keys=%s contracts=%s",
                    list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                    contracts,
                )
                if not contracts:
                    _LOGGER.warning(
                        "[config_flow] Login succeeded but no contracts found for user %s",
                        self._credentials["user"],
                    )
                    errors["base"] = "no_contracts"
                else:
                    return self.async_create_entry(title="EKZ", data=self._credentials)
            except ValueError as e:
                msg = str(e)
                _LOGGER.warning("[config_flow] Login failed: %s", msg)
                if "SMS" in msg:
                    errors["base"] = "sms_2fa_not_supported"
                elif "TOTP code was rejected" in msg or "TOTP" in msg:
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

        # Generate current TOTP code to display to the user
        totp_code = pyotp.TOTP(self._credentials["totp_secret"]).now()

        return self.async_show_form(
            step_id="confirm",
            data_schema=vol.Schema({}),
            description_placeholders={"totp_code": totp_code},
            errors=errors,
        )
