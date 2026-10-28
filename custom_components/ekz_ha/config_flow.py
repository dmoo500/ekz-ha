"""Config flow for EKZ integration."""

import logging

import pyotp
import voluptuous as vol
from homeassistant import config_entries

from .api.client import EkzApiClient
from .api.session import EkzAuthenticationError, EkzConnectionError
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class EkzConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Ekz config flow."""

    VERSION = 1
    MINOR_VERSION = 1

    def __init__(self):
        self._credentials = {}
        self._reauth = False

    async def async_step_reauth(self, entry_data):
        """Collect replacement credentials for the existing entry."""
        self._reauth = True
        return await self.async_step_user()

    async def async_step_user(self, user_input=None):
        """Step 1: collect credentials and TOTP secret, then show generated OTP."""
        if not self._reauth:
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
                    vol.Required(
                        "totp_secret", default=(user_input or {}).get("totp_secret", "")
                    ): str,
                    vol.Optional(
                        "device_name", default=(user_input or {}).get("device_name", "")
                    ): str,
                }
            ),
            errors=errors,
        )

    async def async_step_confirm(self, user_input=None):
        """Step 2: show the current TOTP code for manual EKZ registration, then validate login."""
        errors = {}

        if user_input is not None:
            api_client = EkzApiClient(
                self._credentials["user"],
                self._credentials["password"],
                self._credentials["totp_secret"],
                self._credentials.get("device_name"),
            )
            try:
                # Try to fetch installation data to verify login
                data = await api_client.get_consumption_installations()
                if not data.contracts:
                    data = await api_client.get_production_installations()
                if not data.contracts:
                    _LOGGER.warning(
                        "[config_flow] Login succeeded but no contracts found for user %s",
                        self._credentials["user"],
                    )
                    errors["base"] = "no_contracts"
                else:
                    if self._reauth:
                        entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
                        assert entry is not None
                        return self.async_update_reload_and_abort(
                            entry,
                            data={**entry.data, **self._credentials},
                        )
                    return self.async_create_entry(title="EKZ", data=self._credentials)
            except EkzAuthenticationError as e:
                msg = str(e)
                _LOGGER.warning("[config_flow] Login failed: %s", msg)
                if "SMS" in msg:
                    errors["base"] = "sms_2fa_not_supported"
                elif "TOTP code was rejected" in msg or "TOTP" in msg:
                    errors["base"] = "invalid_totp"
                else:
                    errors["base"] = "invalid_auth"
            except EkzConnectionError as e:
                _LOGGER.warning("[config_flow] Login service unavailable: %s", e)
                errors["base"] = (
                    "ekz_maintenance" if "maintenance" in str(e).lower() else "cannot_connect"
                )
            except Exception as e:  # noqa: BLE001
                _LOGGER.exception("[config_flow] Unexpected error during login: %s", e)
                errors["base"] = "cannot_connect"
            finally:
                await api_client.close()

        # Generate current TOTP code to display to the user
        totp_code = pyotp.TOTP(self._credentials["totp_secret"]).now()

        return self.async_show_form(
            step_id="confirm",
            data_schema=vol.Schema({}),
            description_placeholders={"totp_code": totp_code},
            errors=errors,
        )
