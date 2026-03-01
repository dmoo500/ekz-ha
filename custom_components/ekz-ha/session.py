"""Interact with EKZ."""

import logging

import aiohttp
from bs4 import BeautifulSoup
import pyotp

from .apitypes import ConsumptionData, InstallationData, InstallationSelectionData

_LOGGER = logging.getLogger(__name__)

HTML_HEADERS = {"Accept": "text/html,application/xhtml+xml,application/xml"}
JSON_HEADERS = {"Accept": "application/json, text/plain, */*"}


class Session:
    """Represents a session with the EKZ API."""

    def __init__(
        self,
        username: str,
        password: str,
        totp_secret: str | None = None,
        device_name: str | None = None,
    ) -> None:
        """Construct an instance of the EKZ session."""
        self._session = aiohttp.ClientSession()
        self._session.headers.add("User-Agent", "ekz-ha")
        self._username = username
        self._password = password
        self._totp_secret = totp_secret.strip().replace(" ", "") if totp_secret else None
        self._device_name = device_name.strip() if device_name else None
        self._logged_in = False

    def _init_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._session.headers.add("User-Agent", "ekz-ha")
            self._logged_in = False

    async def _reset_session(self):
        if self._session is not None:
            await self._session.close()            
        self._session = None
        self._logged_in = False

    async def _ensure_logged_in(self):
        if self._logged_in:
            return
        self._init_session()

        async with self._session.get(
            "https://my.ekz.ch/verbrauch/", headers=HTML_HEADERS
        ) as r:
            if not r.ok:
                raise ValueError("EKZ seems unreachable")
            html = await r.text()

            # Find the login form and get the action URL, so we can submit credentials.
            soup = BeautifulSoup(html, "html.parser")
            loginform = soup.select("form[id=kc-form-login]")
            if not loginform:
                if "Es tut uns leid" in html:
                    raise ValueError("myEKZ appears to be offline for maintenance")
                raise ValueError("Login form not found on page")
            authurl = loginform[0]["action"]

            async with self._session.post(
                authurl, data={"username": self._username, "password": self._password}
            ) as r:
                html = await r.text()
                if not r.ok:
                    raise ValueError("Login failed. Bad user/password?")
                # Check for TOTP 2FA form
                soup = BeautifulSoup(html, "html.parser")
                otpform = soup.select("form[id=otp]")
                smscode_form = soup.select("form[id=kc-sms-code-login-form]")

                # Check for credential/device selector (appears before OTP when multiple
                # authenticators are registered)
                credential_form = soup.select(
                    "form[id=select-credential], form[id=kc-select-credential-form]"
                )
                if credential_form:
                    cred_action = credential_form[0]["action"]
                    # Find all selectable credential items — Keycloak renders them as
                    # <a> / <button> elements whose text is the device name, carrying a
                    # data-kc-id or similar attribute with the credential id.
                    options = credential_form[0].select(
                        "input[name=credentialId]"
                    ) or credential_form[0].select(
                        "[data-credential-id]"
                    ) or credential_form[0].select(
                        "a[id^=credential], button[id^=credential]"
                    )
                    _LOGGER.debug(
                        "[EKZ] Credential selector found. device_name=%s, options=%s",
                        self._device_name,
                        [o.get_text(strip=True) for o in options],
                    )
                    chosen = None
                    if options:
                        if self._device_name:
                            # Try to match by text content
                            for opt in options:
                                label = opt.get_text(strip=True)
                                cred_id = (
                                    opt.get("value")
                                    or opt.get("data-credential-id")
                                    or opt.get("data-kc-id")
                                    or opt.get("href", "")
                                )
                                if self._device_name.lower() in label.lower():
                                    chosen = (cred_id, label)
                                    break
                        if chosen is None:
                            # Fall back to first option
                            opt = options[0]
                            chosen = (
                                opt.get("value")
                                or opt.get("data-credential-id")
                                or opt.get("data-kc-id")
                                or opt.get("href", ""),
                                opt.get_text(strip=True),
                            )
                        cred_id, cred_label = chosen
                        _LOGGER.debug(
                            "[EKZ] Selecting credential '%s' (id=%s)", cred_label, cred_id
                        )
                        # If the credential is a link (href), follow it; otherwise POST
                        if cred_id and cred_id.startswith("http"):
                            async with self._session.get(cred_id, headers=HTML_HEADERS) as r:
                                html = await r.text()
                                soup = BeautifulSoup(html, "html.parser")
                                otpform = soup.select("form[id=otp]")
                        else:
                            async with self._session.post(
                                cred_action,
                                data={"credentialId": cred_id},
                                headers=HTML_HEADERS,
                            ) as r:
                                html = await r.text()
                                soup = BeautifulSoup(html, "html.parser")
                                otpform = soup.select("form[id=otp]")
                    else:
                        _LOGGER.warning(
                            "[EKZ] Credential selector found but no options could be parsed. "
                            "Raw form: %s", credential_form[0]
                        )

                if otpform:
                    if not self._totp_secret:
                        raise ValueError(
                            "EKZ requires a TOTP code (authenticator app) but no TOTP secret was configured. "
                            "Please reconfigure the integration and enter the TOTP secret key from your authenticator app."
                        )
                    otp_action = otpform[0]["action"]
                    totp_code = pyotp.TOTP(self._totp_secret).now()
                    _LOGGER.debug("[EKZ] Submitting TOTP code for login 2FA")
                    async with self._session.post(
                        otp_action, data={"otp": totp_code}
                    ) as r:
                        html = await r.text()
                        if not r.ok:
                            raise ValueError("TOTP submission failed. Check your TOTP secret key.")
                        soup = BeautifulSoup(html, "html.parser")
                        if soup.select("form[id=otp]"):
                            raise ValueError(
                                "TOTP code was rejected by EKZ. Check that your TOTP secret key is correct and that the system clock is accurate."
                            )
                elif smscode_form:
                    raise ValueError(
                        "EKZ requires an SMS 2FA code, which is not supported. "
                        "You must disable SMS 2FA and switch to an authenticator app instead. "
                        "Go to: https://login.ekz.ch/auth/realms/myEKZ/account/?referrer=cos-myekz-webapp"
                        "&referrer_uri=https://my.ekz.ch/nutzerdaten/#/account-security/signing-in"
                    )
                elif "Es tut uns leid" in html:
                    raise ValueError("myEKZ appears to be offline for maintenance")

                self._logged_in = True

    async def installation_selection_data(self) -> InstallationSelectionData:
        """Fetch the available installations."""
        await self._ensure_logged_in()
        for variant in ["?installationVariant=CONSUMPTION", ""]:
            async with self._session.get(
                "https://my.ekz.ch/api/portal-services/consumption-view/v1/installation-selection-data"
                + variant,
                headers=JSON_HEADERS,
            ) as r:
                if not r.ok:
                    _LOGGER.warning(
                        "Refreshing session as fetching InstallationSelectionData failed (status %s)",
                        r.status,
                    )
                    await self._reset_session()
                    return InstallationSelectionData()
                data = await r.json()
                _LOGGER.debug(
                    "[installation_selection_data] variant=%s, keys=%s, contracts=%s",
                    variant or "(none)",
                    list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                    data.get("contracts") if isinstance(data, dict) else data,
                )
                if isinstance(data, dict) and data.get("contracts"):
                    return data
                if variant == "":
                    # Both attempts returned no contracts — return whatever we have
                    _LOGGER.warning(
                        "[installation_selection_data] No contracts found in either API variant. "
                        "Full response: %s", data
                    )
                    return data
        return InstallationSelectionData()

    async def get_installation_data(self, installation_id: str) -> InstallationData:
        """Fetch the metadata for an installation."""
        await self._ensure_logged_in()
        async with self._session.get(
            "https://my.ekz.ch/api/portal-services/consumption-view/v1/installation-data"
            "?installationId=" + installation_id,
            headers=JSON_HEADERS,
        ) as r:
            if not r.ok:
                # We may have timed out. Mark as not logged in and return an empty object.
                _LOGGER.warning(
                    "Refreshing session as fetching InstallationData failed"
                )
                await self._reset_session()
                return InstallationData()
            data = await r.json()
            if data == []:
                _LOGGER.warning(
                    "Refreshing session as fetching InstallationData returned empty results"
                )
                await self._reset_session()
            return data

    async def get_consumption_data(
        self, installation_id: str, data_type: str, date_from: str, date_to: str
    ) -> ConsumptionData:
        """Fetch the consumption date at the given intallation in the date range provided."""
        await self._ensure_logged_in()
        async with self._session.get(
            f"https://my.ekz.ch/api/portal-services/consumption-view/v1/consumption-data"
            f"?installationId={installation_id}&from={date_from}&to={date_to}&type={data_type}",
            headers=JSON_HEADERS,
        ) as r:
            if not r.ok:
                # We may have timed out. Mark as not logged in and return an empty object.
                _LOGGER.warning("Refreshing session as fetching ConsumptionData failed")
                await self._reset_session()
                return ConsumptionData()
            data = await r.json()
            if data == []:
                _LOGGER.warning(
                    "Refreshing session as fetching ConsumptionData returned empty results"
                )
                await self._reset_session()
            return data
