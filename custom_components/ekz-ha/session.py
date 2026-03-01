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
    ) -> None:
        """Construct an instance of the EKZ session."""
        self._session = aiohttp.ClientSession()
        self._session.headers.add("User-Agent", "ekz-ha")
        self._username = username
        self._password = password
        self._totp_secret = totp_secret.strip().replace(" ", "") if totp_secret else None
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

                if otpform:
                    if not self._totp_secret:
                        raise ValueError(
                            "EKZ requires a TOTP code (authenticator app) but no TOTP secret was configured. "
                            "Please reconfigure the integration and enter the TOTP secret key from your authenticator app."
                        )
                    otp_action = otpform[0]["action"]
                    totp_code = pyotp.TOTP(self._totp_secret).now()
                    _LOGGER.debug("[EKZ] Submitting TOTP code for 2FA")
                    async with self._session.post(
                        otp_action, data={"otp": totp_code}
                    ) as r:
                        html = await r.text()
                        if not r.ok:
                            raise ValueError("TOTP submission failed. Check your TOTP secret key.")
                        soup = BeautifulSoup(html, "html.parser")
                        # If OTP form still present, the code was wrong
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
        async with self._session.get(
            "https://my.ekz.ch/api/portal-services/consumption-view/v1/installation-selection-data"
            "?installationVariant=CONSUMPTION",
            headers=JSON_HEADERS,
        ) as r:
            if not r.ok:
                # We may have timed out. Mark as not logged in and return an empty object.
                _LOGGER.warning(
                    "Refreshing session as fetching InstallationSelectionData failed"
                )
                await self._reset_session()
                return InstallationSelectionData()
            data = await r.json()
            if data == []:
                _LOGGER.warning(
                    "Refreshing session as fetching InstallationSelectionData returned empty results"
                )
                await self._reset_session()
            return data

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
