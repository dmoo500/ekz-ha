"""Interact with EKZ."""

import aiohttp
from bs4 import BeautifulSoup

from config.custom_components.ekz_ha.apitypes import (
    ConsumptionData,
    InstallationData,
    InstallationSelectionData,
)

HTML_HEADERS = {"Accept": "text/html,application/xhtml+xml,application/xml"}
JSON_HEADERS = {"Accept": "application/json, text/plain, */*"}


class Session:
    """Represents a session with the EKZ API."""

    def __init__(
        self,
        username: str,
        password: str,
    ) -> None:
        """Construct an instance of the EKZ session."""
        self._session = aiohttp.ClientSession()
        self._session.headers.add("User-Agent", "ekz-ha")
        self._username = username
        self._password = password
        self._logged_in = False

    async def _ensure_logged_in(self):
        if self._logged_in:
            return

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
                # Find the 2FA form, if available and get the action URL.
                soup = BeautifulSoup(html, "html.parser")
                twofaform = soup.select("form[id=kc-sms-code-login-form]")
                if not twofaform:
                    if "Es tut uns leid" in html:
                        raise ValueError("myEKZ appears to be offline for maintenance")
                else:
                    raise ValueError(
                        "2FA is incompatible with the EKZ HA integration. Please disable 2FA at https://login.ekz.ch/auth/realms/myEKZ/account/?referrer=cos-myekz-webapp&referrer_uri=https://my.ekz.ch/nutzerdaten/#/account-security/signing-in."
                    )

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
                self._logged_in = False
                return InstallationSelectionData()
            return await r.json()

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
                self._logged_in = False
                return InstallationData()
            return await r.json()

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
                self._logged_in = False
                return ConsumptionData()
            return await r.json()
