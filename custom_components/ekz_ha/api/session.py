"""Interact with EKZ."""

import logging
import re
from datetime import datetime

import aiohttp
import pyotp
from bs4 import BeautifulSoup

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

        # Clean and validate TOTP secret
        if totp_secret:
            cleaned_secret = totp_secret.strip().replace(" ", "").replace("-", "").upper()
            # Validate Base32 format (only A-Z, 2-7)
            if not re.match(r'^[A-Z2-7]+=*$', cleaned_secret):
                raise ValueError(
                    f"Invalid TOTP secret format. Secret must be Base32 encoded (only A-Z and 2-7). "
                    f"Received secret contains invalid characters. "
                    f"Please check your authenticator app and re-enter the secret."
                )
            self._totp_secret = cleaned_secret
            _LOGGER.debug(
                "[EKZ TOTP] Secret validated (length=%d). Format OK.",
                len(cleaned_secret)
            )
        else:
            self._totp_secret = None

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

    async def _fetch_with_retry(
        self, url: str, operation_name: str, empty_value, extra_context: str = ""
    ):
        """Fetch data with automatic retry on failure."""
        async with self._session.get(url, headers=JSON_HEADERS) as r:
            if not r.ok:
                _LOGGER.warning(
                    "Refreshing session as fetching %s failed (status %s%s) - retrying once",
                    operation_name,
                    r.status,
                    extra_context,
                )
                await self._reset_session()
                await self._ensure_logged_in()
                async with self._session.get(url, headers=JSON_HEADERS) as retry_r:
                    if not retry_r.ok:
                        _LOGGER.error(
                            "Retry failed: fetching %s failed (status %s%s)",
                            operation_name,
                            retry_r.status,
                            extra_context,
                        )
                        return empty_value
                    data = await retry_r.json()
                    if data == []:
                        _LOGGER.warning(
                            "Refreshing session as fetching %s returned empty results",
                            operation_name,
                        )
                        await self._reset_session()
                    return data
            data = await r.json()
            if data == []:
                _LOGGER.warning(
                    "Refreshing session as fetching %s returned empty results",
                    operation_name,
                )
                await self._reset_session()
            return data

    async def _ensure_logged_in(self):
        if self._logged_in:
            return
        self._init_session()

        async with self._session.get("https://my.ekz.ch/verbrauch/", headers=HTML_HEADERS) as r:
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
                authurl,
                data={"username": self._username, "password": self._password},
                allow_redirects=True,
            ) as r:
                html = await r.text()
                if not r.ok:
                    raise ValueError("Login failed. Bad user/password?")

                soup = BeautifulSoup(html, "html.parser")
                all_form_ids = [f.get("id", "<no-id>") for f in soup.select("form")]
                _LOGGER.debug("[EKZ login] Step: after password. Forms on page: %s", all_form_ids)

                otpform = soup.select("form[id=otp], form[id=kc-otp-login-form]")
                smscode_form = soup.select("form[id=kc-sms-code-login-form]")

                if otpform:
                    if not self._totp_secret:
                        raise ValueError(
                            "EKZ requires a TOTP code (authenticator app) but no TOTP secret was configured. "
                            "Please reconfigure the integration and enter the TOTP secret key from your authenticator app."
                        )
                    otp_action = otpform[0]["action"]

                    # Generate TOTP code
                    current_time = datetime.now()
                    totp = pyotp.TOTP(self._totp_secret)
                    totp_code = totp.now()

                    _LOGGER.info(
                        "[EKZ TOTP] Generated code at %s (UTC: %s). Code: %s",
                        current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        totp_code[:3] + "***"  # Log first 3 digits only for privacy
                    )

                    # The OTP form may also contain a device selector (selectedCredentialId).
                    # Both fields must be submitted together in a single POST.
                    post_data: dict[str, str] = {"otp": totp_code}

                    radio_inputs = otpform[0].select("input[name=selectedCredentialId]")
                    if radio_inputs:
                        # Build list of (credential_id, device_label) from the radio inputs
                        candidates: list[tuple[str, str]] = []
                        for inp in radio_inputs:
                            cred_id = inp.get("value", "")
                            inp_id = inp.get("id", "")
                            label_tag = (
                                otpform[0].select_one(f"label[for={inp_id}]") if inp_id else None
                            )
                            if label_tag:
                                title_span = label_tag.select_one("span.pf-c-tile__title")
                                device_label = (
                                    title_span.get_text(strip=True)
                                    if title_span
                                    else label_tag.get_text(strip=True)
                                )
                            else:
                                device_label = cred_id
                            candidates.append((cred_id, device_label))

                        _LOGGER.info(
                            "[EKZ login] OTP form contains device selector. Available: %s",
                            [(label, cred_id) for cred_id, label in candidates],
                        )

                        chosen_id = None
                        chosen_label = None
                        if self._device_name:
                            for cred_id, label in candidates:
                                if self._device_name.lower() in label.lower():
                                    chosen_id, chosen_label = cred_id, label
                                    break
                        if chosen_id is None:
                            # Use pre-checked radio first, then fallback to first option
                            for inp in radio_inputs:
                                if inp.has_attr("checked"):
                                    chosen_id = inp.get("value", "")
                                    chosen_label = next(
                                        (lbl for cid, lbl in candidates if cid == chosen_id),
                                        chosen_id,
                                    )
                                    break
                        if chosen_id is None:
                            chosen_id, chosen_label = candidates[0]

                        _LOGGER.info(
                            "[EKZ login] Selecting device '%s' (id=%s)", chosen_label, chosen_id
                        )
                        post_data["selectedCredentialId"] = chosen_id

                    _LOGGER.debug(
                        "[EKZ login] Submitting OTP form with fields: %s", list(post_data.keys())
                    )
                    async with self._session.post(
                        otp_action, data=post_data, allow_redirects=True
                    ) as r:
                        html = await r.text()
                        if not r.ok:
                            raise ValueError("TOTP submission failed. Check your TOTP secret key.")
                        soup = BeautifulSoup(html, "html.parser")
                        all_form_ids = [f.get("id", "<no-id>") for f in soup.select("form")]
                        _LOGGER.debug(
                            "[EKZ login] Step: after TOTP submit. Forms on page: %s", all_form_ids
                        )
                        if soup.select("form[id=otp], form[id=kc-otp-login-form]"):
                            _LOGGER.error(
                                "[EKZ TOTP] Code was rejected. Current system time: %s (UTC: %s). "
                                "Please verify: 1) TOTP secret is correct, 2) System clock is synchronized.",
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                            )
                            raise ValueError(
                                "TOTP code was rejected by EKZ. "
                                "Possible causes:\n"
                                "1. TOTP secret key is incorrect - please check your authenticator app\n"
                                "2. System clock is not synchronized - TOTP requires accurate time\n"
                                "3. TOTP secret was not properly copied (check for missing characters)\n\n"
                                "To fix: Remove and re-add the integration with the correct TOTP secret."
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
                else:
                    _LOGGER.info(
                        "[EKZ login] No known form found after password submit. "
                        "Forms: %s — treating as logged in (may be redirect page).",
                        [f.get("id", "<no-id>") for f in soup.select("form")],
                    )

                self._logged_in = True

    async def installation_selection_data(self) -> dict:
        """Fetch the available installations."""
        await self._ensure_logged_in()
        for variant in ["?installationVariant=CONSUMPTION", ""]:
            url = (
                "https://my.ekz.ch/api/portal-services/consumption-view/v1/installation-selection-data"
                + variant
            )
            data = await self._fetch_with_retry(
                url,
                "InstallationSelectionData",
                None,
                f", variant={variant or '(none)'}",
            )
            if data is None:
                continue
            _LOGGER.debug(
                "[installation_selection_data] variant=%s, keys=%s, contracts=%s",
                variant or "(none)",
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                data.get("contracts") if isinstance(data, dict) else data,
            )
            if isinstance(data, dict) and data.get("contracts"):
                return data
            if variant == "":
                _LOGGER.warning(
                    "[installation_selection_data] No contracts found in either API variant. "
                    "Full response: %s",
                    data,
                )
                return data
        return {}

    async def production_installation_selection_data(self) -> dict:
        """Fetch installations with solar/production variant."""
        await self._ensure_logged_in()
        async with self._session.get(
            "https://my.ekz.ch/api/portal-services/consumption-view/v1/installation-selection-data"
            "?installationVariant=PRODUCTION",
            headers=JSON_HEADERS,
        ) as r:
            if not r.ok:
                _LOGGER.debug(
                    "[production_installation_selection_data] Request failed (status %s) — no production installations",
                    r.status,
                )
                return {}
            data = await r.json()
            _LOGGER.debug(
                "[production_installation_selection_data] keys=%s, contracts=%s",
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                data.get("contracts") if isinstance(data, dict) else data,
            )
            return data if isinstance(data, dict) else {}

    async def get_installation_data(self, installation_id: str) -> dict:
        """Fetch the metadata for an installation."""
        await self._ensure_logged_in()
        url = (
            "https://my.ekz.ch/api/portal-services/consumption-view/v1/installation-data"
            "?installationId=" + installation_id
        )
        return await self._fetch_with_retry(
            url,
            "InstallationData",
            {},
        )

    async def get_consumption_data(
        self, installation_id: str, data_type: str, date_from: str, date_to: str
    ) -> dict:
        """Fetch the consumption date at the given intallation in the date range provided."""
        await self._ensure_logged_in()
        url = (
            f"https://my.ekz.ch/api/portal-services/consumption-view/v1/consumption-data"
            f"?installationId={installation_id}&from={date_from}&to={date_to}&type={data_type}"
        )
        return await self._fetch_with_retry(
            url,
            "ConsumptionData",
            {},
        )
