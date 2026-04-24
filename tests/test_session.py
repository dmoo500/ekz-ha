import tests.mock_ha
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import aiohttp
from custom_components.ekz_ha.session import Session
from custom_components.ekz_ha.apitypes import ConsumptionData, InstallationData, InstallationSelectionData
import asyncio

class TestSession(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.username = "testuser"
        self.password = "testpass"
        self.totp_secret = "JBSWY3DPEHPK3PXP" # Base32
        
        with patch("aiohttp.ClientSession") as mock_cs:
            self.session = Session(self.username, self.password, self.totp_secret)
            self.mock_cs_instance = mock_cs.return_value

    @patch("aiohttp.ClientSession")
    def test_init_session(self, mock_client_session):
        self.session._session = None
        self.session._init_session()
        mock_client_session.assert_called()

    async def test_reset_session(self):
        mock_session_instance = AsyncMock()
        self.session._session = mock_session_instance
        self.session._logged_in = True
        await self.session._reset_session()
        mock_session_instance.close.assert_awaited_once()
        self.assertIsNone(self.session._session)

    @patch("custom_components.ekz_ha.session.BeautifulSoup")
    @patch("pyotp.TOTP")
    async def test_ensure_logged_in_success(self, mock_totp, mock_bs):
        mock_session = MagicMock()
        self.session._session = mock_session
        
        mock_r1 = AsyncMock()
        mock_r1.ok = True
        mock_r1.text.return_value = "<html>login form</html>"
        
        mock_r2 = AsyncMock()
        mock_r2.ok = True
        mock_r2.text.return_value = "<html>otp form</html>"
        
        mock_r3 = AsyncMock()
        mock_r3.ok = True
        mock_r3.text.return_value = "<html>success</html>"
        
        mock_session.get.return_value.__aenter__.return_value = mock_r1
        
        mock_post_cm1 = MagicMock()
        mock_post_cm1.__aenter__ = AsyncMock(return_value=mock_r2)
        mock_post_cm1.__aexit__ = AsyncMock()
        
        mock_post_cm2 = MagicMock()
        mock_post_cm2.__aenter__ = AsyncMock(return_value=mock_r3)
        mock_post_cm2.__aexit__ = AsyncMock()
        
        mock_session.post.side_effect = [mock_post_cm1, mock_post_cm2]
        
        def create_mock_tag(tag_id=None, action=None):
            tag = MagicMock()
            tag.get.side_effect = lambda key, default=None: (tag_id if key == "id" else (action if key == "action" else default))
            tag.__getitem__.side_effect = lambda key: (action if key == "action" else tag_id)
            tag.select.return_value = []
            return tag

        mock_soup1 = MagicMock()
        mock_soup1.select.return_value = [create_mock_tag(action="http://auth-url")]
        
        mock_soup2 = MagicMock()
        mock_soup2.select.side_effect = [
            [create_mock_tag(tag_id="form-id")], # all_form_ids
            [create_mock_tag(action="http://otp-url")], # otpform
            [], # smscode_form
        ]
        
        mock_soup3 = MagicMock()
        mock_soup3.select.side_effect = [
            [create_mock_tag(tag_id="form-id")], # all_form_ids
            [], # otpform
        ]
        
        mock_bs.side_effect = [mock_soup1, mock_soup2, mock_soup3]
        mock_totp.return_value.now.return_value = "123456"
        
        await self.session._ensure_logged_in()
        self.assertTrue(self.session._logged_in)

    async def test_get_consumption_data(self):
        mock_session = MagicMock()
        self.session._session = mock_session
        self.session._logged_in = True
        
        mock_r = AsyncMock()
        mock_r.ok = True
        mock_r.json.return_value = {"series": {"values": [{"timestamp": 123, "value": 1.0}]}}
        mock_session.get.return_value.__aenter__.return_value = mock_r
        
        res = await self.session.get_consumption_data("inst1", "type1", "2024-01-01", "2024-01-02")
        self.assertEqual(res["series"]["values"][0]["value"], 1.0)

    async def test_installation_selection_data(self):
        mock_session = MagicMock()
        self.session._session = mock_session
        self.session._logged_in = True
        
        mock_r = AsyncMock()
        mock_r.ok = True
        mock_r.json.return_value = {"contracts": [{"anlage": "123"}]}
        mock_session.get.return_value.__aenter__.return_value = mock_r
        
        res = await self.session.installation_selection_data()
        self.assertEqual(res["contracts"][0]["anlage"], "123")

    async def test_get_installation_data(self):
        mock_session = MagicMock()
        self.session._session = mock_session
        self.session._logged_in = True
        
        mock_r = AsyncMock()
        mock_r.ok = True
        mock_r.json.return_value = {"status": []}
        mock_session.get.return_value.__aenter__.return_value = mock_r
        
        res = await self.session.get_installation_data("inst1")
        self.assertEqual(res["status"], [])

if __name__ == "__main__":
    unittest.main()
