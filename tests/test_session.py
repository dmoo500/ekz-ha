import tests.mock_ha
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import aiohttp
from custom_components.ekz_ha.session import Session
import asyncio

class TestSession(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.username = "testuser"
        self.password = "testpass"
        self.totp_secret = "JBSWY3DPEHPK3PXP" # Base32
        
        # Avoid creating real ClientSession during init
        with patch("aiohttp.ClientSession") as mock_cs:
            self.session = Session(self.username, self.password, self.totp_secret)
            self.mock_cs_instance = mock_cs.return_value

    async def asyncTearDown(self):
        # Clean up session
        self.session._session = None

    @patch("aiohttp.ClientSession")
    async def test_init_session(self, mock_client_session):
        self.session._session = None
        self.session._init_session()
        mock_client_session.assert_called()
        # Verify User-Agent was added
        self.session._session.headers.add.assert_called_with("User-Agent", "ekz-ha")

    async def test_reset_session(self):
        mock_session_instance = AsyncMock()
        self.session._session = mock_session_instance
        self.session._logged_in = True
        
        await self.session._reset_session()
        
        mock_session_instance.close.assert_awaited_once()
        self.assertIsNone(self.session._session)
        self.assertFalse(self.session._logged_in)

    async def test_ensure_logged_in_already_logged_in(self):
        self.session._logged_in = True
        with patch.object(self.session, "_init_session") as mock_init:
            await self.session._ensure_logged_in()
            mock_init.assert_not_called()

    @patch("custom_components.ekz_ha.session.BeautifulSoup")
    @patch("pyotp.TOTP")
    async def test_ensure_logged_in_success(self, mock_totp, mock_bs):
        mock_session = MagicMock()
        self.session._session = mock_session
        
        # 1. Login form response
        mock_r1 = AsyncMock()
        mock_r1.ok = True
        mock_r1.text.return_value = "<html>login form</html>"
        
        # 2. OTP form response
        mock_r2 = AsyncMock()
        mock_r2.ok = True
        mock_r2.text.return_value = "<html>otp form</html>"
        
        # 3. Final response
        mock_r3 = AsyncMock()
        mock_r3.ok = True
        mock_r3.text.return_value = "<html>success</html>"
        
        # Mocking async context manager for get/post
        mock_session.get.return_value.__aenter__.return_value = mock_r1
        
        # Mock post to return different things on subsequent calls
        mock_post_cm1 = MagicMock()
        mock_post_cm1.__aenter__ = AsyncMock(return_value=mock_r2)
        mock_post_cm1.__aexit__ = AsyncMock()
        
        mock_post_cm2 = MagicMock()
        mock_post_cm2.__aenter__ = AsyncMock(return_value=mock_r3)
        mock_post_cm2.__aexit__ = AsyncMock()
        
        mock_session.post.side_effect = [mock_post_cm1, mock_post_cm2]
        
        # Helper to create mock tags with .get() and .select()
        def create_mock_tag(tag_id=None, action=None):
            tag = MagicMock()
            tag.get.side_effect = lambda key, default=None: (tag_id if key == "id" else (action if key == "action" else default))
            tag.__getitem__.side_effect = lambda key: (action if key == "action" else tag_id)
            tag.select.return_value = []
            tag.get_text.return_value = "Mock Tag"
            return tag

        # Mock BeautifulSoup responses
        mock_soup1 = MagicMock()
        mock_form1 = create_mock_tag(action="http://auth-url")
        mock_soup1.select.return_value = [mock_form1]
        
        mock_soup2 = MagicMock()
        mock_otp_form = create_mock_tag(action="http://otp-url")
        mock_soup2.select.side_effect = [
            [create_mock_tag(tag_id="form-id")], # all_form_ids
            [mock_otp_form], # otpform
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

if __name__ == "__main__":
    unittest.main()
