"""Authentication module for ChemOrbis website."""

import logging
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


class ChemOrbisAuthenticator:
    """Handles login and session setup for ChemOrbis."""

    def __init__(self, driver: webdriver.Chrome, config: dict):
        """Initialize authenticator.

        Args:
            driver: Selenium WebDriver instance.
            config: Application configuration dictionary.
        """
        self.driver = driver
        self.config = config
        self.wait = WebDriverWait(driver, 40)

    def login(self, username: str, password: str) -> None:
        """Perform login to ChemOrbis.

        Args:
            username: Account login/email.
            password: Account password.

        Raises:
            Exception: If login fails.
        """
        login_url = self.config["urls"]["login"]
        logger.info(f"Navigating to login page: {login_url}")
        self.driver.get(login_url)

        # Enter credentials
        username_field = self.wait.until(
            EC.presence_of_element_located((By.ID, "login-username"))
        )
        username_field.send_keys(username)

        password_field = self.wait.until(
            EC.presence_of_element_located((By.ID, "login-password"))
        )
        password_field.send_keys(password)

        # Handle shadow DOM cookie/consent button
        self._click_shadow_dom_consent()

        # Submit login form
        submit_button = self.driver.find_element(
            By.CSS_SELECTOR, 'input[type="submit"][value="Login"]'
        )
        submit_button.click()
        logger.info("Login form submitted")

        # Handle post-login popups
        self._handle_post_login_popups()

    def _click_shadow_dom_consent(self) -> None:
        """Click consent button inside shadow DOM (cookie banner)."""
        try:
            element = self.driver.execute_script("""
                var layoutDynamic = document.querySelector('efilli-layout-dynamic');
                var shadowRoot = layoutDynamic.shadowRoot;
                return shadowRoot.querySelector(
                    'div[data-id="e81a61a0-48df-46e2-a9f3-229cb29342ec"]'
                );
            """)
            if element:
                self.driver.execute_script("arguments[0].click();", element)
                logger.info("Shadow DOM consent button clicked")
        except Exception as e:
            logger.warning(f"Could not click shadow DOM consent: {e}")

    def _handle_post_login_popups(self) -> None:
        """Dismiss popups that may appear after login."""
        # "Remember me" checkbox
        try:
            checkbox = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "remV"))
            )
            checkbox.click()
            logger.info("Remember-me checkbox clicked")
        except Exception as e:
            logger.warning(f"Remember-me checkbox not found: {e}")

        # Close promotional popup
        try:
            close_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.close.youtube-close")
                )
            )
            close_button.click()
            logger.info("Promotional popup closed")
        except Exception as e:
            logger.warning(f"No promotional popup to close: {e}")