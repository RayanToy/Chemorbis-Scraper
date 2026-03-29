"""Scraper for ChemOrbis Price Wizard (chart data / CSV exports)."""

import os
import time
import shutil
import logging
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

from src.utils import get_latest_file

logger = logging.getLogger(__name__)


class PriceWizardScraper:
    """Scrapes price chart data via the Price Wizard tool.

    Downloads CSV files with time-series price data
    grouped by territory and product group.
    """

    REGIONAL_TERRITORIES = {"Asia", "Europe", "US"}

    def __init__(self, driver: webdriver.Chrome, config: dict):
        """Initialize the Price Wizard scraper.

        Args:
            driver: Selenium WebDriver instance.
            config: Application configuration dictionary.
        """
        self.driver = driver
        self.config = config
        self.url = config["urls"]["price_wizard"]
        self.scraping_cfg = config["scraping"]

    def scrape(self, df: pd.DataFrame, output_base_dir: str, downloads_dir: str) -> list[str]:
        """Scrape price wizard data for all territory/product group combinations.

        Args:
            df: DataFrame with columns: Territory, Product Group.
            output_base_dir: Root directory for saving downloaded files.
            downloads_dir: Browser downloads directory path.

        Returns:
            List of paths to successfully downloaded files.
        """
        downloaded_files = []
        groups = df.groupby(["Territory", "Product Group"])

        for (territory, product_group), group_df in groups:
            logger.info(f"Processing: {territory} / {product_group}")

            result = self._scrape_group(
                territory=territory,
                product_group=product_group,
                output_base_dir=output_base_dir,
                downloads_dir=downloads_dir,
            )

            if result:
                downloaded_files.append(result)

        return downloaded_files

    def _scrape_group(
        self,
        territory: str,
        product_group: str,
        output_base_dir: str,
        downloads_dir: str,
    ) -> str | None:
        """Scrape data for a territory/product group with retries.

        Returns:
            Path to downloaded file on success, None on failure.
        """
        max_retries = self.scraping_cfg["max_retries"]

        for retry in range(max_retries):
            try:
                return self._attempt_scrape(
                    territory, product_group,
                    output_base_dir, downloads_dir
                )
            except Exception as e:
                logger.warning(
                    f"Attempt {retry + 1}/{max_retries} failed for "
                    f"{territory}/{product_group}: {e}"
                )
                if retry == max_retries - 1:
                    logger.error(
                        f"All retries exhausted for {territory}/{product_group}"
                    )
                    return None
                time.sleep(self.scraping_cfg["retry_delay"])

    def _attempt_scrape(
        self,
        territory: str,
        product_group: str,
        output_base_dir: str,
        downloads_dir: str,
    ) -> str:
        """Single attempt to scrape and download CSV data.

        Returns:
            Path to the moved file.
        """
        wait_time = self.scraping_cfg["page_load_wait"]
        timeout = self.scraping_cfg["loop_timeout"]
        element_wait = self.scraping_cfg["element_wait"]

        self.driver.get(self.url)

        # Select region type if territory is regional
        if territory in self.REGIONAL_TERRITORIES:
            wait = WebDriverWait(self.driver, 10)
            button = wait.until(
                EC.element_to_be_clickable((By.ID, "f1:svP:options:1"))
            )
            button.click()

        # Select territory
        self._click_list_item_with_retry(territory, timeout)
        time.sleep(wait_time)

        # Select product group
        self._click_list_item_with_retry(product_group, timeout)
        time.sleep(5)

        # Select all products
        self._select_all_checkboxes(
            "#f1\\3A svP\\3A comboproduct_ul input[type='checkbox']"
        )

        # Select all transaction types
        time.sleep(5)
        self._select_all_checkboxes_sequential(
            "#f1\\3A svP\\3A combotransactiontype input[type='checkbox']"
        )

        # Click "Show all" button
        button = self.driver.find_element(
            By.CLASS_NAME, "btn.btn-default.btn-xs.mar-top"
        )
        button.click()

        # Click "Create" button
        start_time = time.time()
        while True:
            try:
                button = WebDriverWait(self.driver, element_wait).until(
                    EC.visibility_of_element_located(
                        (By.ID, "f1:svP:createButton")
                    )
                )
                button.click()
                break
            except Exception:
                if time.time() - start_time > timeout:
                    raise RuntimeError("Timeout waiting for Create button")

        # Download CSV from chart
        self._download_csv(timeout)

        # Move file to target directory
        time.sleep(self.scraping_cfg["download_wait"])
        return self._move_downloaded_file(
            downloads_dir, output_base_dir, territory, product_group
        )

    def _click_list_item_with_retry(self, text: str, timeout: int) -> None:
        """Click a list item (li) by text content with retry loop."""
        start_time = time.time()
        wait_time = self.scraping_cfg["page_load_wait"]

        while True:
            try:
                time.sleep(wait_time)
                button = WebDriverWait(self.driver, self.scraping_cfg["element_wait"]).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, f"//li[contains(text(), '{text}')]")
                    )
                )
                button.click()
                return
            except Exception:
                if time.time() - start_time > timeout:
                    raise RuntimeError(
                        f"Timeout clicking list item '{text}'"
                    )

    def _select_all_checkboxes(self, css_selector: str) -> None:
        """Click all checkboxes matching a CSS selector."""
        checkboxes = self.driver.find_elements(By.CSS_SELECTOR, css_selector)
        for checkbox in checkboxes:
            checkbox.click()
            time.sleep(1)

    def _select_all_checkboxes_sequential(self, css_selector: str) -> None:
        """Click checkboxes one by one, re-querying DOM each time.

        Needed when clicking a checkbox causes DOM to update.
        """
        checkbox_selectors = self.driver.find_elements(
            By.CSS_SELECTOR, css_selector
        )
        for i in range(len(checkbox_selectors)):
            checkboxes = self.driver.find_elements(
                By.CSS_SELECTOR, css_selector
            )
            checkboxes[i].click()
            time.sleep(2)

    def _download_csv(self, timeout: int) -> None:
        """Download CSV from Highcharts export menu."""
        start_time = time.time()

        while True:
            try:
                buttons = self.driver.find_elements(
                    By.CLASS_NAME, "highcharts-exporting-group"
                )
                buttons[1].click()
                time.sleep(5)

                csv_button = self.driver.find_element(
                    By.XPATH, "//li[contains(text(), 'Download CSV')]"
                )
                csv_button.click()
                time.sleep(5)
                return
            except Exception:
                if time.time() - start_time > timeout:
                    raise RuntimeError("Timeout downloading CSV")

    def _move_downloaded_file(
        self,
        downloads_dir: str,
        output_base_dir: str,
        territory: str,
        product_group: str,
    ) -> str:
        """Move downloaded CSV to organized directory structure.

        Returns:
            Path to the moved file.
        """
        target_dir = os.path.join(
            output_base_dir, "price_wizard", territory, product_group
        )
        os.makedirs(target_dir, exist_ok=True)

        latest_file = get_latest_file(downloads_dir)
        if not latest_file:
            raise FileNotFoundError(
                f"No downloaded file found in {downloads_dir}"
            )

        current_date = datetime.now().strftime("%Y-%m-%d")
        new_path = os.path.join(
            target_dir,
            f"{current_date}_{os.path.basename(latest_file)}"
        )
        shutil.move(latest_file, new_path)
        logger.info(f"File saved: {new_path}")
        return new_path