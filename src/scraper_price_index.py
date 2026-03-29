"""Scraper for ChemOrbis Price Index reports."""

import os
import time
import shutil
import logging
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

from src.utils import get_latest_file

logger = logging.getLogger(__name__)


class PriceIndexScraper:
    """Scrapes weekly price index data from ChemOrbis.

    Downloads Excel files with historical price index data
    for each combination of territory, product group, product,
    and transaction type.
    """

    def __init__(self, driver: webdriver.Chrome, config: dict):
        """Initialize the Price Index scraper.

        Args:
            driver: Selenium WebDriver instance.
            config: Application configuration dictionary.
        """
        self.driver = driver
        self.config = config
        self.url = config["urls"]["price_index"]
        self.scraping_cfg = config["scraping"]

    def scrape(self, df: pd.DataFrame, output_base_dir: str, downloads_dir: str) -> list[str]:
        """Scrape price index data for all rows in the DataFrame.

        Args:
            df: DataFrame with columns: Territory, Product Group,
                Product, Transaction Type.
            output_base_dir: Root directory for saving downloaded files.
            downloads_dir: Browser downloads directory path.

        Returns:
            List of paths to successfully downloaded files.
        """
        downloaded_files = []

        for index, row in df.iterrows():
            territory = row["Territory"]
            product_group = row["Product Group"]
            product = row["Product"]
            transaction_type = row["Transaction Type"]

            logger.info(
                f"Processing: {territory} / {product_group} / "
                f"{product} / {transaction_type}"
            )

            success = self._scrape_single_row(
                territory=territory,
                product_group=product_group,
                product=product,
                transaction_type=transaction_type,
                output_base_dir=output_base_dir,
                downloads_dir=downloads_dir,
            )

            if success:
                downloaded_files.append(success)

        return downloaded_files

    def _scrape_single_row(
        self,
        territory: str,
        product_group: str,
        product: str,
        transaction_type: str,
        output_base_dir: str,
        downloads_dir: str,
    ) -> str | None:
        """Scrape data for a single parameter combination with retries.

        Returns:
            Path to downloaded file on success, None on failure.
        """
        max_retries = self.scraping_cfg["max_retries"]

        for retry in range(max_retries):
            try:
                return self._attempt_scrape(
                    territory, product_group, product,
                    transaction_type, output_base_dir, downloads_dir
                )
            except Exception as e:
                logger.warning(
                    f"Attempt {retry + 1}/{max_retries} failed for "
                    f"{territory}/{product_group}: {e}"
                )
                if retry == max_retries - 1:
                    logger.error(
                        f"All retries exhausted for "
                        f"{territory}/{product_group}/{product}"
                    )
                    return None
                time.sleep(self.scraping_cfg["retry_delay"])

    def _attempt_scrape(
        self,
        territory: str,
        product_group: str,
        product: str,
        transaction_type: str,
        output_base_dir: str,
        downloads_dir: str,
    ) -> str:
        """Single attempt to scrape and download data.

        Returns:
            Path to the moved file.
        """
        wait_time = self.scraping_cfg["page_load_wait"]
        timeout = self.scraping_cfg["loop_timeout"]

        self.driver.get(self.url)
        time.sleep(wait_time)

        # Select filters sequentially
        self._click_label_with_retry(territory, timeout)
        time.sleep(wait_time)

        self._click_label_with_retry(product_group, timeout)
        time.sleep(wait_time)

        self._click_label_with_retry(product, timeout)
        time.sleep(wait_time)

        self._click_label_with_retry(transaction_type, timeout)
        time.sleep(wait_time)

        # Click "Report" button
        self._click_with_retry(
            By.ID,
            "preparePIRSubView:pirform:reportBtn",
            timeout,
        )

        # Set currency to USD
        self._select_currency_usd(timeout)

        # Download Excel file
        self._download_excel()

        # Move file to target directory
        time.sleep(self.scraping_cfg["download_wait"])
        return self._move_downloaded_file(
            downloads_dir, output_base_dir,
            territory, product_group, product, transaction_type
        )

    def _click_label_with_retry(self, label_text: str, timeout: int) -> None:
        """Click a label element by text content with retry loop.

        Args:
            label_text: Text content of the label to click.
            timeout: Maximum seconds to keep trying.
        """
        start_time = time.time()
        wait_time = self.scraping_cfg["page_load_wait"]

        while True:
            try:
                time.sleep(wait_time)
                button = WebDriverWait(self.driver, self.scraping_cfg["element_wait"]).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, f"//label[contains(text(), '{label_text}')]")
                    )
                )
                button.click()
                return
            except Exception:
                if time.time() - start_time > timeout:
                    raise RuntimeError(
                        f"Timeout clicking label '{label_text}'"
                    )

    def _click_with_retry(self, by: By, value: str, timeout: int) -> None:
        """Click an element by locator with retry loop."""
        start_time = time.time()
        wait_time = self.scraping_cfg["page_load_wait"]

        while True:
            try:
                time.sleep(wait_time)
                button = self.driver.find_element(by, value)
                button.click()
                return
            except Exception:
                if time.time() - start_time > timeout:
                    raise RuntimeError(
                        f"Timeout clicking element {by}={value}"
                    )

    def _select_currency_usd(self, timeout: int) -> None:
        """Select USD currency from dropdown."""
        start_time = time.time()
        currency_code = self.config.get("currency_usd_code", "840")

        while True:
            try:
                dropdown = self.driver.find_element(
                    By.ID,
                    "preparePIRSubView:pirform:j_idt143:0:comboCurrency",
                )
                select = Select(dropdown)
                select.select_by_value(currency_code)
                return
            except Exception:
                if time.time() - start_time > timeout:
                    logger.warning("Could not set currency to USD, continuing")
                    return

    def _download_excel(self) -> None:
        """Click export dropdown and download Excel data file."""
        element_wait = self.scraping_cfg["element_wait"]

        button = WebDriverWait(self.driver, element_wait).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[contains(@id, 'dropdownMenu')]")
            )
        )
        button.click()

        button = WebDriverWait(self.driver, element_wait).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(text(), 'EXCEL (data)')]")
            )
        )
        button.click()

    def _move_downloaded_file(
        self,
        downloads_dir: str,
        output_base_dir: str,
        territory: str,
        product_group: str,
        product: str,
        transaction_type: str,
    ) -> str:
        """Move downloaded file to organized directory structure.

        Returns:
            Path to the moved file.

        Raises:
            FileNotFoundError: If no file found in downloads directory.
        """
        target_dir = os.path.join(
            output_base_dir, "price_index",
            territory, product_group, product, transaction_type
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