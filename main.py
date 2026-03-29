#!/usr/bin/env python3
"""ChemOrbis Price Data Scraper — Main Entry Point.

This script automates the collection and consolidation of polymer price data
from ChemOrbis.com. It performs:
1. Authentication on the ChemOrbis platform
2. Scraping price index data (weekly Excel reports)
3. Scraping price wizard data (CSV chart exports)
4. Consolidating all data into a single formatted Excel file

Usage:
    python main.py                    # Run full pipeline
    python main.py --process-only     # Skip scraping, process existing files
    python main.py --scrape-only      # Scrape only, skip processing

Configuration:
    - config.yaml: URLs, timeouts, quote mappings
    - .env: Credentials and local file paths
"""

import argparse
import logging
import os
import sys

import pandas as pd
from selenium import webdriver

from src.utils import load_config, load_env_credentials, get_env_path, setup_logging
from src.auth import ChemOrbisAuthenticator
from src.scraper_price_index import PriceIndexScraper
from src.scraper_price_wizard import PriceWizardScraper
from src.data_processor import DataProcessor
from src.excel_formatter import format_output_excel
from src.database import Database

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="ChemOrbis Price Data Scraper and Processor"
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="Skip scraping, only process existing downloaded files",
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Only scrape data, skip processing step",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    return parser.parse_args()


def create_driver() -> webdriver.Chrome:
    """Create and configure Chrome WebDriver instance.

    Returns:
        Configured Chrome WebDriver.
    """
    options = webdriver.ChromeOptions()
    # Add options for stability
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=options)
    return driver


def run_scrapers(config: dict) -> None:
    """Execute both scrapers: Price Index and Price Wizard.

    Args:
        config: Application configuration dictionary.
    """
    login, password = load_env_credentials()
    downloads_dir = get_env_path("DOWNLOADS_DIR")
    output_base_dir = get_env_path("BASE_OUTPUT_DIR")

    if not downloads_dir or not output_base_dir:
        raise ValueError(
            "DOWNLOADS_DIR and BASE_OUTPUT_DIR must be set in .env"
        )

    input_file = config["input_file"]

    driver = create_driver()

    try:
        # Authenticate
        auth = ChemOrbisAuthenticator(driver, config)
        auth.login(login, password)

        # Scrape Price Index data
        logger.info("=" * 60)
        logger.info("Starting Price Index scraping")
        logger.info("=" * 60)

        df_price_index = pd.read_excel(
            input_file, header=0, index_col=0,
            sheet_name=config["sheets"]["price_index"],
        )

        price_index_scraper = PriceIndexScraper(driver, config)
        price_index_scraper.scrape(df_price_index, output_base_dir, downloads_dir)

        # Scrape Price Wizard data
        logger.info("=" * 60)
        logger.info("Starting Price Wizard scraping")
        logger.info("=" * 60)

        df_price_wizard = pd.read_excel(
            input_file, header=0, index_col=0,
            sheet_name=config["sheets"]["price_wizard"],
        )

        price_wizard_scraper = PriceWizardScraper(driver, config)
        price_wizard_scraper.scrape(df_price_wizard, output_base_dir, downloads_dir)

    finally:
        driver.quit()
        logger.info("Browser closed")


def run_processing(config: dict) -> None:
    """Process downloaded files and create consolidated output.

    Args:
        config: Application configuration dictionary.
    """
    output_base_dir = get_env_path("BASE_OUTPUT_DIR")
    input_file = config["input_file"]
    output_file = config["output_file"]

    processor = DataProcessor(config)

    # Process Price Index files
    price_index_dir = os.path.join(output_base_dir, "price_index")
    price_index_df = processor.process_price_index_files(price_index_dir)

    # Process Price Wizard files
    price_wizard_dir = os.path.join(output_base_dir, "price_wizard")
    price_wizard_df = processor.process_price_wizard_files(price_wizard_dir)

    # Load rename mapping from input Excel
    df_mapping = pd.read_excel(
        input_file, header=0, index_col=0,
        sheet_name=config["sheets"]["price_index"],
    )
    rename_mapping = {}
    if "Старое название" in df_mapping.columns and "Название котировки ChemOrbis" in df_mapping.columns:
        rename_mapping = dict(
            zip(df_mapping["Старое название"], df_mapping["Название котировки ChemOrbis"])
        )

    # Consolidate
    consolidated = processor.consolidate(
        price_index_df, price_wizard_df, rename_mapping
    )

    # Save and format
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    consolidated.to_excel(output_file, index=False)
    format_output_excel(output_file)

    logger.info(f"Output saved: {output_file} ({len(consolidated)} rows)")
    # Cохраняем в SQLite
    with Database() as db:
        new_rows = db.insert_dataframe(consolidated)
        summary = db.get_summary()
        logger.info(f"Database summary: {summary}")


def main() -> None:
    """Main entry point for the application."""
    args = parse_args()
    setup_logging()

    logger.info("ChemOrbis Price Data Scraper started")

    config = load_config(args.config)

    try:
        if not args.process_only:
            run_scrapers(config)

        if not args.scrape_only:
            run_processing(config)

        logger.info("Pipeline completed successfully")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()