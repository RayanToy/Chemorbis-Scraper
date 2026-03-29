"""Data processing module for consolidating scraped ChemOrbis data."""

import os
import re
import glob
import logging

import pandas as pd

from src.utils import remove_extra_spaces

logger = logging.getLogger(__name__)


class DataProcessor:
    """Processes and consolidates raw scraped data files.

    Handles both Excel files (Price Index) and CSV files (Price Wizard),
    performing data cleaning, normalization, and merging.
    """

    def __init__(self, config: dict):
        """Initialize processor with configuration.

        Args:
            config: Application configuration dictionary.
        """
        self.config = config

    def process_price_index_files(self, directory: str) -> pd.DataFrame:
        """Process all Excel files from Price Index scraper.

        Reads .xls files, extracts quote names from the last row,
        unpivots price levels, and normalizes data.

        Args:
            directory: Root directory containing scraped .xls files.

        Returns:
            Consolidated DataFrame with columns:
            Year, Week, Week Start Date, Date, Level, Price, Quote, Agency.
        """
        excel_files = glob.glob(
            os.path.join(directory, "**", "*.xls"), recursive=True
        )
        logger.info(f"Found {len(excel_files)} Price Index Excel files")

        dataframes = []

        for file_path in excel_files:
            try:
                df = self._process_single_excel(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")

        if not dataframes:
            logger.warning("No Price Index data processed")
            return pd.DataFrame()

        result = pd.concat(dataframes, ignore_index=True)
        result["Agency"] = "Chemorbis"

        # Rename and clean
        result = result.rename(columns={"Week Publish Date": "Date"})
        result = result.dropna(subset=["Price"])
        result = result.drop_duplicates(subset=["Quote", "Date", "Level"])

        # Parse dates
        result["Date"] = pd.to_datetime(result["Date"], format="%d-%m-%y")
        result["Week Start Date"] = pd.to_datetime(
            result["Week Start Date"], format="%d-%m-%y"
        )
        result["Month"] = result["Date"].dt.month

        # Remove holiday entries
        result = result.loc[result["Price"] != "Holiday"]

        # Convert prices (handle comma as decimal separator + thousands)
        result["Price"] = result["Price"].apply(self._convert_price)

        return result

    def process_price_wizard_files(self, directory: str) -> pd.DataFrame:
        """Process all CSV files from Price Wizard scraper.

        Reads CSV files, unpivots quote columns, and normalizes dates.

        Args:
            directory: Root directory containing scraped .csv files.

        Returns:
            Consolidated DataFrame with standardized columns.
        """
        csv_files = glob.glob(
            os.path.join(directory, "**", "*.csv"), recursive=True
        )
        logger.info(f"Found {len(csv_files)} Price Wizard CSV files")

        dataframes = []

        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path, sep=";")
                quote_headers = df.columns[1:]
                df = pd.melt(
                    df,
                    id_vars=df.columns[0],
                    value_vars=quote_headers,
                    var_name="Quote",
                    value_name="Price",
                )
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")

        if not dataframes:
            logger.warning("No Price Wizard data processed")
            return pd.DataFrame()

        result = pd.concat(dataframes, ignore_index=True)

        # Split date and week number columns
        if "Date (Week #)" in result.columns:
            result[["Date", "Week #"]] = result["Date (Week #)"].str.split(
                " ", expand=True
            )
            result.drop(columns=["Date (Week #)"], inplace=True)

        # Parse dates
        result["Date"] = pd.to_datetime(result["Date"], format="%d/%m/%y")

        if "Date (Month #)" in result.columns:
            result["Date (Month #)"] = pd.to_datetime(result["Date (Month #)"])
            result["Date"].fillna(result["Date (Month #)"], inplace=True)
            result.drop(columns=["Date (Month #)"], inplace=True)

        if "Week #" in result.columns:
            result.drop(columns=["Week #"], inplace=True)

        # Add metadata columns
        result["Agency"] = "Chemorbis"
        result["Month"] = result["Date"].dt.month
        result["Year"] = result["Date"].dt.year
        result["Week"] = result["Date"].dt.isocalendar().week
        result["Week Start Date"] = None
        result["Level"] = "Avg"

        result["Date"] = result["Date"].dt.date
        result = result.dropna(subset=["Price"])
        result = result.drop_duplicates(subset=["Quote", "Date"])
        result.sort_values(by=["Quote", "Date"], ascending=True, inplace=True)

        return result

    def consolidate(
        self,
        price_index_df: pd.DataFrame,
        price_wizard_df: pd.DataFrame,
        rename_mapping: dict,
    ) -> pd.DataFrame:
        """Merge and clean both data sources into final dataset.

        Args:
            price_index_df: Processed Price Index data.
            price_wizard_df: Processed Price Wizard data.
            rename_mapping: Dictionary mapping old quote names to new ones.

        Returns:
            Final consolidated and cleaned DataFrame.
        """
        combined = pd.concat([price_wizard_df, price_index_df])
        combined["Date"] = pd.to_datetime(combined["Date"])

        # Remove holiday entries
        combined = combined.loc[combined["Price"] != "Holiday"]

        # Normalize level names
        combined["Level"] = combined["Level"].replace("Avg.", "Avg")

        # Set default currency and UOM
        combined["Currency"] = "USD"
        combined["UOM"] = "mt"

        # Apply quote name replacements from config
        quote_replacements = self.config.get("quote_replacements", {})
        combined["Quote"] = combined["Quote"].replace(quote_replacements)

        # Apply rename mapping from Excel reference
        if rename_mapping:
            combined["Quote"] = combined["Quote"].replace(rename_mapping)

        # Set EUR currency for specific quotes
        eur_quotes = self.config.get("eur_currency_quotes", [])
        for quote in eur_quotes:
            combined.loc[combined["Quote"] == quote, "Currency"] = "EUR"

        # Clean extra spaces in quote names
        exclude_quotes = set(
            self.config.get("exclude_from_space_cleanup", [])
        )
        combined["Quote"] = combined["Quote"].apply(
            lambda q: q if q in exclude_quotes else remove_extra_spaces(q)
        )

        # Remove unwanted quotes
        quotes_to_remove = self.config.get("quotes_to_remove", [])
        combined = combined[~combined["Quote"].isin(quotes_to_remove)]

        # Remove specific rows (GPPS without Week Start Date)
        mask = (
            (combined["Quote"] == "GPPS Injection - Import - CIF China Main Port/Hong Kong")
            & (combined["Week Start Date"].isna())
        )
        removed_count = mask.sum()
        if removed_count > 0:
            logger.info(f"Removed {removed_count} GPPS rows without Week Start Date")
        combined = combined[~mask]

        # Parse dates for output
        combined["Date"] = pd.to_datetime(combined["Date"]).dt.date
        combined["Week Start Date"] = pd.to_datetime(
            combined["Week Start Date"]
        ).dt.date

        # Select and order final columns
        output_columns = [
            "Quote", "Currency", "UOM", "Level", "Price",
            "Date", "Agency", "Month", "Year", "Week", "Week Start Date",
        ]
        combined = combined[output_columns]

        logger.info(f"Consolidated dataset: {len(combined)} rows")
        return combined

    @staticmethod
    def _process_single_excel(file_path: str) -> pd.DataFrame:
        """Process a single Price Index Excel file.

        Args:
            file_path: Path to the .xls file.

        Returns:
            Unpivoted DataFrame with quote name added.
        """
        logger.debug(f"Processing: {file_path}")
        df = pd.read_excel(file_path, header=0)

        # Quote name is stored in the last row, 5th column
        quote_name = df.iloc[-1, 4]
        df = df.drop(df.tail(1).index)

        df.columns = [
            "Year", "Week", "Week Start Date",
            "Week Publish Date", "Low", "Avg.", "High",
        ]

        # Unpivot price levels
        df = pd.melt(
            df,
            id_vars=["Year", "Week", "Week Start Date", "Week Publish Date"],
            value_vars=["Low", "Avg.", "High"],
            var_name="Level",
            value_name="Price",
        )

        df["Quote"] = quote_name
        return df

    @staticmethod
    def _convert_price(price) -> float:
        """Convert price string to float, handling comma decimals.

        Prices with commas are assumed to be in thousands
        (e.g., '1,234' → 1234.0).

        Args:
            price: Price value (string or numeric).

        Returns:
            Converted float value.
        """
        if isinstance(price, str):
            if "," in price:
                return float(price.replace(",", ".")) * 1000
            return float(price)
        return float(price)