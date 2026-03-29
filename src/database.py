"""Database module for storing and querying price data."""

import sqlite3
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = "data/chemorbis.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quote TEXT NOT NULL,
    currency TEXT NOT NULL,
    uom TEXT NOT NULL,
    level TEXT NOT NULL,
    price REAL NOT NULL,
    date DATE NOT NULL,
    agency TEXT NOT NULL,
    month INTEGER,
    year INTEGER,
    week INTEGER,
    week_start_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(quote, date, level)
);

CREATE INDEX IF NOT EXISTS idx_quote ON quotes(quote);
CREATE INDEX IF NOT EXISTS idx_date ON quotes(date);
CREATE INDEX IF NOT EXISTS idx_quote_date ON quotes(quote, date);

CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT NOT NULL,
    territory TEXT,
    product_group TEXT,
    rows_added INTEGER,
    status TEXT NOT NULL
);
"""


class Database:
    """SQLite database for ChemOrbis price data.

    Supports incremental loading — only new records are inserted,
    duplicates are skipped based on (quote, date, level) uniqueness.
    """

    def __init__(self, db_path: str = DB_PATH):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._create_tables()
        logger.info(f"Database connected: {db_path}")

    def _create_tables(self) -> None:
        """Create tables if they don't exist."""
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def insert_dataframe(self, df: pd.DataFrame, source: str = "scraper") -> int:
        """Insert DataFrame into quotes table, skipping duplicates.

        Args:
            df: DataFrame with price data.
            source: Data source identifier for the log.

        Returns:
            Number of new rows inserted.
        """
        before_count = self._row_count()

        df_db = df.rename(columns={
            "Quote": "quote",
            "Currency": "currency",
            "UOM": "uom",
            "Level": "level",
            "Price": "price",
            "Date": "date",
            "Agency": "agency",
            "Month": "month",
            "Year": "year",
            "Week": "week",
            "Week Start Date": "week_start_date",
        })

        df_db.to_sql(
            "quotes",
            self.conn,
            if_exists="append",
            index=False,
            method="multi",
        )

        after_count = self._row_count()
        new_rows = after_count - before_count

        # Log the scrape
        self.conn.execute(
            "INSERT INTO scrape_log (source, rows_added, status) VALUES (?, ?, ?)",
            (source, new_rows, "success"),
        )
        self.conn.commit()

        logger.info(f"Inserted {new_rows} new rows (total: {after_count})")
        return new_rows

    def query(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.

        Args:
            sql: SQL query string.
            params: Query parameters.

        Returns:
            Query results as DataFrame.
        """
        return pd.read_sql_query(sql, self.conn, params=params)

    def get_latest_prices(self, quote: Optional[str] = None) -> pd.DataFrame:
        """Get the most recent price for each quote.

        Args:
            quote: Optional filter by quote name.

        Returns:
            DataFrame with latest prices.
        """
        sql = """
            SELECT quote, currency, level, price, date
            FROM quotes
            WHERE date = (SELECT MAX(date) FROM quotes q2 WHERE q2.quote = quotes.quote)
        """
        if quote:
            sql += " AND quote LIKE ?"
            return self.query(sql, (f"%{quote}%",))
        return self.query(sql)

    def get_price_history(
        self, quote: str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """Get price history for a specific quote.

        Args:
            quote: Quote name (partial match).
            start_date: Optional start date filter (YYYY-MM-DD).
            end_date: Optional end date filter (YYYY-MM-DD).

        Returns:
            DataFrame with price history sorted by date.
        """
        sql = "SELECT * FROM quotes WHERE quote LIKE ?"
        params = [f"%{quote}%"]

        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)

        sql += " ORDER BY date"
        return self.query(sql, tuple(params))

    def get_scrape_history(self) -> pd.DataFrame:
        """Get log of all scraping runs.

        Returns:
            DataFrame with scrape history.
        """
        return self.query("SELECT * FROM scrape_log ORDER BY scrape_date DESC")

    def get_summary(self) -> dict:
        """Get database summary statistics.

        Returns:
            Dictionary with summary stats.
        """
        stats = {}
        stats["total_rows"] = self._row_count()
        stats["unique_quotes"] = self.query(
            "SELECT COUNT(DISTINCT quote) as cnt FROM quotes"
        )["cnt"].iloc[0]
        stats["date_range"] = self.query(
            "SELECT MIN(date) as min_date, MAX(date) as max_date FROM quotes"
        ).iloc[0].to_dict()
        stats["scrape_runs"] = self.query(
            "SELECT COUNT(*) as cnt FROM scrape_log"
        )["cnt"].iloc[0]
        return stats

    def _row_count(self) -> int:
        """Get total row count in quotes table."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM quotes")
        return cursor.fetchone()[0]

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()
        logger.info("Database connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()