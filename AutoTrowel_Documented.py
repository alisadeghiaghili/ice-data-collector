# -*- coding: utf-8 -*-
"""
AutoTrowel - ICE.ir Currency History Scraper with Incremental Load

A production-grade ETL pipeline for extracting currency exchange rates from Iran
Commodity Exchange (ICE.ir) API with intelligent incremental loading and SQL Server integration.

Author: sadeghi.a
Created: Sun Jan 25 17:08:08 2026
Version: 2.0.0

Features:
    - Fetches historical currency data from ICE.ir API
    - Supports pagination for complete data retrieval
    - Converts Persian/Jalali dates to standard format
    - Implements incremental loading to avoid duplicates
    - Stores data in SQL Server with proper schema
    - Handles multiple currency types (Bill/WireTransfer)
    - Comprehensive error handling and retry logic
    - Detailed logging for debugging and monitoring
    - Configuration management via environment variables
    - Progress tracking and performance metrics
    
Dependencies:
    - requests: HTTP client for API calls
    - pandas: Data manipulation and analysis
    - jdatetime: Jalali/Gregorian date conversion
    - sqlalchemy: Database ORM and connection management
    - pyodbc: SQL Server ODBC driver
    - tenacity: Retry logic for robustness
    
Usage:
    python AutoTrowel_Documented.py
    
    Or programmatically:
    >>> from AutoTrowel_Documented import CurrencyETL
    >>> etl = CurrencyETL(connection_string="...")
    >>> etl.run()
"""

import os
import sys
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime
from pathlib import Path
import re

import requests
import numpy as np
import pandas as pd
import jdatetime
from sqlalchemy import create_engine, CHAR, NVARCHAR, BigInteger, DATETIME
from sqlalchemy.exc import SQLAlchemyError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)


# Setup logging
def setup_logging(log_level: int = logging.INFO) -> logging.Logger:
    """Configure logging with file and console handlers."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(
        log_dir / f"autotrowel_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


logger = setup_logging()


class JDate:
    """
    Adapter class for handling Jalali (Persian) date conversions.
    
    Attributes:
        value: The input date value in various formats
    
    Examples:
        >>> jd = JDate('14010101')
        >>> jd.format('Y-m-d')
        '1401-01-01'
    """
    
    def __init__(self, value: Any):
        """Initialize JDate with a date value."""
        self.value = value

    def format(self, fmt: str) -> Optional[str]:
        """
        Format the stored date value according to the provided format string.
        
        Args:
            fmt: Format string (e.g., 'Y-m-d', 'Y/m/d')
        
        Returns:
            Formatted Jalali date string, or None if value is invalid
        """
        if not self.value or self.value == 'null':
            return None
        
        try:
            # 8-digit string format (YYYYMMDD)
            if isinstance(self.value, str) and self.value.isdigit() and len(self.value) == 8:
                jd = jdatetime.date(
                    int(self.value[0:4]),
                    int(self.value[4:6]),
                    int(self.value[6:8])
                )
            # ISO date string format (YYYY-MM-DD)
            elif isinstance(self.value, str) and '-' in self.value:
                g = datetime.strptime(self.value, "%Y-%m-%d")
                jd = jdatetime.date.fromgregorian(date=g)
            # datetime object
            elif isinstance(self.value, datetime):
                jd = jdatetime.date.fromgregorian(date=self.value)
            else:
                logger.warning(f"Unsupported date format: {self.value}")
                return None
            
            return jd.strftime(
                fmt.replace('Y', '%Y')
                   .replace('m', '%m')
                   .replace('d', '%d')
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Date formatting error for '{self.value}': {e}")
            return None


class Config:
    """Configuration manager for AutoTrowel ETL pipeline."""
    
    BASE_URL = "https://api.ice.ir/api/v1/markets/{market}/currencies/history/{currency_id}/"
    PAGE_SIZE = 1000
    API_TIMEOUT = 30
    MAX_RETRIES = 3
    
    # Bill = physical banknotes, WireTransfer = electronic transfer
    CURRENCY_TYPES = {
        14: 'Bill', 15: 'WireTransfer',      # USD
        18: 'Bill', 19: 'WireTransfer',      # AED
        26: 'Bill', 27: 'WireTransfer',      # JPY
        34: 'Bill', 35: 'WireTransfer',      # EUR
        38: 'Bill', 39: 'WireTransfer',      # RUB
        42: 'Bill', 43: 'WireTransfer',      # CNY
        50: 'Bill', 51: 'WireTransfer',      # IQD
        71: 'Bill', 72: 'WireTransfer',      # INR
    }
    
    CURRENCY_TYPE_FA = {
        'Bill': 'اسکناس',
        'WireTransfer': 'حواله'
    }
    
    CURRENCY_META = {
        14: {'symbol': 'USD', 'name': 'دلار آمریکا'},
        15: {'symbol': 'USD', 'name': 'دلار آمریکا'},
        34: {'symbol': 'EUR', 'name': 'یورو'},
        35: {'symbol': 'EUR', 'name': 'یورو'},
        18: {'symbol': 'AED', 'name': 'درهم امارات'},
        19: {'symbol': 'AED', 'name': 'درهم امارات'},
        26: {'symbol': 'JPY', 'name': 'ین ژاپن'},
        27: {'symbol': 'JPY', 'name': 'ین ژاپن'},
        38: {'symbol': 'RUB', 'name': 'روبل روسیه'},
        39: {'symbol': 'RUB', 'name': 'روبل روسیه'},
        42: {'symbol': 'CNY', 'name': 'یوان چین'},
        43: {'symbol': 'CNY', 'name': 'یوان چین'},
        50: {'symbol': 'IQD', 'name': 'دینار عراق'},
        51: {'symbol': 'IQD', 'name': 'دینار عراق'},
        71: {'symbol': 'INR', 'name': 'روپیه هند'},
        72: {'symbol': 'INR', 'name': 'روپیه هند'},
    }


class CurrencyETL:
    """Main ETL pipeline for currency data extraction and loading."""
    
    def __init__(self, connection_string: Optional[str] = None, table_name: str = 'IceAssets'):
        """
        Initialize the ETL pipeline.
        
        Args:
            connection_string: SQL Server connection string (or from env var)
            table_name: Target database table name
        """
        self.connection_string = connection_string or os.getenv(
            'DB_CONNECTION_STRING',
            "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        )
        self.table_name = table_name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        logger.info(f"CurrencyETL initialized - Table: {table_name}")
    
    @staticmethod
    def clean_persian_number(text: Any) -> Optional[str]:
        """Convert Persian digits to English and remove non-numeric characters."""
        if not text or pd.isna(text):
            return None
        
        try:
            text = str(text)
            trans = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
            text = text.translate(trans)
            cleaned = re.sub(r'[^\d]', '', text)
            return cleaned if cleaned else None
        except Exception as e:
            logger.warning(f"Failed to clean number '{text}': {e}")
            return None
    
    @staticmethod
    def correct_date_format(jalali_date: Any) -> Optional[str]:
        """Convert YYYYMMDD format to YYYY-MM-DD format."""
        if not jalali_date:
            return None
        
        try:
            jalali_date = str(jalali_date)
            if len(jalali_date) == 8:
                return f"{jalali_date[:4]}-{jalali_date[4:6]}-{jalali_date[6:8]}"
            return jalali_date
        except Exception as e:
            logger.warning(f"Failed to format date '{jalali_date}': {e}")
            return None
    
    def build_currency_urls(self) -> List[Dict[str, Any]]:
        """Generate list of API endpoint URLs for all configured currencies."""
        urls = []
        for currency_id, ctype in Config.CURRENCY_TYPES.items():
            market = 1 if ctype == 'Bill' else 2
            urls.append({
                'currency_id': currency_id,
                'ctype': ctype,
                'url': Config.BASE_URL.format(market=market, currency_id=currency_id)
            })
        return urls
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
        reraise=True
    )
    def fetch_currency_history(self, url: str) -> List[Dict]:
        """
        Fetch complete currency history from ICE.ir API with automatic pagination.
        
        Args:
            url: Base API endpoint URL
        
        Returns:
            List of all currency history records
        
        Raises:
            requests.RequestException: If API request fails after retries
        """
        all_results = []
        offset = 0
        
        logger.debug(f"Starting to fetch data from: {url}")
        
        while True:
            params = {
                'lang': 'fa',
                'limit': Config.PAGE_SIZE,
                'offset': offset
            }
            
            try:
                response = self.session.get(url, params=params, timeout=Config.API_TIMEOUT)
                response.raise_for_status()
                
                data = response.json()
                results = data.get('results', [])
                count = data.get('count', 0)
                
                if not results:
                    break
                
                all_results.extend(results)
                offset += Config.PAGE_SIZE
                
                logger.debug(f"Fetched {len(results)} records (offset={offset}, total={count})")
                
                if offset >= count:
                    break
                    
            except requests.Timeout:
                logger.warning(f"Timeout fetching from {url}, retrying...")
                raise
            except requests.RequestException as e:
                logger.error(f"Request error: {e}")
                raise
        
        logger.info(f"Completed fetching {len(all_results)} total records")
        return all_results
    
    def parse_item(self, item: Dict, now: datetime, currency_id: int) -> Optional[Dict]:
        """
        Parse a single API response item into standardized database record format.
        
        Args:
            item: Raw API response item
            now: Current timestamp for scrape tracking
            currency_id: Currency identifier
        
        Returns:
            Standardized record dictionary or None if parsing fails
        """
        try:
            meta = Config.CURRENCY_META.get(currency_id)
            if not meta:
                logger.warning(f"No metadata found for currency_id={currency_id}")
                return None
            
            ctype = Config.CURRENCY_TYPES.get(currency_id)
            sell = item.get('sell_price')
            buy = item.get('buy_price')
            
            return {
                'Date': JDate(item.get('date')).format('Y-m-d'),
                'Name': meta['name'],
                'SellPrice': int(float(sell)) if sell and sell != 'null' else None,
                'BuyPrice': int(float(buy)) if buy and buy != 'null' else None,
                'Symbol': meta['symbol'],
                'PersianCurrencyType': Config.CURRENCY_TYPE_FA.get(ctype),
                'EnglishCurrencyType': ctype,
                'PersianAssetType': 'ارز',
                'EnglishAssetType': 'Currency',
                'ScrapeDate': now.strftime('%Y-%m-%d'),
                'Scrapetime': now.strftime('%H:%M:%S'),
                'ScrapeDateTime': now.strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            logger.error(f"Failed to parse item {item}: {e}")
            return None
    
    def process_all_currency_data(self) -> pd.DataFrame:
        """
        Fetch and process currency data for all configured currencies.
        
        Returns:
            DataFrame with processed currency data
        """
        now = datetime.now()
        all_data = []
        urls = self.build_currency_urls()
        
        logger.info(f"Processing {len(urls)} currency endpoints...")
        
        for idx, meta in enumerate(urls, 1):
            currency_id = meta['currency_id']
            ctype = meta['ctype']
            
            logger.info(f"[{idx}/{len(urls)}] Fetching currency_id={currency_id} ({ctype})")
            
            try:
                items = self.fetch_currency_history(meta['url'])
                logger.info(f"  ✓ Retrieved {len(items)} records")
                
                for item in items:
                    parsed = self.parse_item(item, now, currency_id)
                    if parsed:
                        all_data.append(parsed)
                        
            except Exception as e:
                logger.error(f"  ✗ Failed to fetch currency_id={currency_id}: {e}")
                continue
        
        if not all_data:
            logger.warning("No data collected from API")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        logger.info(f"Created DataFrame with {len(df)} rows")
        
        # Clean dates
        df['Date'] = df['Date'].apply(self.clean_persian_number).apply(self.correct_date_format)
        
        # Replace various null representations
        df = df.replace([np.nan, "", "nan--"], None)
        
        # Validate data
        null_dates = df['Date'].isna().sum()
        if null_dates > 0:
            logger.warning(f"Found {null_dates} records with null dates")
        
        return df
    
    def load_existing_keys(self) -> pd.DataFrame:
        """
        Load existing record keys from database for deduplication.
        
        Returns:
            DataFrame with existing keys
        """
        try:
            engine = create_engine(self.connection_string)
            query = f"""
                SELECT [Date], [Symbol], [EnglishCurrencyType]
                FROM {self.table_name}
                WHERE [EnglishAssetType] = 'Currency'
            """
            df_keys = pd.read_sql(query, engine)
            logger.info(f"Loaded {len(df_keys)} existing keys from database")
            return df_keys
        except SQLAlchemyError as e:
            logger.error(f"Database error loading keys: {e}")
            raise
    
    def find_new_records(self, df_new: pd.DataFrame, df_existing_keys: pd.DataFrame) -> pd.DataFrame:
        """
        Identify new records by comparing against existing database keys.
        
        Args:
            df_new: New records fetched from API
            df_existing_keys: Existing keys from database
        
        Returns:
            Subset of df_new containing only new records
        """
        if df_existing_keys.empty:
            logger.info("No existing records in database - all records are new")
            return df_new
        
        key_cols = ['Date', 'Symbol', 'EnglishCurrencyType']
        
        df_new_keys = df_new[key_cols].copy()
        df_existing_keys = df_existing_keys[key_cols].copy()
        
        df_merged = df_new_keys.merge(
            df_existing_keys,
            on=key_cols,
            how='left',
            indicator=True
        )
        
        new_mask = df_merged['_merge'] == 'left_only'
        df_delta = df_new.loc[new_mask].copy()
        
        logger.info(f"Found {len(df_delta)} new records out of {len(df_new)} total")
        return df_delta
    
    def save_to_database(self, df: pd.DataFrame) -> bool:
        """
        Save DataFrame to SQL Server database with proper schema.
        
        Args:
            df: DataFrame to save
        
        Returns:
            True if successful, False otherwise
        """
        if df.empty:
            logger.info("No data to save")
            return True
        
        try:
            logger.info(f"Saving {len(df)} rows to database table '{self.table_name}'...")
            
            engine = create_engine(self.connection_string, fast_executemany=True)
            
            df.to_sql(
                self.table_name,
                engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                dtype={
                    'Date': CHAR(10),
                    'Name': NVARCHAR(100),
                    'SellPrice': BigInteger(),
                    'BuyPrice': BigInteger(),
                    'ScrapeDate': CHAR(10),
                    'Scrapetime': CHAR(8),
                    'Symbol': CHAR(3),
                    'PersianCurrencyType': NVARCHAR(50),
                    'EnglishCurrencyType': NVARCHAR(50),
                    'PersianAssetType': NVARCHAR(50),
                    'EnglishAssetType': NVARCHAR(50),
                    'ScrapeDateTime': DATETIME()
                }
            )
            
            logger.info(f"✓ Successfully inserted {len(df)} rows into {self.table_name}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Database error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving to database: {e}")
            return False
    
    def run(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("Starting AutoTrowel ETL Pipeline")
        logger.info("=" * 60)
        
        try:
            # Step 1: Fetch fresh data from API
            logger.info("[1/4] Fetching fresh data from ICE.ir API...")
            df_new = self.process_all_currency_data()
            
            if df_new.empty:
                logger.warning("No data fetched from API - aborting")
                return False
            
            # Step 2: Load existing keys from database
            logger.info("[2/4] Loading existing keys from database...")
            df_existing_keys = self.load_existing_keys()
            
            # Step 3: Detect new records
            logger.info("[3/4] Detecting new records...")
            df_delta = self.find_new_records(df_new, df_existing_keys)
            
            if df_delta.empty:
                logger.info("✓ Database already up to date - no new records to insert")
                return True
            
            # Step 4: Save to database
            logger.info(f"[4/4] Inserting {len(df_delta)} new records...")
            success = self.save_to_database(df_delta)
            
            # Summary
            duration = datetime.now() - start_time
            logger.info("=" * 60)
            logger.info(f"Pipeline completed in {duration.total_seconds():.2f} seconds")
            logger.info(f"Status: {'SUCCESS' if success else 'FAILED'}")
            logger.info("=" * 60)
            
            return success
            
        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted by user")
            return False
        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}", exc_info=True)
            return False
        finally:
            self.session.close()


# Legacy functions for backward compatibility
def incremental_load(connection_string: str, table_name: str = 'IceAssets') -> None:
    """
    Legacy function for backward compatibility.
    
    Args:
        connection_string: SQL Server connection string
        table_name: Target table name
    """
    etl = CurrencyETL(connection_string, table_name)
    success = etl.run()
    sys.exit(0 if success else 1)


def main():
    """Main entry point."""
    try:
        connection_string = os.getenv(
            'DB_CONNECTION_STRING',
            "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        )
        
        etl = CurrencyETL(connection_string)
        success = etl.run()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
