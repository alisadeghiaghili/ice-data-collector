# -*- coding: utf-8 -*-
"""
AutoTrowel - ICE.ir Currency History Scraper with Incremental Load

A production-grade ETL pipeline for extracting currency exchange rates from Iran
Commodity Exchange (ICE.ir) API with intelligent incremental loading and SQL Server integration.

Author: sadeghi.a
Created: Sun Jan 25 17:08:08 2026
Version: 1.0.0

Features:
    - Fetches historical currency data from ICE.ir API
    - Supports pagination for complete data retrieval
    - Converts Persian/Jalali dates to standard format
    - Implements incremental loading to avoid duplicates
    - Stores data in SQL Server with proper schema
    - Handles multiple currency types (Bill/WireTransfer)
    
Dependencies:
    - requests: HTTP client for API calls
    - pandas: Data manipulation and analysis
    - jdatetime: Jalali/Gregorian date conversion
    - sqlalchemy: Database ORM and connection management
    - pyodbc: SQL Server ODBC driver
    
Usage:
    python AutoTrowel.py
    
    Or programmatically:
    >>> from AutoTrowel import incremental_load
    >>> CONNECTION_STRING = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
    >>> incremental_load(CONNECTION_STRING)
"""

import requests
import numpy as np
import pandas as pd
import jdatetime
from datetime import datetime
from sqlalchemy import create_engine, CHAR, NVARCHAR, BigInteger, DATETIME
import re

# ======================================================
# JDate Adapter
# ======================================================

class JDate:
    """
    Adapter class for handling Jalali (Persian) date conversions.
    
    This class provides a flexible interface for converting various date formats
    (string dates, datetime objects, Gregorian dates) to Jalali format with
    customizable output formatting.
    
    Attributes:
        value: The input date value in various formats (str, datetime, int)
    
    Examples:
        >>> jd = JDate('14010101')
        >>> jd.format('Y-m-d')
        '1401-01-01'
        
        >>> jd = JDate(datetime(2022, 3, 21))
        >>> jd.format('Y/m/d')
        '1401/01/01'
    """
    
    def __init__(self, value):
        """
        Initialize JDate with a date value.
        
        Args:
            value: Date in various formats:
                - String (YYYYMMDD format): e.g., '14010525'
                - String (YYYY-MM-DD format): e.g., '2022-08-16'
                - datetime object: datetime(2022, 8, 16)
                - None or 'null': For missing dates
        """
        self.value = value

    def format(self, fmt: str):
        """
        Format the stored date value according to the provided format string.
        
        Args:
            fmt (str): Format string where:
                - 'Y' represents year (4 digits)
                - 'm' represents month (2 digits)
                - 'd' represents day (2 digits)
                Example: 'Y-m-d', 'Y/m/d'
        
        Returns:
            str: Formatted Jalali date string, or None if value is invalid
        
        Raises:
            ValueError: If the date string format is invalid
        
        Examples:
            >>> JDate('14010101').format('Y-m-d')
            '1401-01-01'
            
            >>> JDate(datetime(2022, 3, 21)).format('Y/m/d')
            '1401/01/01'
        """
        if not self.value or self.value == 'null':
            return None
        
        # Handle 8-digit string format (YYYYMMDD)
        if isinstance(self.value, str) and self.value.isdigit() and len(self.value) == 8:
            jd = jdatetime.date(
                int(self.value[0:4]),
                int(self.value[4:6]),
                int(self.value[6:8])
            )
        # Handle ISO date string format (YYYY-MM-DD)
        elif isinstance(self.value, str) and '-' in self.value:
            g = datetime.strptime(self.value, "%Y-%m-%d")
            jd = jdatetime.date.fromgregorian(date=g)
        # Handle datetime object
        elif isinstance(self.value, datetime):
            jd = jdatetime.date.fromgregorian(date=self.value)
        else:
            return None
        
        # Convert custom format to strftime format
        return jd.strftime(
            fmt.replace('Y', '%Y')
               .replace('m', '%m')
               .replace('d', '%d')
        )


# ======================================================
# Configuration Constants
# ======================================================

# API endpoint template for currency history
BASE_URL = "https://api.ice.ir/api/v1/markets/{market}/currencies/history/{currency_id}/"

# Number of records to fetch per API call (matches API maximum)
PAGE_SIZE = 1000

# Mapping of currency IDs to their transaction types
# Bill = Physical banknotes, WireTransfer = Electronic transfer
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

# Persian translations for currency types
CURRENCY_TYPE_FA = {
    'Bill': 'اسکناس',
    'WireTransfer': 'حواله'
}

# Master currency metadata - source of truth for currency identification
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


# ======================================================
# Helper Functions
# ======================================================

def clean_persian_number(text):
    """
    Convert Persian/Farsi digits to English digits and remove non-numeric characters.
    
    Persian digits (۰۱۲۳۴۵۶۷۸۹) are commonly used in Iranian APIs and need to be
    converted to standard English digits (0123456789) for processing.
    
    Args:
        text (str): String potentially containing Persian digits
    
    Returns:
        str: String with only English digits, or None if no digits found
    
    Examples:
        >>> clean_persian_number('۱۴۰۱۰۵۲۵')
        '14010525'
        
        >>> clean_persian_number('قیمت: ۱۲۳,۴۵۶')
        '123456'
        
        >>> clean_persian_number(None)
        None
    """
    if not text:
        return None
    
    # Create translation table for Persian to English digits
    trans = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
    text = text.translate(trans)
    
    # Remove all non-digit characters
    return re.sub(r'[^\d]', '', text) or None


def correct_date_format(jalali_date):
    """
    Convert YYYYMMDD format to YYYY-MM-DD format.
    
    Args:
        jalali_date (str): Date string in YYYYMMDD format (e.g., '14010525')
    
    Returns:
        str: Date string in YYYY-MM-DD format (e.g., '1401-05-25'), or None if input is None
    
    Examples:
        >>> correct_date_format('14010525')
        '1401-05-25'
        
        >>> correct_date_format(None)
        None
    """
    if not jalali_date:
        return None
    return f"{jalali_date[:4]}-{jalali_date[4:6]}-{jalali_date[6:8]}"


# ======================================================
# URL Builder
# ======================================================

def build_currency_urls():
    """
    Generate list of API endpoint URLs for all configured currencies.
    
    Constructs URLs for fetching currency history from ICE.ir API based on
    currency IDs and types defined in CURRENCY_TYPES. Market ID is determined
    by transaction type: 1 for Bill (banknotes), 2 for WireTransfer.
    
    Returns:
        list[dict]: List of dictionaries containing:
            - currency_id (int): Unique currency identifier
            - ctype (str): Currency type ('Bill' or 'WireTransfer')
            - url (str): Complete API endpoint URL
    
    Examples:
        >>> urls = build_currency_urls()
        >>> urls[0]
        {
            'currency_id': 14,
            'ctype': 'Bill',
            'url': 'https://api.ice.ir/api/v1/markets/1/currencies/history/14/'
        }
    """
    urls = []
    for currency_id, ctype in CURRENCY_TYPES.items():
        # Market 1 = Bill (physical), Market 2 = WireTransfer (electronic)
        market = 1 if ctype == 'Bill' else 2
        urls.append({
            'currency_id': currency_id,
            'ctype': ctype,
            'url': BASE_URL.format(
                market=market,
                currency_id=currency_id
            )
        })
    return urls


# ======================================================
# API Fetching with Pagination
# ======================================================

def fetch_currency_history(url):
    """
    Fetch complete currency history from ICE.ir API with automatic pagination.
    
    The API returns paginated results with a maximum of PAGE_SIZE (1000) records
    per call. This function automatically fetches all pages until no more data
    is available.
    
    Args:
        url (str): Base API endpoint URL for the currency
    
    Returns:
        list[dict]: List of all currency history records, where each record contains:
            - date: Date of the record
            - sell_price: Selling price
            - buy_price: Buying price
            - slug: Currency slug identifier
            - id: Unique record ID
    
    Raises:
        requests.HTTPError: If API request fails
        requests.Timeout: If request times out (20 second timeout)
        requests.RequestException: For other network-related errors
    
    Examples:
        >>> url = "https://api.ice.ir/api/v1/markets/1/currencies/history/14/"
        >>> records = fetch_currency_history(url)
        >>> len(records)
        5432
    """
    all_results = []
    offset = 0
    
    while True:
        params = {
            'lang': 'fa',          # Farsi language for response
            'limit': PAGE_SIZE,    # Records per page
            'offset': offset       # Starting position
        }
        
        # Make API request with 20-second timeout
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()  # Raise exception for HTTP errors
        
        data = r.json()
        results = data.get('results', [])
        count = data.get('count', 0)
        
        # Stop if no more results
        if not results:
            break
        
        all_results.extend(results)
        offset += PAGE_SIZE
        
        # Stop if we've fetched all available records
        if offset >= count:
            break
    
    return all_results


# ======================================================
# Data Parser
# ======================================================

def parse_item(item, now, currency_id):
    """
    Parse a single API response item into standardized database record format.
    
    Converts raw API response data into a structured dictionary with proper
    data types, date formatting, and metadata enrichment using CURRENCY_META.
    
    Args:
        item (dict): Raw API response item containing:
            - date: Date string (various formats)
            - sell_price: Selling price (string or numeric)
            - buy_price: Buying price (string or numeric)
        now (datetime): Current timestamp for scrape tracking
        currency_id (int): Currency ID from CURRENCY_TYPES
    
    Returns:
        dict: Standardized record with keys:
            - Date (str): Formatted Jalali date (YYYY-MM-DD)
            - Name (str): Persian currency name
            - SellPrice (int): Selling price in Rials
            - BuyPrice (int): Buying price in Rials
            - Symbol (str): International currency symbol
            - PersianCurrencyType (str): Transaction type in Persian
            - EnglishCurrencyType (str): Transaction type in English
            - PersianAssetType (str): 'ارز' (Currency)
            - EnglishAssetType (str): 'Currency'
            - ScrapeDate (str): Date when scraped (YYYY-MM-DD)
            - Scrapetime (str): Time when scraped (HH:MM:SS)
            - ScrapeDateTime (str): Full timestamp (YYYY-MM-DD HH:MM:SS)
        
        Returns None if currency_id is not found in CURRENCY_META.
    
    Examples:
        >>> item = {'date': '14010525', 'sell_price': '28500', 'buy_price': '28300'}
        >>> now = datetime(2026, 2, 2, 14, 30, 0)
        >>> record = parse_item(item, now, 14)
        >>> record['Symbol']
        'USD'
        >>> record['SellPrice']
        28500
    """
    # Get currency metadata
    meta = CURRENCY_META.get(currency_id)
    if not meta:
        return None
    
    ctype = CURRENCY_TYPES.get(currency_id)
    sell = item.get('sell_price')
    buy = item.get('buy_price')
    
    return {
        'Date': JDate(item.get('date')).format('Y-m-d'),
        'Name': meta['name'],
        'SellPrice': int(float(sell)) if sell and sell != 'null' else None,
        'BuyPrice': int(float(buy)) if buy and buy != 'null' else None,
        'Symbol': meta['symbol'],
        'PersianCurrencyType': CURRENCY_TYPE_FA.get(ctype),
        'EnglishCurrencyType': ctype,
        'PersianAssetType': 'ارز',
        'EnglishAssetType': 'Currency',
        'ScrapeDate': now.strftime('%Y-%m-%d'),
        'Scrapetime': now.strftime('%H:%M:%S'),
        'ScrapeDateTime': now.strftime('%Y-%m-%d %H:%M:%S')
    }


# ======================================================
# Main Data Processor
# ======================================================

def process_all_currency_data():
    """
    Fetch and process currency data for all configured currencies.
    
    Main orchestration function that:
    1. Fetches data from all currency endpoints
    2. Parses and standardizes each record
    3. Cleans and formats dates
    4. Creates a consolidated pandas DataFrame
    
    Returns:
        pd.DataFrame: DataFrame with columns:
            - Date: Formatted date (YYYY-MM-DD)
            - Name: Currency name in Persian
            - SellPrice: Selling price (integer)
            - BuyPrice: Buying price (integer)
            - Symbol: Currency symbol (USD, EUR, etc.)
            - PersianCurrencyType: Type in Persian
            - EnglishCurrencyType: Type in English
            - PersianAssetType: 'ارز'
            - EnglishAssetType: 'Currency'
            - ScrapeDate: Date of scraping
            - Scrapetime: Time of scraping
            - ScrapeDateTime: Full timestamp
        
        Returns empty DataFrame if no data is fetched.
    
    Raises:
        requests.RequestException: If API requests fail
    
    Examples:
        >>> df = process_all_currency_data()
        >>> df.shape
        (87456, 12)
        >>> df['Symbol'].unique()
        array(['USD', 'EUR', 'AED', 'JPY', 'RUB', 'CNY', 'IQD', 'INR'])
    """
    now = datetime.now()
    all_data = []
    
    # Iterate through all configured currencies
    for meta in build_currency_urls():
        currency_id = meta['currency_id']
        print(f"Fetching currency_id={currency_id} ({meta['ctype']})")
        
        # Fetch data from API
        items = fetch_currency_history(meta['url'])
        print(f"  -> {len(items)} records")
        
        # Parse each item
        for item in items:
            parsed = parse_item(item, now, currency_id)
            if parsed:
                all_data.append(parsed)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    if df.empty:
        return df
    
    # Clean and format dates
    df['Date'] = (
        df['Date']
        .apply(clean_persian_number)
        .apply(correct_date_format)
    )
    
    # Replace various null representations with None
    return df.replace([np.nan, "", "nan--"], None)


def load_existing_keys(connection_string, table_name='IceAssets'):
    """
    Load existing record keys from database for deduplication.
    
    Retrieves composite keys (Date, Symbol, EnglishCurrencyType) of existing
    currency records to identify which new records should be inserted.
    
    Args:
        connection_string (str): SQLAlchemy connection string
        table_name (str, optional): Target table name. Defaults to 'IceAssets'
    
    Returns:
        pd.DataFrame: DataFrame with existing keys containing columns:
            - Date: Record date
            - Symbol: Currency symbol
            - EnglishCurrencyType: Transaction type
    
    Raises:
        sqlalchemy.exc.SQLAlchemyError: If database connection or query fails
    
    Examples:
        >>> conn_str = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        >>> existing = load_existing_keys(conn_str)
        >>> existing.shape
        (45123, 3)
    """
    engine = create_engine(connection_string)
    query = f"""
        SELECT 
            [Date],
            [Symbol],
            [EnglishCurrencyType]
        FROM {table_name}
        WHERE [EnglishAssetType] = 'Currency'
    """
    df_keys = pd.read_sql(query, engine)
    return df_keys


def find_new_records(df_new, df_existing_keys):
    """
    Identify new records by comparing against existing database keys.
    
    Performs a left join to find records in df_new that don't exist in the
    database based on the composite key (Date, Symbol, EnglishCurrencyType).
    
    Args:
        df_new (pd.DataFrame): New records fetched from API
        df_existing_keys (pd.DataFrame): Existing keys from database
    
    Returns:
        pd.DataFrame: Subset of df_new containing only new records not in database
    
    Examples:
        >>> df_new = process_all_currency_data()  # 100 records
        >>> df_existing = load_existing_keys(conn_str)  # 90 matching keys
        >>> df_delta = find_new_records(df_new, df_existing)
        >>> len(df_delta)
        10
    """
    key_cols = ['Date', 'Symbol', 'EnglishCurrencyType']
    
    df_new_keys = df_new[key_cols].copy()
    df_existing_keys = df_existing_keys[key_cols].copy()
    
    # Left join to identify new records
    df_merged = df_new_keys.merge(
        df_existing_keys,
        on=key_cols,
        how='left',
        indicator=True
    )
    
    # Filter for records not in existing database
    new_mask = df_merged['_merge'] == 'left_only'
    return df_new.loc[new_mask].copy()


# ======================================================
# Database Operations
# ======================================================

def save_to_database(df, connection_string, table_name='IceAssets'):
    """
    Save DataFrame to SQL Server database with proper schema.
    
    Inserts new records into the specified table using pandas to_sql with
    optimized settings for performance and data integrity.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        connection_string (str): SQLAlchemy connection string
        table_name (str, optional): Target table name. Defaults to 'IceAssets'
    
    Returns:
        None
    
    Raises:
        sqlalchemy.exc.SQLAlchemyError: If database operations fail
    
    Side Effects:
        - Creates database engine
        - Inserts records into specified table
        - Prints insertion summary
    
    Database Schema:
        - Date: CHAR(10) - Date in YYYY-MM-DD format
        - Name: NVARCHAR(100) - Currency name in Persian
        - SellPrice: BigInteger - Selling price
        - BuyPrice: BigInteger - Buying price
        - ScrapeDate: CHAR(10) - Scrape date
        - Scrapetime: CHAR(8) - Scrape time
        - Symbol: CHAR(3) - Currency symbol
        - PersianCurrencyType: NVARCHAR(50) - Type in Persian
        - EnglishCurrencyType: NVARCHAR(50) - Type in English
        - PersianAssetType: NVARCHAR(50) - 'ارز'
        - EnglishAssetType: NVARCHAR(50) - 'Currency'
        - ScrapeDateTime: DATETIME - Full timestamp
    
    Examples:
        >>> df = pd.DataFrame([...])  # New records
        >>> conn_str = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        >>> save_to_database(df, conn_str)
        Inserted 123 rows into IceAssets
    """
    if df.empty:
        print("No data to save")
        return
    
    # Create database engine with fast_executemany for performance
    engine = create_engine(connection_string, fast_executemany=True)
    
    df.to_sql(
        table_name,
        engine,
        if_exists='append',  # Append to existing table
        index=False,
        chunksize=1000,      # Insert in batches of 1000
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
    print(f"Inserted {len(df)} rows into {table_name}")


def incremental_load(connection_string, table_name='IceAssets'):
    """
    Perform intelligent incremental data load from ICE.ir API to database.
    
    Main entry point that orchestrates the complete ETL pipeline:
    1. Fetch fresh data from API
    2. Load existing keys from database
    3. Identify new records (delta)
    4. Insert only new records
    
    This approach prevents duplicate inserts and maintains database integrity
    while allowing the script to be run multiple times safely.
    
    Args:
        connection_string (str): SQLAlchemy connection string format:
            "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        table_name (str, optional): Target table name. Defaults to 'IceAssets'
    
    Returns:
        None
    
    Raises:
        requests.RequestException: If API calls fail
        sqlalchemy.exc.SQLAlchemyError: If database operations fail
    
    Side Effects:
        - Fetches data from ICE.ir API
        - Queries database for existing records
        - Inserts new records into database
        - Prints progress messages to stdout
    
    Examples:
        >>> CONNECTION_STRING = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        >>> incremental_load(CONNECTION_STRING)
        Fetching fresh data from API ...
        Fetching currency_id=14 (Bill)
          -> 5432 records
        ...
        Loading existing keys from database ...
        Detecting new records ...
        New records to insert: 87
        Inserted 87 rows into IceAssets
    """
    print("Fetching fresh data from API ...")
    df_new = process_all_currency_data()
    
    if df_new.empty:
        print("No data fetched from API")
        return
    
    print("Loading existing keys from database ...")
    df_existing_keys = load_existing_keys(connection_string, table_name)
    
    print("Detecting new records ...")
    df_delta = find_new_records(df_new, df_existing_keys)
    
    print(f"New records to insert: {len(df_delta)}")
    
    if df_delta.empty:
        print("Database already up to date")
        return
    
    save_to_database(df_delta, connection_string, table_name)


# ======================================================
# Script Entry Point
# ======================================================

if __name__ == "__main__":
    # Connection string configuration
    # Update with your actual database credentials
    CONNECTION_STRING = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
    
    # Run incremental load
    incremental_load(CONNECTION_STRING)
