# -*- coding: utf-8 -*-
"""
ICE.ir Data Collector - Enterprise-grade web scraper for Iran Commodity Exchange.

This module scrapes pricing data from ICE.ir, processes Persian text and numbers,
and stores the data in a SQL Server database using SQLAlchemy 2.0+.

Features:
- SQLAlchemy 2.0+ compatibility with modern ORM patterns
- Headless Firefox web scraping with Selenium
- Persian number and date conversion
- Context managers for resource management
- Retry logic with exponential backoff
- Comprehensive logging and error handling
- Support for Windows trusted connection and SQL authentication

Created on: Tue Aug 12 11:01:08 2025
Author: Ali Sadeghi Aghili
Last Updated: Feb 08 2026
"""

import logging
import sys
import re
import time
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from datetime import datetime
from contextlib import contextmanager

import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

# SQLAlchemy 2.0 imports
from sqlalchemy import create_engine, Table, Column, MetaData, inspect
from sqlalchemy import CHAR, NVARCHAR, BigInteger, Integer
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

# Retry logic
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
        before_sleep_log
    )
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    logging.warning("tenacity not installed - retry logic disabled")

from config import Config


class ICEScraper:
    """
    Web scraper for extracting commodity data from ICE.ir.
    
    This class handles the complete workflow of scraping, processing, and storing
    commodity pricing data with modern SQLAlchemy 2.0 patterns, context managers,
    and comprehensive error handling.
    
    Attributes:
        config: Application configuration
        logger: Logger instance
        metadata: SQLAlchemy metadata for table definitions
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize the ICE scraper with configuration.
        
        Args:
            config: Configuration object (if None, loads from environment)
        """
        # Load configuration
        self.config = config or Config.from_env()
        
        # Setup logging
        self._setup_logging()
        
        # CSS selectors for data extraction
        self.selectors = {
            'dates': '.pt-4 .text-light-blue',
            'prices': '.pt-4 h2.text-light',
            'names': '.pt-4 h4.text-light'
        }
        
        # SQLAlchemy 2.0 metadata
        self.metadata = MetaData()
        self._define_table_schema()
        
        self.logger.info("ICEScraper initialized successfully")
    
    def _setup_logging(self) -> None:
        """
        Configure logging with both file and console handlers.
        """
        # Create logs directory
        log_dir = Path(self.config.logging.directory)
        log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.config.logging.level)
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(self.config.logging.format_detailed)
        simple_formatter = logging.Formatter(self.config.logging.format_simple)
        
        # File handler
        file_handler = logging.FileHandler(
            log_dir / f"ice_scraper_{datetime.now().strftime('%Y%m%d')}.log",
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.config.logging.level)
        console_handler.setFormatter(simple_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def _define_table_schema(self) -> None:
        """
        Define database table schema using SQLAlchemy 2.0 Table construct.
        """
        self.table = Table(
            self.config.database.table_name,
            self.metadata,
            Column('Date', CHAR(10), nullable=False),
            Column('Name', NVARCHAR(100), nullable=False),
            Column('Price', BigInteger, nullable=False),
            Column('ScrapeDate', CHAR(10), nullable=False),
            Column('ScrapeTime', CHAR(8), nullable=False),
            extend_existing=True
        )
        self.logger.debug(f"Table schema defined: {self.config.database.table_name}")
    
    @contextmanager
    def _get_webdriver(self):
        """
        Context manager for Firefox WebDriver with automatic cleanup.
        
        Yields:
            webdriver.Firefox: Configured Firefox WebDriver instance
            
        Raises:
            WebDriverException: If WebDriver setup fails
        """
        driver = None
        try:
            self.logger.info("Setting up Firefox WebDriver...")
            
            # Configure Firefox options
            options = Options()
            options.binary_location = self.config.scraper.firefox_binary_path
            
            # Performance and stability options
            if self.config.scraper.headless:
                options.add_argument("--headless")
            
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            
            # Set realistic user agent
            options.set_preference(
                "general.useragent.override",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            
            # Optimize loading performance
            options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", False)
            options.set_preference("media.volume_scale", "0.0")
            
            # Create service and driver
            service = Service(self.config.scraper.geckodriver_path)
            driver = webdriver.Firefox(service=service, options=options)
            
            # Set timeouts
            driver.implicitly_wait(self.config.scraper.implicit_wait)
            driver.set_page_load_timeout(self.config.scraper.page_load_timeout)
            
            self.logger.info("WebDriver setup completed successfully")
            yield driver
            
        except Exception as e:
            self.logger.error(f"Failed to setup WebDriver: {str(e)}")
            raise WebDriverException(f"WebDriver setup failed: {str(e)}")
        
        finally:
            # Cleanup
            if driver:
                try:
                    driver.quit()
                    self.logger.info("WebDriver closed successfully")
                except Exception as e:
                    self.logger.warning(f"Error closing WebDriver: {str(e)}")
    
    def _create_retry_decorator(self):
        """Create retry decorator with configuration."""
        if not TENACITY_AVAILABLE:
            # Return a no-op decorator
            def no_retry(func):
                return func
            return no_retry
        
        return retry(
            stop=stop_after_attempt(self.config.retry.max_attempts),
            wait=wait_exponential(
                multiplier=self.config.retry.wait_exponential_multiplier,
                max=self.config.retry.wait_exponential_max
            ),
            retry=retry_if_exception_type((TimeoutException, WebDriverException)),
            before_sleep=before_sleep_log(self.logger, logging.WARNING),
            reraise=True
        )
    
    def _scrape_page_content(self, driver: webdriver.Firefox) -> str:
        """
        Load the target page and extract HTML content with retry logic.
        
        Args:
            driver: WebDriver instance
            
        Returns:
            Page HTML content
            
        Raises:
            TimeoutException: If page loading times out after retries
        """
        # Apply retry decorator if available
        retry_decorator = self._create_retry_decorator()
        
        @retry_decorator
        def _load_with_retry():
            self.logger.info(f"Loading page: {self.config.scraper.url}")
            driver.get(self.config.scraper.url)
            
            # Wait for content with explicit wait
            wait = WebDriverWait(driver, self.config.scraper.explicit_wait)
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.selectors['dates']))
            )
            
            # Additional wait for dynamic content
            time.sleep(3)
            
            return driver.page_source
        
        try:
            html_content = _load_with_retry()
            self.logger.info(f"Page loaded successfully. Content length: {len(html_content)} characters")
            return html_content
        
        except Exception as e:
            self.logger.error(f"Failed to load page after retries: {str(e)}")
            raise
    
    def _extract_data_from_html(self, html_content: str) -> Dict[str, List[str]]:
        """
        Parse HTML content and extract commodity data.
        
        Args:
            html_content: HTML content to parse
            
        Returns:
            Dictionary containing extracted data lists
        """
        try:
            self.logger.info("Parsing HTML content...")
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Extract data using CSS selectors
            dates = [
                date.get_text(strip=True).strip() 
                for date in soup.select(self.selectors['dates'])
            ]
            prices = [
                price.get_text(strip=True).strip() 
                for price in soup.select(self.selectors['prices'])
            ]
            names = [
                name.get_text(strip=True).strip() 
                for name in soup.select(self.selectors['names'])
            ]
            
            # Log extraction results
            self.logger.info(f"Extracted {len(dates)} dates, {len(prices)} prices, {len(names)} names")
            
            # Validate data consistency
            if not (len(dates) == len(prices) == len(names)):
                self.logger.warning(
                    f"Data length mismatch - Dates: {len(dates)}, "
                    f"Prices: {len(prices)}, Names: {len(names)}"
                )
            
            if len(dates) == 0:
                raise ValueError("No data extracted from page - selectors may be outdated")
            
            return {
                'dates': dates,
                'prices': prices,
                'names': names
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML: {str(e)}")
            raise
    
    @staticmethod
    def clean_persian_number(text: str) -> Optional[float]:
        """
        Clean and convert Persian numbers to float.
        
        Args:
            text: Text containing Persian numbers and formatting
            
        Returns:
            Converted number or None if conversion fails
        """
        if not text or pd.isna(text):
            return None
        
        try:
            # Persian to English digit mapping
            persian_digits = '۰۱۲۳۴۵۶۷۸۹'
            english_digits = '0123456789'
            trans_table = str.maketrans(persian_digits, english_digits)
            
            # Convert to string and replace Persian digits
            text = str(text).translate(trans_table)
            
            # Remove commas and other formatting characters except dots
            cleaned = re.sub(r'[^\d.]', '', text)
            
            return float(cleaned) if cleaned else None
                
        except (ValueError, TypeError) as e:
            logging.debug(f"Failed to convert '{text}' to number: {str(e)}")
            return None
    
    @staticmethod
    def correct_date_format(text: str) -> str:
        """
        Convert date from YYYYMMDD format to YYYY/MM/DD format.
        
        Args:
            text: Date string in YYYYMMDD format
            
        Returns:
            Formatted date string in YYYY/MM/DD format
        """
        try:
            text = str(int(float(text)))
            if len(text) == 8:
                return f"{text[:4]}/{text[4:6]}/{text[6:8]}"
            else:
                logging.warning(f"Invalid date format: {text}")
                return text
        except (ValueError, TypeError) as e:
            logging.warning(f"Error formatting date '{text}': {str(e)}")
            return str(text)
    
    def _process_scraped_data(self, raw_data: Dict[str, List[str]]) -> pd.DataFrame:
        """
        Process raw scraped data into a clean DataFrame.
        
        Args:
            raw_data: Raw extracted data
            
        Returns:
            Processed and cleaned DataFrame
        """
        try:
            self.logger.info("Processing scraped data...")
            
            # Create DataFrame
            df = pd.DataFrame({
                'Date': raw_data['dates'],
                'Name': raw_data['names'],
                'Price': raw_data['prices']
            })
            
            self.logger.info(f"Created DataFrame with {len(df)} rows")
            
            # Add timestamp columns
            current_time = datetime.now()
            df['ScrapeDate'] = current_time.strftime('%Y-%m-%d')
            df['ScrapeTime'] = current_time.strftime('%H:%M:%S')
            
            # Clean and convert columns
            self.logger.info("Cleaning Date column...")
            df['Date'] = df['Date'].apply(self.clean_persian_number)
            df['Date'] = df['Date'].apply(self.correct_date_format)
            
            self.logger.info("Cleaning Price column...")
            df['Price'] = df['Price'].apply(self.clean_persian_number)
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0).astype('int64')
            
            # Handle duplicates
            initial_rows = len(df)
            self.logger.info("Checking for duplicate records...")
            df = df.drop_duplicates(subset=['Date', 'Name'], keep='first')
            
            duplicates_removed = initial_rows - len(df)
            if duplicates_removed > 0:
                self.logger.warning(f"Removed {duplicates_removed} duplicate records")
            else:
                self.logger.info("No duplicate records found")
            
            # Log data quality metrics
            null_dates = df['Date'].isna().sum()
            zero_prices = (df['Price'] == 0).sum()
            
            if null_dates > 0:
                self.logger.warning(f"Found {null_dates} null dates")
            if zero_prices > 0:
                self.logger.warning(f"Found {zero_prices} zero prices")
            
            self.logger.info(f"Data processing completed. Final dataset: {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing scraped data: {str(e)}")
            raise
    
    def _save_to_database(self, df: pd.DataFrame) -> bool:
        """
        Save processed data to SQL Server using SQLAlchemy 2.0 patterns.
        
        Args:
            df: Processed data to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(
                f"Saving {len(df)} rows to database table '{self.config.database.table_name}'..."
            )
            
            # Create engine with configuration
            engine = self.config.create_engine()
            
            # Create table if it doesn't exist
            inspector = inspect(engine)
            if not inspector.has_table(self.config.database.table_name):
                self.logger.info(f"Creating table: {self.config.database.table_name}")
                self.metadata.create_all(engine)
            
            # Save to database using pandas (compatible with SQLAlchemy 2.0)
            df.to_sql(
                name=self.config.database.table_name,
                con=engine,
                if_exists='append',
                index=False,
                method='multi',  # Batch insert for better performance
                chunksize=1000
            )
            
            # Cleanup
            engine.dispose()
            
            self.logger.info("Data saved to database successfully")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error saving to database: {str(e)}")
            return False
    
    def scrape_and_store(self) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        Execute the complete scraping workflow.
        
        This method orchestrates the entire process: web scraping, data processing,
        and database storage with comprehensive error handling and logging.
        
        Returns:
            Tuple of (success status, processed DataFrame or None)
        """
        start_time = datetime.now()
        self.logger.info("Starting ICE.ir scraping workflow...")
        
        try:
            # Use context manager for WebDriver
            with self._get_webdriver() as driver:
                # Scrape page content
                html_content = self._scrape_page_content(driver)
                
                # Extract raw data
                raw_data = self._extract_data_from_html(html_content)
            
            # Process data
            processed_df = self._process_scraped_data(raw_data)
            
            # Save to database
            success = self._save_to_database(processed_df)
            
            # Log completion
            duration = datetime.now() - start_time
            self.logger.info(
                f"Scraping workflow completed in {duration.total_seconds():.2f} seconds"
            )
            
            return success, processed_df if success else None
            
        except Exception as e:
            self.logger.error(f"Scraping workflow failed: {str(e)}", exc_info=True)
            return False, None


def main():
    """
    Main function to execute the scraping workflow.
    
    This function provides a simple interface to run the scraper with
    configuration from environment variables.
    """
    try:
        # Load and display configuration
        config = Config.from_env()
        
        # Optional: Print configuration status
        if '--show-config' in sys.argv:
            config.print_status()
            return
        
        # Initialize and run scraper
        scraper = ICEScraper(config)
        success, df = scraper.scrape_and_store()
        
        if success:
            print(f"✅ Scraping completed successfully! Saved {len(df)} records.")
            sys.exit(0)
        else:
            print("❌ Scraping failed. Check logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user")
        sys.exit(1)
    except (ValueError, FileNotFoundError) as e:
        print(f"❌ Configuration error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        logging.exception("Unexpected error in main()")
        sys.exit(1)


if __name__ == "__main__":
    main()
