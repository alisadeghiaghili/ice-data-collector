# -*- coding: utf-8 -*-
"""
This module scrapes pricing data from the Iran Commodity Exchange (ICE.ir) website,
processes Persian text and numbers, and stores the data in a SQL Server database.

Features:
- Headless Firefox web scraping with Selenium
- Persian number conversion to English
- Date format standardization
- Comprehensive logging
- Error handling and data validation
- Database storage with SQLAlchemy

Created on: Tue Aug 12 11:01:08 2025
Author: sadeghi.a
"""

import logging
import sys
from pathlib import Path
from typing import Optional, List, Dict
import time
from datetime import datetime
import re

import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, CHAR, NVARCHAR, BigInteger
from sqlalchemy.exc import SQLAlchemyError


class ICEScraper:
    """
    A web scraper for extracting commodity data from ICE.ir (Iran Commodity Exchange).
    
    This class handles the complete workflow of scraping, processing, and storing
    commodity pricing data with comprehensive logging and error handling.
    """
    
    def __init__(self, 
                 firefox_binary_path: str = r"path_to_firefox.exe",
                 geckodriver_path: str = r"path_to_geko",
                 db_connection: str = None,
                 log_level: int = logging.INFO):
        """
        Initialize the ICE scraper with configuration options.
        
        Args:
            firefox_binary_path (str): Path to Firefox executable
            geckodriver_path (str): Path to geckodriver executable
            db_connection (str): Database connection string
            log_level (int): Logging level (default: INFO)
        """
        self.url = "https://ice.ir/"
        self.firefox_binary_path = firefox_binary_path
        self.geckodriver_path = geckodriver_path
        self.db_connection = db_connection or (
            "mssql+pyodbc://user@server/db?"
            "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
        
        # Setup logging
        self._setup_logging(log_level)
        
        # CSS selectors for data extraction
        self.selectors = {
            'dates': '.pt-4 .text-light-blue',
            'prices': '.pt-4 h2.text-light',
            'names': '.pt-4 h4.text-light'
        }
        
        # Database column mappings
        self.dtype_mapping = {
            'Date': CHAR(10),
            'Name': NVARCHAR(100),
            'Price': BigInteger(),
            'ScrapeDate': CHAR(10),
            'ScrapeTime': CHAR(8)
        }
        
        self.logger.info("ICEScraper initialized successfully")
    
    def _setup_logging(self, log_level: int) -> None:
        """
        Configure logging with both file and console handlers.
        
        Args:
            log_level (int): Logging level
        """
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler for detailed logs
        file_handler = logging.FileHandler(
            log_dir / f"ice_scraper_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        
        # Console handler for important messages
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(simple_formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def _setup_webdriver(self) -> webdriver.Firefox:
        """
        Configure and create Firefox WebDriver with optimized settings.
        
        Returns:
            webdriver.Firefox: Configured Firefox WebDriver instance
            
        Raises:
            WebDriverException: If WebDriver setup fails
        """
        try:
            self.logger.info("Setting up Firefox WebDriver...")
            
            # Verify required files exist
            if not Path(self.firefox_binary_path).exists():
                raise FileNotFoundError(f"Firefox binary not found: {self.firefox_binary_path}")
            
            if not Path(self.geckodriver_path).exists():
                raise FileNotFoundError(f"Geckodriver not found: {self.geckodriver_path}")
            
            # Configure Firefox options
            options = Options()
            options.binary_location = self.firefox_binary_path
            
            # Performance and stability options
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
            service = Service(self.geckodriver_path)
            driver = webdriver.Firefox(service=service, options=options)
            
            # Set timeouts
            driver.implicitly_wait(10)
            driver.set_page_load_timeout(30)
            
            self.logger.info("WebDriver setup completed successfully")
            return driver
            
        except Exception as e:
            self.logger.error(f"Failed to setup WebDriver: {str(e)}")
            raise WebDriverException(f"WebDriver setup failed: {str(e)}")
    
    def _scrape_page_content(self, driver: webdriver.Firefox) -> str:
        """
        Load the target page and extract HTML content.
        
        Args:
            driver (webdriver.Firefox): WebDriver instance
            
        Returns:
            str: Page HTML content
            
        Raises:
            TimeoutException: If page loading times out
        """
        try:
            self.logger.info(f"Loading page: {self.url}")
            driver.get(self.url)
            
            # Wait for content to load with explicit wait
            wait = WebDriverWait(driver, 15)
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.selectors['dates']))
            )
            
            # Additional wait for dynamic content
            time.sleep(3)
            
            html_content = driver.page_source
            self.logger.info(f"Page loaded successfully. Content length: {len(html_content)} characters")
            
            return html_content
            
        except TimeoutException as e:
            self.logger.error(f"Timeout while loading page: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading page: {str(e)}")
            raise
    
    def _extract_data_from_html(self, html_content: str) -> Dict[str, List[str]]:
        """
        Parse HTML content and extract commodity data.
        
        Args:
            html_content (str): HTML content to parse
            
        Returns:
            Dict[str, List[str]]: Dictionary containing extracted data lists
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
            
            return {
                'dates': dates,
                'prices': prices,
                'names': names
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML: {str(e)}")
            raise
    
    def clean_persian_number(self, text: str) -> Optional[float]:
        """
        Clean and convert Persian numbers to float.
        
        This method handles Persian digits, removes formatting characters,
        and converts the cleaned text to a numeric value.
        
        Args:
            text (str): Text containing Persian numbers and formatting
            
        Returns:
            Optional[float]: Converted number or None if conversion fails
        """
        if not text or pd.isna(text):
            self.logger.debug(f"Empty or NaN input: {text}")
            return None
        
        try:
            # Persian to English digit mapping
            persian_digits = '۰۱۲۳۴۵۶۷۸۹'
            english_digits = '0123456789'
            
            # Convert to string and replace Persian digits
            text = str(text)
            for persian, english in zip(persian_digits, english_digits):
                text = text.replace(persian, english)
            
            # Remove commas and other formatting characters except dots
            cleaned = re.sub(r'[^\d.]', '', text)
            
            if cleaned:
                result = float(cleaned)
                self.logger.debug(f"Converted '{text}' to {result}")
                return result
            else:
                self.logger.debug(f"No numeric content found in: {text}")
                return None
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to convert '{text}' to number: {str(e)}")
            return None
    
    def correct_date_format(self, text: str) -> str:
        """
        Convert date from YYYYMMDD format to YYYY/MM/DD format.
        
        Args:
            text (str): Date string in YYYYMMDD format
            
        Returns:
            str: Formatted date string in YYYY/MM/DD format
        """
        try:
            text = str(int(float(text)))  # Handle potential float conversion
            if len(text) == 8:
                formatted_date = f"{text[:4]}/{text[4:6]}/{text[6:8]}"
                self.logger.debug(f"Formatted date '{text}' to '{formatted_date}'")
                return formatted_date
            else:
                self.logger.warning(f"Invalid date format: {text}")
                return text
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error formatting date '{text}': {str(e)}")
            return str(text)
    
    def _process_scraped_data(self, raw_data: Dict[str, List[str]]) -> pd.DataFrame:
        """
        Process raw scraped data into a clean DataFrame.
        
        Args:
            raw_data (Dict[str, List[str]]): Raw extracted data
            
        Returns:
            pd.DataFrame: Processed and cleaned DataFrame
        """
        try:
            self.logger.info("Processing scraped data...")
            
            # Create DataFrame from raw data
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
            
            # Clean and convert numeric columns
            self.logger.info("Cleaning Date column...")
            df['Date'] = df['Date'].apply(self.clean_persian_number)
            df['Date'] = df['Date'].apply(self.correct_date_format)
            
            self.logger.info("Cleaning Price column...")
            df['Price'] = df['Price'].apply(self.clean_persian_number)
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0).astype(float)
            
            # Handle duplicates
            initial_rows = len(df)
            self.logger.info("Checking for duplicate records...")
            
            # Drop duplicates based on Date and Name (keeping first occurrence)
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
            
            self.logger.info(f"Data processing completed successfully. Final dataset: {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing scraped data: {str(e)}")
            raise
    
    def _save_to_database(self, df: pd.DataFrame, table_name: str = 'scraped') -> bool:
        """
        Save processed data to SQL Server database.
        
        Args:
            df (pd.DataFrame): Processed data to save
            table_name (str): Target table name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Saving {len(df)} rows to database table '{table_name}'...")
            
            # Create database engine
            engine = create_engine(self.db_connection)
            
            # Save to database
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                dtype=self.dtype_mapping
            )
            
            self.logger.info("Data saved to database successfully")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error saving to database: {str(e)}")
            return False
    
    def scrape_and_store(self) -> bool:
        """
        Execute the complete scraping workflow.
        
        This method orchestrates the entire process: web scraping, data processing,
        and database storage with comprehensive error handling and logging.
        
        Returns:
            bool: True if the entire workflow completed successfully
        """
        start_time = datetime.now()
        self.logger.info("Starting ICE.ir scraping workflow...")
        
        driver = None
        try:
            # Setup WebDriver
            driver = self._setup_webdriver()
            
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
            self.logger.info(f"Scraping workflow completed in {duration.total_seconds():.2f} seconds")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Scraping workflow failed: {str(e)}")
            return False
        
        finally:
            # Cleanup WebDriver
            if driver:
                try:
                    driver.quit()
                    self.logger.info("WebDriver closed successfully")
                except Exception as e:
                    self.logger.warning(f"Error closing WebDriver: {str(e)}")


def main():
    """
    Main function to execute the scraping workflow.
    
    This function provides a simple interface to run the scraper with
    default settings and comprehensive error handling.
    """
    try:
        # Initialize and run scraper
        scraper = ICEScraper(log_level=logging.INFO)
        
        success = scraper.scrape_and_store()
        
        if success:
            print("✅ Scraping completed successfully!")
            sys.exit(0)
        else:
            print("❌ Scraping failed. Check logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
