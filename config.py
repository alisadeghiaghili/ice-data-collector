# -*- coding: utf-8 -*-
"""
Secure Configuration Module for ICE Data Collector.

Provides secure credential management using environment variables.
Compatible with SQLAlchemy 2.0+ and supports both SQL authentication
and Windows trusted connection.

NO credentials should ever be hardcoded in this file.

Usage:
    from config import Config
    
    config = Config()
    engine = config.create_engine()
"""

import os
import logging
from typing import Optional, Dict, Any
from urllib.parse import quote_plus
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


def load_env_file(env_path: str = '.env') -> None:
    """
    Load environment variables from .env file.
    
    Simple implementation that doesn't require python-dotenv.
    For production, consider using python-dotenv library.
    
    Args:
        env_path: Path to .env file (default: '.env')
    """
    if not os.path.exists(env_path):
        logger.debug(f"No {env_path} file found - using system environment variables")
        return
    
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Parse KEY=VALUE
                if '=' not in line:
                    continue
                    
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                # Remove quotes if present
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                
                # Only set if not already in environment (env vars have priority)
                if key not in os.environ:
                    os.environ[key] = value
        
        logger.info(f"Loaded environment variables from {env_path}")
    except Exception as e:
        logger.warning(f"Failed to load {env_path}: {e}")


def validate_no_placeholders(value: str, var_name: str) -> None:
    """
    Validate that environment variable doesn't contain placeholder values.
    
    Args:
        value: Variable value to check
        var_name: Variable name for error message
    
    Raises:
        ValueError: If placeholder detected
    """
    placeholders = ['your_', 'example', 'placeholder', 'changeme', 'change_me', 'path_to']
    
    if any(ph in value.lower() for ph in placeholders):
        raise ValueError(
            f"⚠️  Environment variable {var_name} contains placeholder value: '{value}'\n"
            f"Please update your .env file with actual values.\n"
            f"See .env.example for template."
        )


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    server: str
    database: str
    driver: str = 'ODBC Driver 17 for SQL Server'
    port: int = 1433
    user: Optional[str] = None
    password: Optional[str] = None
    trusted_connection: bool = False
    table_name: str = 'scraped'
    connection_timeout: int = 30
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.trusted_connection and (not self.user or not self.password):
            raise ValueError(
                "Either set trusted_connection=True or provide user and password"
            )
        
        # Validate no placeholders
        validate_no_placeholders(self.server, 'server')
        validate_no_placeholders(self.database, 'database')
    
    def get_connection_string(self) -> str:
        """
        Build SQLAlchemy connection string.
        
        Returns:
            Connection string compatible with SQLAlchemy 2.0
        """
        driver_encoded = quote_plus(self.driver)
        
        if self.trusted_connection:
            # Windows Authentication
            conn_str = (
                f"mssql+pyodbc://@{self.server}:{self.port}/{self.database}"
                f"?driver={driver_encoded}"
                f"&trusted_connection=yes"
            )
            logger.debug("Using Windows trusted connection")
        else:
            # SQL Server Authentication
            user_encoded = quote_plus(self.user)
            password_encoded = quote_plus(self.password)
            
            conn_str = (
                f"mssql+pyodbc://{user_encoded}:{password_encoded}"
                f"@{self.server}:{self.port}/{self.database}"
                f"?driver={driver_encoded}"
            )
            logger.debug(f"Using SQL authentication for user: {self.user}")
        
        return conn_str
    
    @classmethod
    def from_env(cls, prefix: str = 'DB') -> 'DatabaseConfig':
        """
        Create DatabaseConfig from environment variables.
        
        Args:
            prefix: Environment variable prefix (e.g., 'DB' or 'ICE_DB')
        
        Returns:
            DatabaseConfig instance
        
        Environment Variables:
            {PREFIX}_SERVER: Database server hostname  
            {PREFIX}_NAME: Database name
            {PREFIX}_USER: Database username (not required if trusted_connection=yes)
            {PREFIX}_PASSWORD: Database password (not required if trusted_connection=yes)
            {PREFIX}_DRIVER: ODBC driver (default: 'ODBC Driver 17 for SQL Server')
            {PREFIX}_PORT: Database port (default: 1433)
            {PREFIX}_TRUSTED_CONNECTION: Use Windows auth (yes/true/1)
            {PREFIX}_TABLE_NAME: Target table name (default: 'scraped')
            {PREFIX}_CONNECTION_TIMEOUT: Connection timeout seconds (default: 30)
        """
        server = os.getenv(f'{prefix}_SERVER')
        database = os.getenv(f'{prefix}_NAME')
        
        if not server or not database:
            raise ValueError(
                f"❌ Missing required environment variables: "
                f"{prefix}_SERVER and {prefix}_NAME\n"
                f"Please set them in .env file or environment.\n"
                f"See .env.example for template."
            )
        
        # Check if trusted connection is enabled
        trusted_conn_str = os.getenv(f'{prefix}_TRUSTED_CONNECTION', 'no').lower()
        trusted_connection = trusted_conn_str in ('yes', 'true', '1')
        
        # Get credentials (only required if not using trusted connection)
        user = os.getenv(f'{prefix}_USER')
        password = os.getenv(f'{prefix}_PASSWORD')
        
        return cls(
            server=server,
            database=database,
            driver=os.getenv(f'{prefix}_DRIVER', 'ODBC Driver 17 for SQL Server'),
            port=int(os.getenv(f'{prefix}_PORT', '1433')),
            user=user,
            password=password,
            trusted_connection=trusted_connection,
            table_name=os.getenv(f'{prefix}_TABLE_NAME', 'scraped'),
            connection_timeout=int(os.getenv(f'{prefix}_CONNECTION_TIMEOUT', '30'))
        )


@dataclass
class ScraperConfig:
    """Web scraper configuration."""
    firefox_binary_path: str
    geckodriver_path: str
    url: str = 'https://ice.ir/'
    page_load_timeout: int = 30
    implicit_wait: int = 10
    explicit_wait: int = 15
    headless: bool = True
    
    def __post_init__(self):
        """Validate paths after initialization."""
        if not Path(self.firefox_binary_path).exists():
            raise FileNotFoundError(
                f"Firefox binary not found: {self.firefox_binary_path}"
            )
        
        if not Path(self.geckodriver_path).exists():
            raise FileNotFoundError(
                f"Geckodriver not found: {self.geckodriver_path}"
            )
    
    @classmethod
    def from_env(cls) -> 'ScraperConfig':
        """
        Create ScraperConfig from environment variables.
        
        Returns:
            ScraperConfig instance
        
        Environment Variables:
            FIREFOX_BINARY_PATH: Path to Firefox executable
            GECKODRIVER_PATH: Path to geckodriver executable
            ICE_URL: Target URL (default: https://ice.ir/)
            PAGE_LOAD_TIMEOUT: Page load timeout seconds (default: 30)
            IMPLICIT_WAIT: Implicit wait seconds (default: 10)
            EXPLICIT_WAIT: Explicit wait seconds (default: 15)
            HEADLESS: Run in headless mode (default: true)
        """
        firefox_path = os.getenv('FIREFOX_BINARY_PATH')
        geckodriver_path = os.getenv('GECKODRIVER_PATH')
        
        if not firefox_path or not geckodriver_path:
            raise ValueError(
                "❌ Missing required environment variables: "
                "FIREFOX_BINARY_PATH and GECKODRIVER_PATH\n"
                "Please set them in .env file."
            )
        
        validate_no_placeholders(firefox_path, 'FIREFOX_BINARY_PATH')
        validate_no_placeholders(geckodriver_path, 'GECKODRIVER_PATH')
        
        headless_str = os.getenv('HEADLESS', 'true').lower()
        
        return cls(
            firefox_binary_path=firefox_path,
            geckodriver_path=geckodriver_path,
            url=os.getenv('ICE_URL', 'https://ice.ir/'),
            page_load_timeout=int(os.getenv('PAGE_LOAD_TIMEOUT', '30')),
            implicit_wait=int(os.getenv('IMPLICIT_WAIT', '10')),
            explicit_wait=int(os.getenv('EXPLICIT_WAIT', '15')),
            headless=headless_str in ('yes', 'true', '1')
        )


@dataclass
class LogConfig:
    """Logging configuration."""
    level: int = logging.INFO
    directory: str = 'logs'
    format_detailed: str = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    format_simple: str = '%(asctime)s - %(levelname)s - %(message)s'
    
    @classmethod
    def from_env(cls) -> 'LogConfig':
        """
        Create LogConfig from environment variables.
        
        Returns:
            LogConfig instance
        
        Environment Variables:
            LOG_LEVEL: Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
            LOG_DIR: Log directory path (default: 'logs')
        """
        level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
        levels = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        
        return cls(
            level=levels.get(level_str, logging.INFO),
            directory=os.getenv('LOG_DIR', 'logs')
        )


@dataclass
class RetryConfig:
    """Retry logic configuration."""
    max_attempts: int = 3
    wait_exponential_multiplier: int = 1
    wait_exponential_max: int = 10
    retry_on_exceptions: tuple = (Exception,)
    
    @classmethod
    def from_env(cls) -> 'RetryConfig':
        """
        Create RetryConfig from environment variables.
        
        Returns:
            RetryConfig instance
        
        Environment Variables:
            RETRY_MAX_ATTEMPTS: Maximum retry attempts (default: 3)
            RETRY_WAIT_MULTIPLIER: Exponential backoff multiplier (default: 1)
            RETRY_WAIT_MAX: Maximum wait time seconds (default: 10)
        """
        return cls(
            max_attempts=int(os.getenv('RETRY_MAX_ATTEMPTS', '3')),
            wait_exponential_multiplier=int(os.getenv('RETRY_WAIT_MULTIPLIER', '1')),
            wait_exponential_max=int(os.getenv('RETRY_WAIT_MAX', '10'))
        )


@dataclass
class Config:
    """Main application configuration."""
    database: DatabaseConfig
    scraper: ScraperConfig
    logging: LogConfig
    retry: RetryConfig
    
    @classmethod
    def from_env(cls, db_prefix: str = 'DB') -> 'Config':
        """
        Create Config from environment variables.
        
        Args:
            db_prefix: Database environment variable prefix
        
        Returns:
            Config instance with all sub-configurations
        """
        return cls(
            database=DatabaseConfig.from_env(db_prefix),
            scraper=ScraperConfig.from_env(),
            logging=LogConfig.from_env(),
            retry=RetryConfig.from_env()
        )
    
    def create_engine(self, **kwargs):
        """
        Create SQLAlchemy 2.0 engine with proper configuration.
        
        Args:
            **kwargs: Additional arguments to pass to create_engine
        
        Returns:
            SQLAlchemy Engine instance
        """
        from sqlalchemy import create_engine
        
        engine_kwargs = {
            'echo': False,
            'pool_pre_ping': True,
            'pool_recycle': 3600,
            'connect_args': {
                'timeout': self.database.connection_timeout
            }
        }
        engine_kwargs.update(kwargs)
        
        return create_engine(
            self.database.get_connection_string(),
            **engine_kwargs
        )
    
    def print_status(self) -> None:
        """Print configuration status for debugging (passwords masked)."""
        print("\n" + "="*60)
        print("Configuration Status")
        print("="*60)
        
        # Database config
        print("\nDatabase Configuration:")
        print(f"  Server: {self.database.server}:{self.database.port}")
        print(f"  Database: {self.database.database}")
        print(f"  Driver: {self.database.driver}")
        print(f"  Table: {self.database.table_name}")
        print(f"  Trusted Connection: {'✓ Yes' if self.database.trusted_connection else '✗ No'}")
        
        if not self.database.trusted_connection:
            print(f"  User: {self.database.user}")
            print(f"  Password: {'*' * 8}")
        
        # Scraper config
        print("\nScraper Configuration:")
        print(f"  URL: {self.scraper.url}")
        print(f"  Firefox: {self.scraper.firefox_binary_path}")
        print(f"  Geckodriver: {self.scraper.geckodriver_path}")
        print(f"  Headless: {self.scraper.headless}")
        print(f"  Page Load Timeout: {self.scraper.page_load_timeout}s")
        
        # Logging config
        print("\nLogging Configuration:")
        print(f"  Level: {logging.getLevelName(self.logging.level)}")
        print(f"  Directory: {self.logging.directory}")
        
        # Retry config
        print("\nRetry Configuration:")
        print(f"  Max Attempts: {self.retry.max_attempts}")
        print(f"  Wait Multiplier: {self.retry.wait_exponential_multiplier}")
        print(f"  Max Wait: {self.retry.wait_exponential_max}s")
        
        print("\n" + "="*60 + "\n")


# Auto-load .env file when module is imported
load_env_file()


if __name__ == '__main__':
    # Test configuration when run directly
    print("Testing configuration...\n")
    
    try:
        config = Config.from_env()
        config.print_status()
        
        # Test engine creation
        engine = config.create_engine()
        print("✓ SQLAlchemy engine created successfully\n")
        engine.dispose()
        
    except (ValueError, FileNotFoundError) as e:
        print(f"\n❌ Configuration Error:\n{e}\n")
        exit(1)
