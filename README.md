# ICE.ir Web Scraper 🚀

A robust, enterprise-grade web scraper for extracting commodity pricing data from the Iran Commodity Exchange (ICE.ir) website. Features comprehensive logging, error handling, Persian text processing, and automated database storage.

## ✨ Features

- ** Reliable Web Scraping**: Headless Firefox with optimized settings and explicit waits
- ** Persian Text Processing**: Automatic conversion of Persian numbers and date formatting
- ** Data Validation**: Duplicate removal, quality checks, and data consistency validation
- **️ Database Integration**: Seamless SQL Server storage with SQLAlchemy ORM
- ** Enterprise Logging**: Dual logging system with file rotation and detailed error tracking
- ** Performance Optimized**: Efficient WebDriver configuration and resource management
- **️ Error Resilience**: Comprehensive exception handling and graceful failure recovery

## 🚀 Quick Start

### Prerequisites

- **Python 3.7+**
- **Firefox Browser** installed
- **geckodriver** downloaded and accessible
- **SQL Server** with ODBC Driver 17
- Required Python packages (see [Installation](#installation))

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ice-scraper
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Download geckodriver**
   - Download from [geckodriver releases](https://github.com/mozilla/geckodriver/releases)
   - Place in `D:\geckodriver.exe` or update path in code

### Basic Usage

**Option 1: Command Line (Recommended)**
```bash
python ice_scraper.py
```

**Option 2: Programmatic Usage**
```python
from ice_scraper import ICEScraper

# Basic usage with defaults
scraper = ICEScraper()
success = scraper.scrape_and_store()

if success:
    print("✅ Scraping completed successfully!")
else:
    print("❌ Scraping failed. Check logs for details.")
```

## ⚙️ Configuration

### Custom Configuration
```python
scraper = ICEScraper(
    firefox_binary_path=r"C:\Program Files\Mozilla Firefox\firefox.exe",
    geckodriver_path=r"D:\geckodriver.exe",
    db_connection="",
    log_level=logging.DEBUG  # DEBUG, INFO, WARNING, ERROR
)
```

### Database Connection
The default connection string targets SQL Server:
```python
"mssql+pyodbc://domainUser@server/db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
```

Update the connection string in the class initialization for different databases or credentials.

## 📊 Data Pipeline

### 1. Web Scraping
- Loads ICE.ir in headless Firefox
- Waits for dynamic content using explicit waits
- Extracts dates, names, and prices using CSS selectors

### 2. Data Processing
- Converts Persian digits (۰۱۲۳۴۵۶۷۸۹) to English (0123456789)
- Formats dates from YYYYMMDD to YYYY/MM/DD
- Removes formatting characters and converts to numeric types
- **Removes duplicates** based on Date+Name composite key

### 3. Quality Validation
- Checks for null dates and zero prices
- Reports data consistency issues
- Logs quality metrics and warnings

### 4. Database Storage
- Stores in SQL Server table with proper data types
- Uses `append` mode to preserve historical data
- Includes scrape timestamp for audit trail

## 📋 Database Schema

| Column     | Type         | Description                    |
|------------|--------------|--------------------------------|
| Date       | CHAR(10)     | Formatted date (YYYY/MM/DD)    |
| Name       | NVARCHAR(100)| Commodity name                 |
| Price      | BigInteger   | Price value in Rls             |
| ScrapeDate | CHAR(10)     | Date when scraped (YYYY-MM-DD)|
| ScrapeTime | CHAR(8)      | Time when scraped (HH:MM:SS)  |

## 📝 Logging

### Log Locations
- **File Logs**: `./logs/ice_scraper_YYYYMMDD.log`
- **Console Logs**: Real-time output to stdout

### Log Levels
- **DEBUG**: Detailed execution traces and data conversion details
- **INFO**: General workflow progress and success messages
- **WARNING**: Data quality issues and non-critical problems
- **ERROR**: Critical failures and exceptions

### Sample Log Output
```
2025-08-12 14:30:10 - INFO - ICEScraper initialized successfully
2025-08-12 14:30:11 - INFO - Setting up Firefox WebDriver...
2025-08-12 14:30:12 - INFO - WebDriver setup completed successfully
2025-08-12 14:30:13 - INFO - Loading page: https://ice.ir/
2025-08-12 14:30:18 - INFO - Page loaded successfully. Content length: 245678 characters
2025-08-12 14:30:19 - INFO - Extracted 147 dates, 147 prices, 147 names
2025-08-12 14:30:19 - INFO - Created DataFrame with 147 rows
2025-08-12 14:30:19 - INFO - Checking for duplicate records...
2025-08-12 14:30:19 - WARNING - Removed 3 duplicate records
2025-08-12 14:30:20 - INFO - Saving 144 rows to database table 'scraped'...
2025-08-12 14:30:21 - INFO - Data saved to database successfully
2025-08-12 14:30:21 - INFO - Scraping workflow completed in 11.34 seconds
```

## 🔧 Advanced Usage

### Custom CSS Selectors
```python
scraper = ICEScraper()
scraper.selectors = {
    'dates': '.custom-date-selector',
    'prices': '.custom-price-selector', 
    'names': '.custom-name-selector'
}
```

### Error Handling
```python
try:
    scraper = ICEScraper(log_level=logging.DEBUG)
    success = scraper.scrape_and_store()
    
    if not success:
        # Handle failure - check logs for details
        print("Check ./logs/ for detailed error information")
        
except Exception as e:
    print(f"Critical error: {str(e)}")
```

## 🛠️ Troubleshooting

### Common Issues

**1. WebDriver Not Found**
```
FileNotFoundError: Geckodriver not found: D:\geckodriver.exe
```
- Download geckodriver from Mozilla's GitHub releases
- Update the `geckodriver_path` parameter

**2. Firefox Not Found**
```
FileNotFoundError: Firefox binary not found
```
- Install Firefox or update `firefox_binary_path`

**3. Page Loading Timeout**
```
TimeoutException: Timeout while loading page
```
- Check internet connection
- Verify ICE.ir website is accessible
- Increase timeout in `_scrape_page_content`

**4. Database Connection Issues**
```
SQLAlchemyError: Database connection failed
```
- Verify SQL Server is running
- Check ODBC Driver 17 installation
- Update connection string with correct credentials

### Debug Mode
Enable detailed logging for troubleshooting:
```python
scraper = ICEScraper(log_level=logging.DEBUG)
```

## 🚀 Performance Tips

### Optimization Settings
The scraper includes several performance optimizations:
- **Headless mode**: No GUI rendering overhead
- **Disabled extensions**: Faster browser startup
- **Optimized timeouts**: Balanced speed vs reliability
- **Resource management**: Proper cleanup and memory management

## 📦 Dependencies

```txt
selenium>=4.0.0
beautifulsoup4>=4.9.0
pandas>=1.3.0
sqlalchemy>=1.4.0
pyodbc>=4.0.30
```


## 📞 Support

For issues and questions:
1. Check the [logs](#logging) for detailed error information
2. Review [troubleshooting](#troubleshooting) section
3. Open an issue with log details and system information
4. Contact me at [alisadeghiaghili@gmail.com](alisadeghiaghili@gmail.com)
