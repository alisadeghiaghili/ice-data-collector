# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 11:01:08 2025

@author: sadeghi.a
"""
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import time
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, CHAR, NVARCHAR, BigInteger


url = "https://ice.ir/"

firefox_binary_path = r"path_to_firefox.exe"
options = Options()
options.binary_location = firefox_binary_path

options.add_argument("--headless")  
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                                      "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
service = Service(r"path_to_geko")
driver = webdriver.Firefox(service=service, options=options)


driver.get(url)


time.sleep(5)



html = driver.page_source

driver.quit()

soup = BeautifulSoup(html, "html.parser")
dates = [date.get_text(strip=True).strip() for date in soup.select(".pt-4 .text-light-blue")]
prices = [price.get_text(strip=True).strip() for price in soup.select(".pt-4 h2.text-light")]
names = [name.get_text(strip=True).strip() for name in soup.select(".pt-4 h4.text-light")]

 
scraped_data = pd.DataFrame({'Date':dates, 'Name':names, 'Price':prices})
scraped_data['ScrapeDate'] = datetime.strftime(datetime.now(), '%Y-%m-%d')
scraped_data['ScrapeTime'] = datetime.strftime(datetime.now(), '%T')

def clean_persian_number(text: str):
    """
    Clean and convert Persian numbers to float.
    
    Args:
        text (str): Text containing Persian numbers
        
    Returns:
        Optional[float]: Converted number or None if conversion fails
    """
    if not text:
        return None
        
    
    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
    english_digits = '0123456789'
    
    
    for persian, english in zip(persian_digits, english_digits):
        text = text.replace(persian, english)
    
    
    cleaned = re.sub(r'[^\d.]', '', text)
    
    if cleaned:
        return float(cleaned)
    
def correct_date_format(text):
    text = str(text)
    return text[:4] + '/' + text[4:6] + '/' + text[6:8]


for col in scraped_data.columns:
    if col in ["Date", "Price"]:
        scraped_data[col] = scraped_data[col].apply(clean_persian_number)
        
        if col == "Date":
            scraped_data[col]= scraped_data[col].apply(correct_date_format)
        
        if col == "Price":
            scraped_data[col]= pd.to_numeric(scraped_data[col], errors='coerce').fillna(0).astype(float)
    

conn = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
engine = create_engine(conn)

dtype_mapping = {
    'Date': CHAR(10),
    'Name': NVARCHAR(100),
    'Price': BigInteger(),
    'ScrapeDate': CHAR(10),
    'ScrapeTime': CHAR(8)
}

scraped_data.to_sql(
    name='scraped',      
    con=engine,          
    if_exists='append',  
    index=False,          
    dtype=dtype_mapping
)


