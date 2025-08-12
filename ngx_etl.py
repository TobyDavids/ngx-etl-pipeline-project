# import needed libraries
import os
import time
import smtplib
from datetime import datetime
import re

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from sqlalchemy import create_engine, text
import smtplib


URL = "https://ngxgroup.com/exchange/data/equities-price-list/"

# Read configs from environment variables
NEON_CONN = os.environ.get("NEON_CONN")
NEON_TABLE = os.environ.get("NEON_TABLE", "my_table")

CHROME_DRIVER_PATH = os.environ.get("CHROME_DRIVER_PATH", "/usr/lib/chromium-browser/chromedriver")

# -------- helpers ----------
def numeric_clean(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    if s in ("", "--", "-"):
        return pd.NA
    s = re.sub(r"[^\d\.]", "", s)  # remove commas and other non-digit except dot
    try:
        return float(s)
    except:
        return pd.NA

# -------- scraping & transform ----------
def scrape_ngx_table():
    print("Starting browser for scraping...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 20)

    try:
        driver.get(URL)
        time.sleep(2)

        try:
            cookie = wait.until(EC.element_to_be_clickable((By.ID, "cookie_action_close_header")))
            driver.execute_script("arguments[0].click();", cookie)
            print("Closed cookie popup")
            time.sleep(1)
        except Exception:
            print("No cookie popup found")

        try:
            sel = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#latestdiclosuresEquities_length select")))
            options_e = sel.find_elements(By.TAG_NAME, "option")
            if options_e:
                options_e[-1].click()
                print("Expanded table to show all rows")
                time.sleep(2)
        except Exception:
            print("Could not expand table options")

        table = wait.until(EC.presence_of_element_located((By.ID, "latestdiclosuresEquities")))
        html = table.get_attribute("outerHTML")
        soup = BeautifulSoup(html, "html.parser")

        headers = [th.get_text(strip=True) for th in soup.find("thead").find_all("th")]
        rows = []
        for tr in soup.find("tbody").find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            rows.append(cells)

        if not headers or not rows:
            raise RuntimeError("No table headers or rows parsed")

        df = pd.DataFrame(rows, columns=headers)
        print(f"Raw scraped data preview:\n{df.head()}")
        return df
    finally:
        driver.quit()
        print("Browser closed.")

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    # Strip any trailing chars after company name starting with "[" if "Company" in df.columns
    if "Company" in df.columns:
        df["Company"] = df["Company"].astype(str).str.strip().str.split(r"\s|\[", n=1).str[0]

    # Replace "--" with pd.NA everywhere
    df.replace("--", pd.NA, inplace=True)

    # Select and rename columns for the DB mapping
    column_map = {
        "Opening Price": "Day Open Price(₦)",
        "High": "Day High Price(₦)",
        "Low": "Day Low Price(₦)",
        "Close": "Share Price (Daily)(₦)",
        "Volume": "Volume (Daily)",
        "Company": "Company ID",
        "Trade Date": "Pricing Date"
    }

    # Make sure all keys exist, else fail early
    missing_cols = [k for k in column_map.keys() if k not in df.columns]
    if missing_cols:
        raise RuntimeError(f"Missing expected columns in raw data: {missing_cols}")

    out_df = pd.DataFrame()
    for src_col, tgt_col in column_map.items():
        out_df[tgt_col] = df[src_col]

    # Clean numeric columns
    for col in ["Day Open Price(₦)", "Day High Price(₦)", "Day Low Price(₦)", "Share Price (Daily)(₦)"]:
        out_df[col] = out_df[col].apply(numeric_clean).astype("Float64")

    # Clean Volume column: remove commas, convert to float
    out_df["Volume (Daily)"] = out_df["Volume (Daily)"].apply(numeric_clean).astype("Float64")

    # Parse Pricing Date (e.g., "12 Aug 25" to timestamp 2025-08-12)
    out_df["Pricing Date"] = pd.to_datetime(out_df["Pricing Date"], format="%d %b %y", errors="coerce")
    if out_df["Pricing Date"].isna().any():
        # Try generic parse if strict failed
        out_df["Pricing Date"] = pd.to_datetime(out_df["Pricing Date"], errors="coerce")

    # Remove rows with missing Pricing Date or Company ID
    out_df["Company ID"] = out_df["Company ID"].astype(str).str.strip()
    out_df.loc[out_df["Company ID"].isin(["nan", "None", ""]), "Company ID"] = pd.NA
    out_df = out_df.dropna(subset=["Pricing Date", "Company ID"], how="any")

    print(f"Transformed data preview:\n{out_df.head()}")
    return out_df

def append_to_db(df: pd.DataFrame):
    if not NEON_CONN:
        raise RuntimeError("NEON_CONN not set in environment")

    engine = create_engine(NEON_CONN, echo=False)
    conn = engine.connect()

    try:
        # Remove duplicates based on Pricing Date and Company ID already in DB
        df_dates = pd.to_datetime(df["Pricing Date"]).dt.date.unique().tolist()

        for d in df_dates:
            delete_sql = text(f"DELETE FROM {NEON_TABLE} WHERE DATE(\"Pricing Date\") = :d")
            conn.execute(delete_sql, {"d": d})

        df.to_sql(NEON_TABLE, engine, if_exists="append", index=False, method="multi", chunksize=5000)
        print(f"Appended {len(df)} rows to DB")
    finally:
        conn.close()
        engine.dispose()

def main():
    start = datetime.utcnow()
    try:
        raw = scrape_ngx_table()
        transformed = transform_df(raw)

        if transformed.empty:
            raise RuntimeError("No rows after transformation — nothing to append")

        append_to_db(transformed)

        max_date = transformed["Pricing Date"].max()
        rows = len(transformed)
        print(f"SUCCESS: appended {rows} rows for {max_date}")

    except Exception as e:
        print(f"ERROR: {e}")
        raise

if __name__ == "__main__":
    main()
