# NGX ETL Pipeline – Senko Finance Project

## 📌 Overview
This project is a **Production-Ready ETL (Extract, Transform, Load) pipeline** designed to automatically pull, clean, and load **daily Nigerian Stock Exchange (NGX)** data into Senko Finance’s analytics environment.

The pipeline is fully automated using **GitHub Actions**, running **every weekday at 4:30 PM WAT** (15:30 UTC) to ensure our team always has fresh, accurate market data for analysis and decision-making.

---

## ⚙ How It Works
1. **Extract**  
   - Connects to the NGX website and retrieves the latest daily trading data using Python.
   - Uses Chromium & ChromeDriver for dynamic content scraping.

2. **Transform**  
   - Cleans and structures the raw market data.
   - Formats it for our analytics systems and applies business logic (e.g., filtering symbols, adding computed metrics).

3. **Load**  
   - Loads the cleaned dataset into our **PostgreSQL (Neon DB)** table.
   - Ensures incremental updates without duplications.

4. **Automation & Notifications**  
   - **GitHub Actions** runs the ETL script automatically on schedule or via manual trigger.
   - Sends a **success/failure log** in GitHub Actions console.
   - Uses environment secrets for secure database and email credentials.

---

## 📅 Schedule
- Runs automatically:
  - **Monday to Friday**
  - **4:30 PM WAT** (15:30 UTC)
- Can be run **manually** via GitHub Actions “Run workflow” button.

---

## 🛠 Tech Stack
- **Python 3.10** (ETL logic)
- **Pandas & Selenium** (data processing & scraping)
- **PostgreSQL (Neon)** (data storage)
- **GitHub Actions** (automation)
- **Chromium & ChromeDriver** (browser automation)

---

## 🎯 Impact at Senko Finance
This ETL pipeline enables:
- **Timely market insights** – Always working with the latest NGX data.
- **Automated workflows** – No manual downloads or processing.
- **Data accuracy & consistency** – Reduces errors in analysis.
- **Scalable infrastructure** – Easy to expand to other exchanges or data sources.

As a **Data Engineer & Analyst** at Senko Finance, this pipeline empowers me to:
- Deliver up-to-date market dashboards for traders & analysts.
- Free up valuable time from repetitive manual tasks.
- Support better investment decisions with fresh, reliable data.

---

## 🚀 Running Locally (Optional)
If you want to test the ETL locally:
```bash
# Clone repository
git clone https://github.com/your-username/ngx-etl.git
cd ngx-etl

# Install dependencies
pip install -r requirements.txt

# Run ETL
python ngx_etl.py
