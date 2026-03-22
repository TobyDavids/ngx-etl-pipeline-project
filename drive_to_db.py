"""
Drive to DB Pipeline
====================
Reads all worksheets from the three financial Excel files
(income_statement, balance_sheet, cash_flow), cleans and transforms
each sheet to long format, then loads into three separate Neon
PostgreSQL tables.

Tables created:
    {NEON_TABLE}_income_statement
    {NEON_TABLE}_balance_sheet
    {NEON_TABLE}_cash_flow

Environment variables (set as GitHub Secrets):
    NEON_CONN  — PostgreSQL connection string
    NEON_TABLE — base table name

Usage:
    python drive_to_db.py
"""

import os
import re
import warnings
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

warnings.filterwarnings("ignore")

# ── Secrets ────────────────────────────────────────────────────────────────
neon_conn  = os.getenv("NEON_CONN")
neon_table = os.getenv("NEON_TABLE")

# ── Three table names derived from the base secret ────────────────────────
TABLE_INCOME   = f"{neon_table}_income_statement"
TABLE_BALANCE  = f"{neon_table}_balance_sheet"
TABLE_CASHFLOW = f"{neon_table}_cash_flow"

STATEMENT_TABLE_MAP = {
    "income_statement": TABLE_INCOME,
    "balance_sheet":    TABLE_BALANCE,
    "cash_flow":        TABLE_CASHFLOW,
}


# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")

FILE_STATEMENT_MAP = {
    r"income.?statement": "income_statement",
    r"balance.?sheet":    "balance_sheet",
    r"cash.?flow":        "cash_flow",
}

SKIP_SHEETS = {"sheet1", "cover", "contents", "readme", "notes"}


# ══════════════════════════════════════════════════════════════════════════════
# 2. DETECT STATEMENT TYPE FROM FILENAME
# ══════════════════════════════════════════════════════════════════════════════

def detect_statement_type(filename):
    for pattern, label in FILE_STATEMENT_MAP.items():
        if re.search(pattern, filename, re.IGNORECASE):
            return label
    return "unknown"


# ══════════════════════════════════════════════════════════════════════════════
# 3. FIND HEADER ROW
# ══════════════════════════════════════════════════════════════════════════════

def find_header_row(filepath, sheet_name):
    try:
        ext = os.path.splitext(filepath)[1].lower()
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        peek = pd.read_excel(
            filepath, sheet_name=sheet_name,
            header=None, nrows=25, dtype=str,
            engine=engine
        )
    except Exception:
        return 12

    for i, row in peek.iterrows():
        row_str = " ".join(row.dropna().astype(str))
        if len(re.findall(r"FQ\d", row_str)) >= 3:
            return i

    return 12


# ══════════════════════════════════════════════════════════════════════════════
# 4. COMPANY NAME EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

def extract_company_name(filepath, sheet_name):
    try:
        ext = os.path.splitext(filepath)[1].lower()
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        peek = pd.read_excel(
            filepath, sheet_name=sheet_name,
            header=None, nrows=5, dtype=str,
            engine=engine
        )
    except Exception:
        return sheet_name

    skip_patterns = [
        "ngse:", "mi key", "spciq", "source:", "period",
        "currency", "magnitude", "reporting", "sort order",
        "s&p capital", "fiscal", "quarter", "standard",
        "spot exchange", "average exchange"
    ]

    for row_idx in range(len(peek)):
        row_cells = [
            str(peek.iloc[row_idx, c]).strip()
            for c in range(peek.shape[1])
            if str(peek.iloc[row_idx, c]).strip() not in ["", "nan", "None"]
        ]
        if not row_cells:
            continue

        cell = row_cells[0]
        if any(p in cell.lower() for p in skip_patterns):
            continue

        match = re.match(r"^(.+?)\s*\|", cell)
        if match:
            name = match.group(1).strip()
            if len(name) > 3:
                return _clean_name(name)

        match = re.match(
            r"^(.+?(?:plc|ltd|group|bank|holdings?))\s+\w+\s+statement",
            cell, re.IGNORECASE
        )
        if match:
            name = match.group(1).strip()
            if len(name) > 3:
                return _clean_name(name)

        if (re.search(r"\b(plc|ltd|bank|group|holdings?)\b", cell, re.IGNORECASE)
                and len(cell) < 60
                and not re.search(r"\d", cell)):
            return _clean_name(cell)

    return sheet_name.strip().title()


def _clean_name(name):
    name = re.split(r"\||[\(\[]", name)[0]
    name = re.sub(r"\s+", " ", name).strip().title()
    name = re.sub(r"\bPlc\b", "Plc", name)
    name = re.sub(r"\bLtd\b", "Ltd", name)
    name = re.sub(r"\bAnd\b", "and", name)
    name = re.sub(r"\bOf\b",  "of",  name)
    return name


# ══════════════════════════════════════════════════════════════════════════════
# 5. CLEANING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

NOISE_PATTERNS = [
    "current/restated", "period ended", "financial filing",
    "spot exchange", "average exchange", "ciq restatement",
    "ciq calculation", "per share items", "supplemental items",
    "supplemental operating", "(₦000)", "(₦)", "ngse:", "mi key"
]

def is_noise_row(label):
    if pd.isna(label):
        return True
    label_lower = str(label).strip().lower()
    if not label_lower:
        return True
    return any(p in label_lower for p in NOISE_PATTERNS)


def parse_period_label(col):
    col = str(col).strip()
    match = re.search(r"(\d{4})\s+FQ(\d)", col)
    if match:
        return f"FQ{match.group(2)} {match.group(1)}"
    return col


QUARTER_END = {"FQ1": "03-31", "FQ2": "06-30", "FQ3": "09-30", "FQ4": "12-31"}

def period_to_date(period):
    try:
        q, yr = str(period).strip().split()
        return pd.Timestamp(f"{yr}-{QUARTER_END[q]}")
    except Exception:
        return pd.NaT


def normalise_variable(name):
    name = str(name).strip()
    name = re.sub(r"%", " pct", name, flags=re.IGNORECASE)
    name = re.sub(r"\bpercent(age)?\b", "pct", name, flags=re.IGNORECASE)
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.lower().replace(" ", "_")
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def clean_value(val):
    if pd.isna(val):
        return np.nan
    val = str(val).strip()
    if val in ["NA", "NM", "—", "-", "", " ", "nan", "None"]:
        return np.nan
    val = val.replace("₦", "").replace(",", "").replace(" ", "")
    val = re.sub(r"^\((.+)\)$", r"-\1", val)
    try:
        return float(val)
    except Exception:
        return np.nan


PER_SHARE_VARS = {
    "basic_eps", "basic_eps_excl_extra_items",
    "diluted_eps_incl_extra_items", "diluted_eps_excl_extra_items",
    "normalized_basic_eps", "normalized_diluted_eps",
    "dividends_per_share",
}

def assign_unit(variable_name):
    if variable_name.endswith("_pct") or "_pct_" in variable_name:
        return "percent"
    if "shares_out" in variable_name:
        return "shares"
    if "shares_per_depositary" in variable_name:
        return "ratio"
    if (variable_name in PER_SHARE_VARS
            or "eps" in variable_name
            or "per_share" in variable_name):
        return "per share"
    return "thousands"


# ══════════════════════════════════════════════════════════════════════════════
# 6. PARSE ONE SHEET → LONG FORMAT
# ══════════════════════════════════════════════════════════════════════════════

def parse_sheet(filepath, sheet_name, sector):
    header_row   = find_header_row(filepath, sheet_name)
    company_name = extract_company_name(filepath, sheet_name)

    try:
        ext = os.path.splitext(filepath)[1].lower()
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        df_raw = pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            header=header_row,
            index_col=0,
            dtype=str,
            engine=engine
        )
    except Exception as e:
        print(f"    [ERROR] Could not read sheet '{sheet_name}': {e}")
        return pd.DataFrame()

    df_raw.index.name = "variable"
    df_raw.dropna(how="all", inplace=True)
    df_raw = df_raw[~df_raw.index.map(is_noise_row)]
    df_raw.dropna(axis=1, how="all", inplace=True)
    df_raw = df_raw[df_raw.index.astype(str).str.strip().astype(bool)]

    df_raw.columns = [parse_period_label(c) for c in df_raw.columns]
    valid_cols = [c for c in df_raw.columns if re.match(r"FQ\d \d{4}", c)]
    df_raw = df_raw[valid_cols]

    if df_raw.empty or not valid_cols:
        print(f"    [WARN] No valid period columns in sheet '{sheet_name}'")
        return pd.DataFrame()

    df_raw.index = (
        df_raw.index.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    df_raw.index = df_raw.index.map(normalise_variable)
    df_raw = df_raw[~df_raw.index.duplicated(keep="first")]

    df = df_raw.reset_index()
    df_long = df.melt(
        id_vars="variable",
        var_name="period",
        value_name="value_raw"
    )

    df_long["value"]    = df_long["value_raw"].apply(clean_value)
    df_long["date"]     = df_long["period"].apply(period_to_date)
    df_long["company"]  = company_name
    df_long["sector"]   = sector
    df_long["currency"] = "NGN"
    df_long["unit"]     = df_long["variable"].apply(assign_unit)
    df_long["source"]   = "SP Capital IQ"

    df_long.drop(columns=["value_raw"], inplace=True)

    df_long = df_long[[
        "company", "sector", "variable",
        "period", "date", "value",
        "currency", "unit", "source"
    ]]

    df_long.sort_values(["variable", "date"], inplace=True)
    df_long.reset_index(drop=True, inplace=True)

    return df_long


# ══════════════════════════════════════════════════════════════════════════════
# 7. PROCESS ONE EXCEL FILE (all sheets)
# ══════════════════════════════════════════════════════════════════════════════

def process_excel_file(filepath):
    filename       = os.path.basename(filepath)
    statement_type = detect_statement_type(filename)

    print(f"\n► File     : {filename}")
    print(f"  Statement: {statement_type}")
    print(f"  Table    : {STATEMENT_TABLE_MAP.get(statement_type, 'unknown')}")

    try:
        # Explicitly set engine based on file extension
        ext = os.path.splitext(filepath)[1].lower()
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        xl = pd.ExcelFile(filepath, engine=engine)
    except Exception as e:
        print(f"  [ERROR] Cannot open file: {e}")
        return statement_type, pd.DataFrame()

    all_sheets = []

    for sheet_name in xl.sheet_names:
        if sheet_name.strip().lower() in SKIP_SHEETS:
            print(f"  ↷ Skipping sheet: {sheet_name}")
            continue

        sector = sheet_name.strip().title()
        print(f"  → Sheet: {sheet_name}  (sector: {sector})")

        df = parse_sheet(filepath, sheet_name, sector)

        if df.empty:
            print(f"    [WARN] Empty result for sheet '{sheet_name}', skipping.")
            continue

        print(f"    Rows     : {len(df):,}")
        print(f"    Variables: {df['variable'].nunique()}")
        print(f"    Periods  : {df['period'].nunique()}")
        print(f"    Company  : {df['company'].iloc[0]}")

        all_sheets.append(df)

    if not all_sheets:
        return statement_type, pd.DataFrame()

    combined = pd.concat(all_sheets, ignore_index=True)
    return statement_type, combined


# ══════════════════════════════════════════════════════════════════════════════
# 8. LOAD TO NEON POSTGRESQL
# ══════════════════════════════════════════════════════════════════════════════

def create_table_if_not_exists(cursor, table):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id        SERIAL PRIMARY KEY,
            company   TEXT,
            sector    TEXT,
            variable  TEXT,
            period    TEXT,
            date      DATE,
            value     DOUBLE PRECISION,
            currency  TEXT,
            unit      TEXT,
            source    TEXT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = '{table}_unique_key'
            ) THEN
                ALTER TABLE {table}
                ADD CONSTRAINT {table}_unique_key
                UNIQUE (company, variable, period);
            END IF;
        END $$;
    """)


def load_to_neon(df, conn_string, table):
    if df.empty:
        print(f"  [WARN] Nothing to load into {table}.")
        return

    print(f"\n  Loading {len(df):,} rows → {table} ...")

    df = df.copy()
    df["date"] = df["date"].where(df["date"].notna(), other=None)

    records = [
        (
            row.company,
            row.sector,
            row.variable,
            row.period,
            row.date.date() if pd.notna(row.date) else None,
            None if pd.isna(row.value) else float(row.value),
            row.currency,
            row.unit,
            row.source
        )
        for row in df.itertuples(index=False)
    ]

    conn   = None
    cursor = None
    try:
        conn   = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        create_table_if_not_exists(cursor, table)

        upsert_sql = f"""
            INSERT INTO {table}
                (company, sector, variable, period,
                 date, value, currency, unit, source)
            VALUES %s
            ON CONFLICT (company, variable, period)
            DO UPDATE SET
                value     = EXCLUDED.value,
                sector    = EXCLUDED.sector,
                date      = EXCLUDED.date,
                currency  = EXCLUDED.currency,
                unit      = EXCLUDED.unit,
                source    = EXCLUDED.source,
                loaded_at = NOW();
        """

        execute_values(cursor, upsert_sql, records, page_size=500)
        conn.commit()
        print(f"  ✓ Successfully loaded {len(records):,} rows into {table}")

    except Exception as e:
        print(f"  [ERROR] Failed to load into {table}: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# 9. MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():

    if not neon_conn:
        raise EnvironmentError("NEON_CONN environment variable is not set.")
    if not neon_table:
        raise EnvironmentError("NEON_TABLE environment variable is not set.")

    print(f"\n{'═' * 80}")
    print(f"  DRIVE TO DB PIPELINE")
    print(f"  Base name : {neon_table}")
    print(f"  Tables    : {TABLE_INCOME}")
    print(f"              {TABLE_BALANCE}")
    print(f"              {TABLE_CASHFLOW}")
    print(f"{'═' * 80}")

    all_files = [
        os.path.join(DATA_FOLDER, f)
        for f in os.listdir(DATA_FOLDER)
        if f.endswith((".xlsx", ".xls"))
        and not f.startswith("~$")
    ]

    if not all_files:
        print(f"[ERROR] No Excel files found in: {DATA_FOLDER}")
        return

    print(f"\n  Found {len(all_files)} Excel file(s)\n")

    # Group DataFrames by statement type
    grouped = {
        "income_statement": [],
        "balance_sheet":    [],
        "cash_flow":        [],
    }

    for filepath in sorted(all_files):
        statement_type, df = process_excel_file(filepath)
        if not df.empty and statement_type in grouped:
            grouped[statement_type].append(df)

    # Load each group into its own table
    print(f"\n{'═' * 80}")
    print(f"  LOADING TO NEON")
    print(f"{'═' * 80}")

    for statement_type, dfs in grouped.items():
        table = STATEMENT_TABLE_MAP[statement_type]

        if not dfs:
            print(f"\n  [WARN] No data for {statement_type}, skipping.")
            continue

        combined = pd.concat(dfs, ignore_index=True)
        combined.sort_values(["company", "variable", "date"], inplace=True)
        combined.reset_index(drop=True, inplace=True)

        print(f"\n  {statement_type.upper()}")
        print(f"  Rows      : {len(combined):,}")
        print(f"  Companies : {combined['company'].nunique()}")
        print(f"  Sectors   : {combined['sector'].nunique()}")
        print(f"  Variables : {combined['variable'].nunique()}")

        load_to_neon(combined, neon_conn, table)

    print(f"\n{'═' * 80}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'═' * 80}\n")


if __name__ == "__main__":
    run_pipeline()
