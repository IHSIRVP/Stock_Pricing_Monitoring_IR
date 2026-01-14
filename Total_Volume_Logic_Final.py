import pandas as pd
import datetime as dt
import requests
import zipfile
import io
from BSE_Data_Download import Today_BSE_Bhav


# ============================================================
# 1. EXTRACT COMPANIES BY ISIN
# ============================================================

def extract_companies_by_isin(df, isin_mapping):
    isin_list = list(isin_mapping.values())
    filtered_df = df[df["ISIN"].isin(isin_list)].copy()
    filtered_df["Company"] = filtered_df["ISIN"].map({v: k for k, v in isin_mapping.items()})
    cols = ["Company"] + [c for c in filtered_df.columns if c != "Company"]
    return filtered_df[cols]


isin_mapping = {
    "Sunteck Realty": "INE805D01034",
    "OBEROI REALTY": "INE093I01010",
    "DLF": "INE271C01023",
    "Embassy Development Ltd": "INE069I01010",
    "PRESTIGE ESTATE": "INE811K01011",
    "HDIL": "INE191I01012",
    "PHOENIX MILLS": "INE211B01039",
    "GODREJ PROPERTIES Ltd.": "INE484J01027",
    "SOBHA LTD.": "INE671H01015",
    "MAHINDRA LIFESPACES": "INE813A01018",
    "OMAXE": "INE800H01010",
    "KOLTE PATIL LTD.": "INE094I01018",
    "BRIGADE ENTERPRISES": "INE791I01019",
    "UNITECH": "INE694A01020",
    "ANANT RAJ": "INE242C01024",
    "ASHIANA HOUSING": "INE365D01021",
    "EMBASSY": "INE041025011",
    "BROOKFIELD": "INE0FDU25010",
    "MINDSPACE": "INE0CCU25019",
    "Lodha Macrotech": "INE670K01029",
    "RUSTOMJEE KEYSTONE": "INE263M01029",
    "Signature Global": "INE903U01023",
    "Puravankara": "INE323I01011",
    "Suraj Estate": "INE843S01025",
    "Arkade Developers": "INE0QRL01017",
    "Kalpataru Ltd": "INE227J01012",
    "Raymond Realty": "INE1SY401010",
    "Sri Lotus Developers": "INE0V9Q01010",
    "Knowledge Realty Trust": "INE1JAR25012",
    "NBCC (India) Ltd.": "INE095N01031",
    "Adtiya Birla Real estate Ltd": "INE055A01016",
    "Valor Estate Ltd": "INE879I01012",
    "Max Estate Ltd": "INE03EI01018",
    "Welspun Enterprise Ltd": "INE625G01013",
    "Ganesh Housing Ltd.": "INE460C01014",
    "Ahluwalia Contracts (India) Ltd.": "INE758C01029",
    "Hubtown Ltd.": "INE703H01016",
    "TARC Ltd.": "INE0EK901012",
    "Marathon Nextgen Realty Ltd.": "INE182D01020",
    "Hemisphere Properties Ltd.": "INE0AJG01018",
    "Ajmera Realty & Infra India Ltd": "INE298G01035"
}

def extract(df):
    return extract_companies_by_isin(df, isin_mapping)


# ============================================================
# 2. STANDARDIZE FORMAT
# ============================================================

def standardize(df, exchange):
    df['TradeDate'] = pd.to_datetime(df['TradDt']).dt.strftime("%Y-%m-%d")
    df[f"Vol_{exchange}"] = df['TtlTradgVol']
    return df[['Company', 'ISIN', 'TradeDate', f"Vol_{exchange}"]]


# ============================================================
# 3. CUSTOM NSE BHAVCOPY LOADER (with headers)
# ============================================================

def load_nse_bhavcopy_custom(date_obj):

    headers = {
        "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    date_str = date_obj.strftime("%Y%m%d")
    yyyy_mm_dd = date_obj.strftime("%Y-%m-%d")
    

    url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
    print(url)
    print(f"🔍 Trying NSE: {yyyy_mm_dd}")

    try:
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"❌ NSE Bhavcopy not found (Status {response.status_code})")
            return None

        z = zipfile.ZipFile(io.BytesIO(response.content))
        csv_file = z.namelist()[0]
        df = pd.read_csv(z.open(csv_file))

        print(f"✅ NSE Bhavcopy loaded for {yyyy_mm_dd}")
        return df

    except Exception as e:
        print(f"❌ NSE Error: {e}")
        return None


# ============================================================
# 4. MAIN UPDATE FUNCTION
# ============================================================

def update_combined_volume(target_date):

    today = dt.datetime.now().date()

    if isinstance(target_date, dt.datetime):
        target_date = target_date.date()

    if target_date > today:
        print(f"❌ Cannot run for a future date: {target_date}. Today is {today}.")
        return None

    dt_obj = dt.datetime.combine(target_date, dt.datetime.min.time())

    # ---- Load panel ----
    try:
        panel = pd.read_csv("Combined_Volume_Panel.csv")

        # 🔥 REMOVE ANY UNNAMED COLUMNS
        panel = panel.loc[:, ~panel.columns.str.contains("^Unnamed")]

    except FileNotFoundError:
        print("❌ Combined_Volume_Panel.csv not found.")
        return

    col_name = f"Vol_{target_date}"

    # Already exists
    if col_name in panel.columns:
        print(f"Column {col_name} already exists.")
        return panel

    # ---- NSE ----
    nse_df = load_nse_bhavcopy_custom(target_date)

    if nse_df is None:
        print("⚠️ NSE FAILED → Creating ZERO column.")
        panel[col_name] = 0

        # Remove unnamed again
        panel = panel.loc[:, ~panel.columns.str.contains("^Unnamed")]

        panel.to_csv("Combined_Volume_Panel.csv", index=False)
        return panel

    filtered_NSE = extract(nse_df)

    # ---- BSE ----
    try:
        bse_df = Today_BSE_Bhav(dt_obj)
        if bse_df is None:
            raise Exception("No BSE data returned")

        filtered_BSE = extract(bse_df)

    except Exception as e:
        print(f"⚠️ BSE FAILED → {e}")
        panel[col_name] = 0

        panel = panel.loc[:, ~panel.columns.str.contains("^Unnamed")]
        panel.to_csv("Combined_Volume_Panel.csv", index=False)
        return panel

    # ---- MERGE ----
    nse_std = standardize(filtered_NSE, "NSE")
    bse_std = standardize(filtered_BSE, "BSE")

    combined = pd.merge(
        nse_std, bse_std,
        on=["Company", "ISIN", "TradeDate"],
        how="outer"
    )

    combined["Vol_NSE"] = combined["Vol_NSE"].fillna(0)
    combined["Vol_BSE"] = combined["Vol_BSE"].fillna(0)
    combined["TotalVol"] = combined["Vol_NSE"] + combined["Vol_BSE"]

    combined_day = combined[["Company", "ISIN", "TotalVol"]]

    panel = pd.merge(panel, combined_day, on=["Company", "ISIN"], how="left")
    panel.rename(columns={"TotalVol": col_name}, inplace=True)
    panel[col_name] = panel[col_name].fillna(0)

    # 🔥 FINAL CLEANUP: Remove unnamed columns again
    panel = panel.loc[:, ~panel.columns.str.contains("Unnamed")]

    panel.to_csv("Combined_Volume_Panel.csv", index=False)
    print(panel)

    return panel


# ============================================================
# FILL MISSING DATE COLUMNS (NEW)
# ============================================================

def fill_missing_date_columns(panel):

    # Extract existing volume date columns
    vol_cols = [c for c in panel.columns if c.startswith("Vol_")]

    if not vol_cols:
        print("⚠️ No Vol_ columns exist. Skipping fill.")
        return panel

    # Convert Vol_YYYY-MM-DD → actual datetime
    existing_dates = sorted([
        pd.to_datetime(c.replace("Vol_", "")).date()
        for c in vol_cols
    ])

    start_date = existing_dates[0]
    end_date = existing_dates[-1]

    print(f"🔍 Checking missing dates between {start_date} → {end_date}")

    all_calendar_dates = pd.date_range(start=start_date, end=end_date)

    for dt_obj in all_calendar_dates:
        date = dt_obj.date()
        col_name = f"Vol_{date}"

        if col_name in panel.columns:
            continue  # column already exists

        print(f"⚠️ Missing date column: {col_name} → Checking trading status")

        # Try NSE first to check if it is a trading day
        nse_df = load_nse_bhavcopy_custom(dt_obj)

        if nse_df is None:
            # Not a trading day → add a zero column
            print(f"🚫 {date} is NOT a trading day → Adding ZERO column")
            panel[col_name] = 0
            continue

        # Trading day → extract volumes
        print(f"📈 {date} IS a trading day → Fetching NSE/BSE volumes")

        # Extract from NSE
        filtered_nse = extract(nse_df)
        nse_std = standardize(filtered_nse, "NSE")

        # BSE
        try:
            bse_df = Today_BSE_Bhav(dt.datetime.combine(date, dt.datetime.min.time()))
            filtered_bse = extract(bse_df)
            bse_std = standardize(filtered_bse, "BSE")
        except:
            print(f"⚠️ BSE failed for {date}, using only NSE")
            bse_std = pd.DataFrame(columns=["Company", "ISIN", "TradeDate", "Vol_BSE"])

        combined = pd.merge(nse_std, bse_std, on=["Company", "ISIN", "TradeDate"], how="outer")
        combined["Vol_NSE"] = combined["Vol_NSE"].fillna(0)
        combined["Vol_BSE"] = combined["Vol_BSE"].fillna(0)
        combined["TotalVol"] = combined["Vol_NSE"] + combined["Vol_BSE"]

        combined_day = combined[["Company", "ISIN", "TotalVol"]]

        # Merge into panel
        panel = pd.merge(panel, combined_day, on=["Company", "ISIN"], how="left")
        panel.rename(columns={"TotalVol": col_name}, inplace=True)
        panel[col_name] = panel[col_name].fillna(0)

    # Cleanup
    panel = panel.loc[:, ~panel.columns.str.contains("Unnamed")]

    return panel


# ============================================================
# 5. 3-MONTH ADJUSTED AVERAGE FUNCTION (corrected)
# ============================================================

def get_3m_average(volume_df, target_date, window=96):
    """
    Corrected logic:
    Average = SUM(window values) / (# of non-zero days in window)
    """

    if not isinstance(target_date, str):
        target_date = str(target_date)

    target_col = f"Vol_{target_date}"
    print("Target column:", target_col)

    if target_col not in volume_df.columns:
        raise ValueError(f"{target_col} not found")

    # Get date columns
    date_cols = [c for c in volume_df.columns if c.startswith("Vol_")]
    print("Total Vol_ columns:", len(date_cols))

    # Sort columns by actual date
    sorted_cols = sorted(date_cols, key=lambda x: pd.to_datetime(x.replace("Vol_", "")))

    idx = sorted_cols.index(target_col)

    start_idx = max(0, idx - window + 1)
    window_cols = sorted_cols[start_idx : idx + 1]

    print("Columns used:", len(window_cols))

    row_sum = volume_df[window_cols].sum(axis=1)

    zero_counts = (volume_df[window_cols] == 0).sum(axis=1)

    non_zero_days = len(window_cols) - zero_counts

    non_zero_days = non_zero_days.replace(0, 1)

    volume_df["3M_Avg"] = row_sum / non_zero_days
    print(row_sum)
    print(non_zero_days)

    return volume_df[["Company", "ISIN", "3M_Avg"]]
