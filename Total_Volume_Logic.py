import pandas as pd
import datetime as dt   # <<< FIXED: unified datetime import
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
    "Ajmera Realty & Infra India Ltd": "INE298G01027"
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
# 3. CUSTOM NSE BHAVCOPY LOADER
# ============================================================

def load_nse_bhavcopy_custom(date_obj):
    date_str = date_obj.strftime("%d%m%Y")
    yyyy_mm_dd = date_obj.strftime("%Y-%m-%d")

    url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"

    print(f"🔍 Trying NSE: {yyyy_mm_dd}")

    try:
        response = requests.get(url, timeout=10)

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
    """
    Compute NSE+BSE volume for given date.
    If ANY exchange fails → add ZERO column & return.
    Reject future dates.
    """

    # ---- FIXED FUTURE DATE VALIDATION ----
    today = dt.datetime.now().date()

    # Convert target_date if datetime
    if isinstance(target_date, dt.datetime):
        target_date = target_date.date()

    if target_date > today:
        print(f"❌ Cannot run for a future date: {target_date}. Today is {today}.")
        return None

    # Convert to datetime object
    dt_obj = dt.datetime.combine(target_date, dt.datetime.min.time())

    # Load existing CSV
    try:
        panel = pd.read_csv("Combined_Volume_Panel.csv")
    except FileNotFoundError:
        print("❌ Combined_Volume_Panel.csv not found.")
        return

    col_name = f"Vol_{target_date}"

    # Prevent duplicate-run
    if col_name in panel.columns:
        print(f"Column {col_name} already exists.")
        return panel

    # ---- LOAD NSE ----
    nse_df = load_nse_bhavcopy_custom(target_date)

    if nse_df is None:
        print("⚠️ NSE FAILED → Creating ZERO column.")
        panel[col_name] = 0
        panel.to_csv("Combined_Volume_Panel.csv", index=False)
        return panel

    filtered_NSE = extract(nse_df)

    # ---- LOAD BSE ----
    try:
        bse_df = Today_BSE_Bhav(dt_obj)
        if bse_df is None:
            raise Exception("No BSE data returned")

        filtered_BSE = extract(bse_df)

    except Exception as e:
        print(f"⚠️ BSE FAILED → {e}")
        print("⚠️ Creating ZERO column.")
        panel[col_name] = 0
        panel.to_csv("Combined_Volume_Panel.csv", index=False)
        return panel

    # ---- MERGE NSE & BSE ----
    nse_std = standardize(filtered_NSE, "NSE")
    bse_std = standardize(filtered_BSE, "BSE")

    combined = pd.merge(
        nse_std,
        bse_std,
        on=["Company", "ISIN", "TradeDate"],
        how="outer"
    )

    combined["Vol_NSE"] = combined["Vol_NSE"].fillna(0)
    combined["Vol_BSE"] = combined["Vol_BSE"].fillna(0)
    combined["TotalVol"] = combined["Vol_NSE"] + combined["Vol_BSE"]

    combined_day = combined[["Company", "ISIN", "TotalVol"]]

    # Merge into panel file
    panel = pd.merge(panel, combined_day, on=["Company", "ISIN"], how="left")
    panel.rename(columns={"TotalVol": col_name}, inplace=True)
    panel[col_name] = panel[col_name].fillna(0)

    # ---- SAVE BACK TO CSV ----
    panel.to_csv("Combined_Volume_Panel.csv", index=False)

    return panel


# ============================================================
# 5. RUN IT
# ============================================================


import pandas as pd

def get_3m_average(volume_df, target_date, window=96):
    """
    Compute adjusted 3-month average:

    3M_Avg = SUM(last 89 days) / (89 - count(zeros in window))

    Zero-volume days are excluded from denominator.
    """

    # Ensure string input
    if not isinstance(target_date, str):
        target_date = str(target_date)

    target_col = f"Vol_{target_date}"
    print("Target column:", target_col)

    if target_col not in volume_df.columns:
        raise ValueError(f"{target_col} not found in panel")

    # ---- STEP 1: Get all date columns ----
    date_cols = [c for c in volume_df.columns if c.startswith("Vol_")]
    print("Total Vol_ columns:", len(date_cols))

    # ---- STEP 2: Sort date columns properly ----
    sorted_cols = sorted(date_cols, key=lambda x: pd.to_datetime(x.replace("Vol_", "")))

    # ---- STEP 3: Locate target column ----
    idx = sorted_cols.index(target_col)

    # ---- STEP 4: Select last 89 columns ----
    start_idx = max(0, idx - window + 1)
    window_cols = sorted_cols[start_idx : idx + 1]

    print("Columns used in window:", len(window_cols))

    # ---- STEP 5: Row sums (over 89 columns) ----
    row_sum = volume_df[window_cols].sum(axis=1)

    # ---- STEP 6: Count zero-volume days ----
    zero_counts = (volume_df[window_cols] == 0).sum(axis=1)

    # ---- STEP 7: Compute adjusted denominator ----
    adjusted_denominator = window - zero_counts +1 
    # Prevent divide-by-zero (if all days are zero)
    adjusted_denominator = adjusted_denominator.replace(0, 1)

    # ---- STEP 8: Final average ----
    volume_df["3M_Avg"] = row_sum / adjusted_denominator

    return volume_df[["Company", "ISIN", "3M_Avg"]]
