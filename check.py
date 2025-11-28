import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# --------------------------
# ISIN Mapping
# --------------------------

rows = []

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

isin_list = list(isin_mapping.values())

# --------------------------
# Master DF Structure
# --------------------------

master_df = pd.DataFrame(columns=["Date"] + list(isin_mapping.keys()))

# --------------------------
# Loop Dates
# --------------------------

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 10, 20)

current = start_date

while current <= end_date:
    date_str = current.strftime("%Y%m%d")
    print("Processing:", date_str)

    url = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20230101_F_0000.csv.zip"
    output_file = "BhavCopy_20251119.csv.zip"

    # NSE website often blocks requests without proper headers — these work reliably
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open(output_file, "wb") as f:
            f.write(response.content)
        print("Download successful:", output_file)
    else:
        print("Failed with status code:", response.status_code)
        print("Response:", response.text[:200])


    if response.status_code != 200 or len(response.content) < 500:
        print("   → BhavCopy not available.")
        current += timedelta(days=1)
        continue

    temp_file = f"temp_{date_str}.csv"
    with open(temp_file, "wb") as f:
        f.write(response.content)

    try:
        df = pd.read_csv(temp_file)
    except:
        print("   → CSV unreadable.")
        current += timedelta(days=1)
        continue

    filtered = df[df["ISIN"].isin(isin_list)]

    row = {"Date": date_str}
    for company, isin in isin_mapping.items():
        matched = filtered[filtered["ISIN"] == isin]
        if not matched.empty:
            row[company] = float(matched["ClsPric"].values[0])
        else:
            row[company] = None

    rows.append(row)

    os.remove(temp_file)
    current += timedelta(days=1)

# --------------------------
# Build DataFrame ONCE (no warnings)
# --------------------------

master_df = pd.DataFrame(rows)

master_df.to_csv("Master_RealEstate_Prices_2025.csv", index=False)

print("\nSaved → Master_RealEstate_Prices_2025.csv")