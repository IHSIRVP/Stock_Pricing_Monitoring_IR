import pandas as pd

# ---------------------------
# ISIN MAPPING
# ---------------------------

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

# ---------------------------
# CREATE EMPTY DATAFRAME
# ---------------------------

columns = [
    "Company",
    "ISIN",
    "Current Close",
    "Previous Close",
    "Price Change",
    "3M Change",
    "6M Change",
    "9M Change",
    "12M Change",
    "3M Average",
    "52W High-Low"
]

real_estate_df = pd.DataFrame(columns=columns)

# Fill in rows
for company, isin in isin_mapping.items():
    real_estate_df.loc[len(real_estate_df)] = {"Company": company, "ISIN": isin}

# ---------------------------
# FUNCTIONS FOR PRICE EXTRACTION
# ---------------------------

def get_6m_change_closing_price(df_today, isin_list):

    filtered = df_today[df_today["ISIN"].isin(isin_list)]
    return filtered[["ISIN", "ClsPric"]].rename(columns={"ClsPric": "Current Close"})


def get_current_closing_price(df_today, isin_list):

    filtered = df_today[df_today["ISIN"].isin(isin_list)]
    return filtered[["ISIN", "ClsPric"]].rename(columns={"ClsPric": "Current Close"})


def get_previous_closing_price(df_prev, isin_list):

    filtered = df_prev[df_prev["ISIN"].isin(isin_list)]
    return filtered[["ISIN", "ClsPric"]].rename(columns={"ClsPric": "Previous Close"})


def calculate_3m_change(real_estate_df, df_today, df_3month):
    
    # Local copy
    df = real_estate_df.copy()

    # Build lookup dictionaries
    today_map = df_today.set_index("ISIN")["ClsPric"].to_dict()
    old_map = df_3month.set_index("ISIN")["ClsPric"].to_dict()

    for idx, row in df.iterrows():
        isin = row["ISIN"]

        today_price = today_map.get(isin)
        old_price = old_map.get(isin)

        # Assign raw values (optional)
        df.at[idx, "Current Close"] = today_price
        df.at[idx, "3M Close"] = old_price

        # Calculate change
        if today_price is not None and old_price is not None:
            df.at[idx, "3M Change"] = (today_price - old_price)/old_price
        else:
            df.at[idx, "3M Change"] = None

    return df

def calculate_6m_change(real_estate_df, df_today, df_6month):
    
    # Local copy
    df = real_estate_df.copy()

    # Build lookup dictionaries
    today_map = df_today.set_index("ISIN")["ClsPric"].to_dict()
    old_map = df_6month.set_index("ISIN")["ClsPric"].to_dict()

    for idx, row in df.iterrows():
        isin = row["ISIN"]

        today_price = today_map.get(isin)
        old_price = old_map.get(isin)

        # Assign raw values (optional)
        df.at[idx, "Current Close"] = today_price
        df.at[idx, "6M Close"] = old_price

        # Calculate change
        if today_price is not None and old_price is not None:
            df.at[idx, "6M Change"] = (today_price - old_price)/old_price
        else:
            df.at[idx, "6M Change"] = None

    return df

def calculate_9m_change(real_estate_df, df_today, df_9month):
    
    # Local copy
    df = real_estate_df.copy()

    # Build lookup dictionaries
    today_map = df_today.set_index("ISIN")["ClsPric"].to_dict()
    old_map = df_9month.set_index("ISIN")["ClsPric"].to_dict()

    for idx, row in df.iterrows():
        isin = row["ISIN"]

        today_price = today_map.get(isin)
        old_price = old_map.get(isin)

        # Assign raw values (optional)
        df.at[idx, "Current Close"] = today_price
        df.at[idx, "9M Close"] = old_price

        # Calculate change
        if today_price is not None and old_price is not None:
            df.at[idx, "9M Change"] = (today_price - old_price)/old_price
        else:
            df.at[idx, "9M Change"] = None

    return df

def calculate_12m_change(real_estate_df, df_today, df_12month):
    
    # Local copy
    df = real_estate_df.copy()

    # Build lookup dictionaries
    today_map = df_today.set_index("ISIN")["ClsPric"].to_dict()
    old_map = df_12month.set_index("ISIN")["ClsPric"].to_dict()

    for idx, row in df.iterrows():
        isin = row["ISIN"]

        today_price = today_map.get(isin)
        old_price = old_map.get(isin)

        # Assign raw values (optional)
        df.at[idx, "Current Close"] = today_price
        df.at[idx, "12M Close"] = old_price

        # Calculate change
        if today_price is not None and old_price is not None:
            df.at[idx, "12M Change"] = (today_price - old_price)/old_price
        else:
            df.at[idx, "12M Change"] = None

    return df

def MainEntryLogicFunction(
        real_estate_df,
        df_today,
        df_prev,
        df_3m,
        df_6m,
        df_9m,
        df_12m
):

    df = real_estate_df.copy()     # do not modify original

    # Make lookups for faster access
    today_map = df_today.set_index("ISIN")["ClsPric"].to_dict()
    prev_map = df_prev.set_index("ISIN")["ClsPric"].to_dict()

    # Loop row-by-row and fill values
    for idx, row in df.iterrows():
        isin = row["ISIN"]

        # Current Close
        df.at[idx, "Current Close"] = today_map.get(isin, None)

        # Previous Close
        df.at[idx, "Previous Close"] = prev_map.get(isin, None)

        # Price Change
        if isin in today_map and isin in prev_map:
            df.at[idx, "Price Change"] = today_map[isin] - prev_map[isin]

    # Compute changes
    df = calculate_3m_change(df, df_today, df_3m)
    df = calculate_6m_change(df, df_today, df_6m)
    df = calculate_9m_change(df, df_today, df_9m)
    df = calculate_12m_change(df, df_today, df_12m)

    # ----------------------------------------
    
    # 👉 Add FinInstrmId (BSE code)
    # ----------------------------------------
    print(df_today)
    fin_map = df_today.set_index("ISIN")["FinInstrmId"].to_dict()
    df["FinInstrmId"] = df["ISIN"].map(fin_map)

    # print(df.columns)
    # print(df[['Company', 'ISIN', 'FinInstrmId', '3M Change', '6M Change', '9M Change', '12M Change']])

    return df
