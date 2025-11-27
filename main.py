import os 
from datetime import datetime, timedelta
from NSE_BHAV_COPY import dataFunct
import zipfile
import pandas as pd
from dateutil.relativedelta import relativedelta
import time
from data_prep import data_creation
from Pricing_Change_Logic import MainEntryLogicFunction
import Pricing_Change_Logic
from Market_Cap_Logic import add_marketcap_column
from BSE_Data_Download import Today_BSE_Bhav
from Volume_Change_Logic import Today_Volume
from High_Low_Logic import get_52week_high_low
from Volume_Change_Logic import isin_mapping

def wait_one_minute():
    print("⏳ Waiting 1 minute before next request...")
    time.sleep(5)

import zipfile
import pandas as pd

def load_bhavcopy(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_name = z.namelist()[0]          # first file inside
        with z.open(csv_name) as f:
            return pd.read_csv(f)



if __name__ == "__main__":

    print(datetime.now().time())
    current = datetime(2025, 11, 26)
    output_path_today, output_path_prev, output_3m, output_6m, output_9m, output_12m = data_creation(current)

    real_estate_df = pd.DataFrame(columns=Pricing_Change_Logic.columns)

    for company, isin in Pricing_Change_Logic.isin_mapping.items():
        real_estate_df.loc[len(real_estate_df)] = {"Company": company, "ISIN": isin}

    # Load zipped CSVs correctly
    df_today = load_bhavcopy(output_path_today)
    df_prev = load_bhavcopy(output_path_prev)
    df_3month = load_bhavcopy(output_3m)
    df_6month = load_bhavcopy(output_6m)
    df_9month = load_bhavcopy(output_9m)
    df_12month = load_bhavcopy(output_12m)
    print(f"OUTPUT PATH", output_path_today)
    print(f"OUTPUT PATH",output_path_prev)
    print(f"OUTPUT PATH",output_3m)
    print(f"OUTPUT PATH",output_6m)
    print(f"OUTPUT PATH",output_9m)
    print(f"OUTPUT PATH",output_12m)
    print(df_prev.columns)
    print(df_prev['FinInstrmId'])

    print(df_today.columns)

    ## PRICE CHANGE 
    price_change_df = MainEntryLogicFunction(
        real_estate_df, 
        df_today, df_prev, df_3month, df_6month, df_9month, df_12month
    )

    # MARKET CAP | MARKET FF 
    market_df = add_marketcap_column(price_change_df)

    ## VOLUME
    Today_BSE_Bhav(date_parm=current)

    final_df = Today_Volume(df_today, BSE_bhav_today=None, master_df=price_change_df)
    final_df = final_df.drop(['3M Close','6M Close', '9M Close','12M Close','FinInstrmId'], axis = 1)
    final_df.to_csv('Final_Data_Frame_Except_3month_Average.csv')

    

    # 52 Week High Low DF Seperate
    hl_df = df_today[['TckrSymb','ISIN','FinInstrmNm', 'FinInstrmId']]
    ISIN_list = list(isin_mapping.values())
    hl_df = hl_df[hl_df['ISIN'].isin(ISIN_list)]
    print(hl_df)
    hl_df["52W_High"] = None
    hl_df["52W_Low"] = None    

    for idx, row in hl_df.iterrows():
        symbol = row["TckrSymb"]       # change column name if needed
        high, low = get_52week_high_low(symbol)
        print(high, low)
        hl_df.at[idx, "52W_High"] = high
        hl_df.at[idx, "52W_Low"] = low

    hl_df.to_csv('52_High_Low.csv')
    



