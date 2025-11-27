import os 
from datetime import datetime, timedelta
from NSE_BHAV_COPY import dataFunct
import zipfile
import pandas as pd
from dateutil.relativedelta import relativedelta
import time


def wait_one_minute():
    print("⏳ Waiting 1 minute before next request...")
    time.sleep(5)


def data_creation(today):

    now = datetime.now()

    # Convert to YYYYMMDD
    today_str = today.strftime('%Y%m%d')

    # Previous day
    prev_day = today - timedelta(days=1)
    prev_day_str = prev_day.strftime('%Y%m%d')

    # Months before (3, 6, 9, 12)
    months_3 = today - relativedelta(months=3)
    months_6 = today - relativedelta(months=6)
    months_9 = today - relativedelta(months=9)
    months_12 = today - relativedelta(months=12)

    months_3_str = months_3.strftime('%Y%m%d')
    months_6_str = months_6.strftime('%Y%m%d')
    months_9_str = months_9.strftime('%Y%m%d')
    months_12_str = months_12.strftime('%Y%m%d')

    print("Today:", today_str)
    print("Previous Day:", prev_day_str)
    print("3 Months Before:", months_3_str)
    print("6 Months Before:", months_6_str)
    print("9 Months Before:", months_9_str)
    print("12 Months Before:", months_12_str)

    # Example usage
    run_str = today_str

    # Today
    print(today_str)
    response_today, output_path_today = dataFunct(run_str, today_str, f"A . | Today |-{today.strftime('%d-%m-%Y')}", "today")

    # Previous Day
    print(prev_day)
    response_prev, output_path_prev = dataFunct(run_str, prev_day_str, f"B . | Prev |-{prev_day.strftime('%d-%m-%Y')}", "prev")

    # 3 Months
    print(months_3_str)
    response_3m, output_3m = dataFunct(run_str, months_3_str,  f"C. | 03-Month |-{months_3.strftime('%d-%m-%Y')}", "3")

    # 6 Months
    print(months_6_str)
    response_6m, output_6m = dataFunct(run_str, months_6_str, f"D .| 06-Month |-{months_6.strftime('%d-%m-%Y')}","6")

    # 9 Months
    print(months_9_str)
    response_9m, output_9m = dataFunct(run_str, months_9_str, f"E. | 09-Month |-{months_9.strftime('%d-%m-%Y')}", "9")

    # 12 Months
    print(months_12_str)
    response_12m, output_12m = dataFunct(run_str, months_12_str, f"F. | 12-Month |-{months_12.strftime('%d-%m-%Y')}", "12")

    return output_path_today, output_path_prev, output_3m, output_6m, output_9m, output_12m